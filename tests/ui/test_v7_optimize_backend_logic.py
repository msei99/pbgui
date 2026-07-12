"""Regression tests for PBGui's FastAPI v7 optimize backend editor logic.

These tests lock down the frontend-only semantics that must stay aligned with
PB7 optimizer behavior:

- effective pymoo algorithm resolution across objective-count cases
- DEAP -> pymoo migration preserves the mapped crossover probability
- pymoo -> DEAP migration preserves explicit values and falls back correctly
- DEAP population size displays PB7's effective fallback when legacy configs
  store null
- ref_dirs_method renders as fixed text or a select depending on PB7 options
- section visibility and field locking stay consistent for NSGA-II / NSGA-III
- NSGA-III effective population size follows the same rules as PB7 for both
  auto and explicit reference-direction settings
- The read-only NSGA-III auto population label shows the numeric effective size
"""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "frontend" / "v7_optimize.html"


def _extract_function(source: str, name: str) -> str:
    """Extract a named JavaScript function from the inline optimize editor script."""
    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"

    brace_start = source.find("{", start)
    assert brace_start >= 0, f"Could not find opening brace for {name!r}"

    depth = 0
    quote: str | None = None
    escaped = False

    for index in range(brace_start, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue

        if char in ("'", '"', "`"):
            quote = char
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]

    raise AssertionError(f"Could not extract complete JavaScript function {name!r}")


def _run_node_assertions(function_names: list[str], *, bootstrap: str, assertions: str) -> None:
    """Run a focused Node assertion script against extracted frontend helpers."""
    source = HTML_PATH.read_text(encoding="utf-8")
    extracted = "\n\n".join(_extract_function(source, name) for name in function_names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');

        {bootstrap}

        {extracted}

        {assertions}
        """
    )
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        "Node-backed frontend regression failed\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


class TestV7OptimizeBackendLogic:
    """Lock down frontend optimize editor behavior against PB7 semantics."""

    def test_effective_pymoo_algorithm_matrix(self) -> None:
        """Algorithm auto/fallback selection must match PB7's objective-count rules."""
        bootstrap = """
        function normalizeOptimizeBackendValue(value) {
          return String(value || '').trim().toLowerCase();
        }
        function normalizeOptimizePymooAlgorithmValue(value) {
          return String(value || '').trim().toLowerCase();
        }
        function getScoringEntries() {
          return [];
        }
        """
        assertions = """
        var cases = [
          ['deap', 'auto', 4, ''],
          ['pymoo', 'auto', 1, 'nsga2'],
          ['pymoo', 'auto', 3, 'nsga2'],
          ['pymoo', 'auto', 4, 'nsga3'],
          ['pymoo', 'nsga3', 1, 'nsga2'],
          ['pymoo', 'nsga3', 2, 'nsga3'],
          ['pymoo', 'nsga2', 5, 'nsga2']
        ];
        cases.forEach(function(testCase) {
          assert.equal(
            getEffectiveOptimizePymooAlgorithm(testCase[0], testCase[1], testCase[2]),
            testCase[3],
            JSON.stringify(testCase)
          );
        });
        """
        _run_node_assertions(
            ["optimizeObjectiveCount", "getEffectiveOptimizePymooAlgorithm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_deap_to_pymoo_migration_preserves_crossover_probability(self) -> None:
        """Switching from DEAP to pymoo must copy crossover_probability into crossover_prob_var."""
        bootstrap = """
        var __fields = {
          'opted-deap-population-size': { value: '250' },
          'opted-crossover-eta': { value: '21' },
          'opted-mutation-eta': { value: '17' },
          'opted-mutation-indpb': { value: '0.12' },
          'opted-pymoo-population-mode': { value: 'auto' },
          'opted-pymoo-mutation-prob-mode': { value: 'auto' },
          'opted-pymoo-population-size': { value: '' },
          'opted-pymoo-crossover-eta': { value: '' },
          'opted-pymoo-crossover-prob-var': { value: '' },
          'opted-pymoo-mutation-eta': { value: '' },
          'opted-pymoo-mutation-prob-value': { value: '' }
        };

        function el(id) { return Object.prototype.hasOwnProperty.call(__fields, id) ? __fields[id] : null; }
        function toNullableNumber(raw) {
          if (raw == null) return null;
          var text = String(raw).trim();
          if (!text) return null;
          var parsed = Number(text);
          return Number.isFinite(parsed) ? parsed : null;
        }
        function getOptimizeFieldNumber(id) {
          var node = el(id);
          return node ? toNullableNumber(node.value) : null;
        }
        function setOptimizeFieldValue(id, value) {
          if (!__fields[id]) __fields[id] = { value: '' };
          __fields[id].value = value == null ? '' : String(value);
        }
        function getOptimizeProbabilityPair() {
          return { crossover: 0.77, mutation: 0.34 };
        }
        """
        assertions = """
        migrateOptimizeDeapFieldsToPymoo();
        assert.equal(__fields['opted-pymoo-population-mode'].value, 'value');
        assert.equal(__fields['opted-pymoo-population-size'].value, '250');
        assert.equal(__fields['opted-pymoo-crossover-eta'].value, '21');
        assert.equal(__fields['opted-pymoo-crossover-prob-var'].value, '0.77');
        assert.equal(__fields['opted-pymoo-mutation-eta'].value, '17');
        assert.equal(__fields['opted-pymoo-mutation-prob-mode'].value, 'value');
        assert.equal(__fields['opted-pymoo-mutation-prob-value'].value, '0.12');
        """
        _run_node_assertions(
            ["migrateOptimizeDeapFieldsToPymoo"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pymoo_to_deap_migration_preserves_explicit_values_and_defaults_auto_ones(self) -> None:
        """Switching from pymoo to DEAP must copy explicit values and restore PB7 defaults for auto ones."""
        bootstrap = """
        function buildFields(populationMode, populationSize, mutationMode, mutationValue) {
          return {
            'opted-pymoo-population-mode': { value: populationMode },
            'opted-pymoo-population-size': { value: populationSize },
            'opted-pymoo-crossover-eta': { value: '21' },
            'opted-pymoo-mutation-eta': { value: '17' },
            'opted-pymoo-mutation-prob-mode': { value: mutationMode },
            'opted-pymoo-mutation-prob-value': { value: mutationValue },
            'opted-deap-population-size': { value: '' },
            'opted-crossover-eta': { value: '' },
            'opted-mutation-eta': { value: '' },
            'opted-mutation-indpb': { value: '' },
            'opted-offspring-multiplier': { value: '' }
          };
        }

        var __fields = buildFields('auto', '', 'auto', '');
        var __probabilityInitCalls = [];

        function el(id) { return Object.prototype.hasOwnProperty.call(__fields, id) ? __fields[id] : null; }
        function toNullableNumber(raw) {
          if (raw == null) return null;
          var text = String(raw).trim();
          if (!text) return null;
          var parsed = Number(text);
          return Number.isFinite(parsed) ? parsed : null;
        }
        function getOptimizeFieldNumber(id) {
          var node = el(id);
          return node ? toNullableNumber(node.value) : null;
        }
        function setOptimizeFieldValue(id, value) {
          if (!__fields[id]) __fields[id] = { value: '' };
          __fields[id].value = value == null ? '' : String(value);
        }
        function optimizeDeapDefaults() {
          return {
            population_size: 500,
            offspring_multiplier: 1,
            crossover_probability: 0.64,
            mutation_probability: 0.34,
            mutation_indpb: 0.0135135135,
            crossover_eta: 20,
            mutation_eta: 20
          };
        }
        function initOptimizeProbabilityPair(crossover, mutation) {
          __probabilityInitCalls.push({ crossover: crossover, mutation: mutation });
        }
        """
        assertions = """
        migrateOptimizePymooFieldsToDeap();
        assert.equal(__fields['opted-deap-population-size'].value, '500');
        assert.equal(__fields['opted-crossover-eta'].value, '21');
        assert.equal(__fields['opted-mutation-eta'].value, '17');
        assert.equal(__fields['opted-mutation-indpb'].value, '0.0135135135');
        assert.equal(__fields['opted-offspring-multiplier'].value, '1');
        assert.deepEqual(__probabilityInitCalls[0], { crossover: 0.64, mutation: 0.34 });

        __fields = buildFields('value', '250', 'value', '0.12');
        __probabilityInitCalls = [];
        migrateOptimizePymooFieldsToDeap();
        assert.equal(__fields['opted-deap-population-size'].value, '250');
        assert.equal(__fields['opted-mutation-indpb'].value, '0.12');
        assert.deepEqual(__probabilityInitCalls[0], { crossover: 0.64, mutation: 0.34 });
        """
        _run_node_assertions(
            [
                "normalizeOptimizePymooMutationProbMode",
                "normalizeOptimizePymooPopulationMode",
                "migrateOptimizePymooFieldsToDeap",
            ],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_deap_population_display_uses_pb7_fallback_for_null(self) -> None:
        """Legacy DEAP null population sizes must reopen as the runtime-effective fallback of 500."""
        bootstrap = """
        function toNullableNumber(raw) {
          if (raw == null) return null;
          var text = String(raw).trim();
          if (!text) return null;
          var parsed = Number(text);
          return Number.isFinite(parsed) ? parsed : null;
        }
        function optimizeDeapDefaults() {
          return { population_size: 500 };
        }
        """
        assertions = """
        assert.equal(resolveOptimizeDeapPopulationDisplayValue(null), 500);
        assert.equal(resolveOptimizeDeapPopulationDisplayValue(''), 500);
        assert.equal(resolveOptimizeDeapPopulationDisplayValue('330'), 330);
        """
        _run_node_assertions(
            ["normalizeOptimizePositiveInteger", "resolveOptimizeDeapPopulationDisplayValue"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_ref_dir_method_field_renders_fixed_or_select_based_on_available_methods(self) -> None:
        """ref_dirs_method must be read-only for one PB7 option and a select when multiple exist."""
        bootstrap = """
        var state = {
          settings: {
            pymoo_ref_dir_method_options: ['das_dennis']
          }
        };

        function uniqStrings(values) {
          var normalized = [];
          (values || []).forEach(function(value) {
            var text = String(value || '').trim();
            if (text && normalized.indexOf(text) < 0) normalized.push(text);
          });
          return normalized;
        }
        function optimizePymooRefDirDefaults() {
          return { method: 'das_dennis' };
        }
        function fieldText2(id, label, value, editable, placeholder, tip, span) {
          return JSON.stringify({ kind: 'text', id: id, label: label, value: value, editable: editable, tip: tip, span: span });
        }
        function fieldSelect2(id, label, options, selected, multi, tip, span) {
          return JSON.stringify({ kind: 'select', id: id, label: label, selected: selected, options: options.map(function(option) { return option.value; }), multi: multi, tip: tip, span: span });
        }
        """
        assertions = """
        var fixedField = JSON.parse(buildOptimizePymooRefDirMethodField('das_dennis'));
        assert.equal(fixedField.kind, 'text');
        assert.equal(fixedField.value, 'das_dennis');

        state.settings.pymoo_ref_dir_method_options = ['das_dennis', 'incremental'];
        var selectField = JSON.parse(buildOptimizePymooRefDirMethodField('incremental'));
        assert.equal(selectField.kind, 'select');
        assert.deepEqual(selectField.options, ['das_dennis', 'incremental']);
        assert.equal(selectField.selected, 'incremental');
        """
        _run_node_assertions(
            [
                "normalizeOptimizePymooRefDirMethodValue",
                "getOptimizePymooRefDirMethodOptions",
                "buildOptimizePymooRefDirMethodField",
            ],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_nsga3_effective_population_matches_pb7_rules(self) -> None:
        """NSGA-III effective population size must match PB7 for explicit and auto reference directions."""
        bootstrap = """
        function toNullableNumber(raw) {
          if (raw == null) return null;
          var text = String(raw).trim();
          if (!text) return null;
          var parsed = Number(text);
          return Number.isFinite(parsed) ? parsed : null;
        }
        """
        assertions = """
        assert.equal(resolveOptimizePymooEffectivePopulationSize(4, 'value', 5, 'value', 2), 10);
        assert.equal(resolveOptimizePymooEffectivePopulationSize(4, 'value', 5, 'auto', ''), 5);
        assert.equal(resolveOptimizePymooEffectivePopulationSize(4, 'auto', '', 'auto', ''), 286);
        """
        _run_node_assertions(
            [
                "normalizeOptimizePymooPopulationMode",
                "normalizeOptimizePymooRefDirPartitionsMode",
                "normalizeOptimizePositiveInteger",
                "resolveOptimizePymooRequestedPopulationSize",
                "optimizeReferenceDirectionCount",
                "resolveOptimizePymooAutoRefDirPartitions",
                "resolveOptimizePymooEffectiveRefDirPartitions",
                "resolveOptimizePymooEffectivePopulationSize",
            ],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_update_sections_matrix_covers_population_locking_and_auto_fields(self) -> None:
        """Section updates must keep mode locks, auto fields, and NSGA-III minimums consistent."""
        bootstrap = """
        function makeField(value) {
          return {
            value: value == null ? '' : String(value),
            style: { display: '' },
            disabled: false,
            min: '',
            querySelector: null
          };
        }
        function makeModeField(value) {
          var field = makeField(value);
          field.autoOption = { disabled: false };
          field.querySelector = function(selector) {
            return selector === 'option[value="auto"]' ? this.autoOption : null;
          };
          return field;
        }
        function buildFields() {
          return {
            'optimize-pymoo-section': { style: { display: '' } },
            'optimize-deap-section': { style: { display: '' } },
            'optimize-pymoo-nsga3-section': { style: { display: '' } },
            'opted-pymoo-effective-algorithm': makeField(''),
            'opted-pymoo-mutation-prob-mode': makeModeField('auto'),
            'opted-pymoo-mutation-prob-value': makeField(''),
            'opted-pymoo-mutation-prob-auto': makeField(''),
            'opted-pymoo-population-mode': makeModeField('auto'),
            'opted-pymoo-population-size': makeField(''),
            'opted-pymoo-population-size-auto': makeField(''),
            'opted-pymoo-ref-dir-npartitions-mode': makeModeField('auto'),
            'opted-pymoo-ref-dir-npartitions': makeField(''),
            'opted-pymoo-ref-dir-npartitions-auto': makeField('')
          };
        }

        var __fields = buildFields();
        var __visibility = { showPymoo: true, showDeap: false, showNsga3: false, effectiveAlgorithm: 'nsga2', objectiveCount: 1, algorithm: 'auto' };
        var __scheduled = 0;

        function el(id) { return Object.prototype.hasOwnProperty.call(__fields, id) ? __fields[id] : null; }
        function optimizeSectionVisibility() { return __visibility; }
        function formatOptimizeEffectiveAlgorithmLabel(visibility) { return 'effective:' + visibility.effectiveAlgorithm; }
        function formatOptimizePymooMutationAutoLabel() { return 'mutation-auto'; }
        function formatOptimizePymooPopulationAutoLabel() { return 'population-auto'; }
        function formatOptimizePymooRefDirPartitionsAutoValue() { return 'refdir-auto'; }
        function scheduleStructuredEditorSync() { __scheduled += 1; }
        function toNullableNumber(raw) {
          if (raw == null) return null;
          var text = String(raw).trim();
          if (!text) return null;
          var parsed = Number(text);
          return Number.isFinite(parsed) ? parsed : null;
        }
        """
        assertions = """
        updateOptimizeBackendSections();
        assert.equal(__fields['opted-pymoo-population-mode'].value, 'value');
        assert.equal(__fields['opted-pymoo-population-mode'].autoOption.disabled, true);
        assert.equal(__fields['opted-pymoo-population-size'].style.display, '');
        assert.equal(__fields['opted-pymoo-population-size-auto'].style.display, 'none');
        assert.equal(__scheduled, 1);

        __fields = buildFields();
        __scheduled = 0;
        __visibility = { showPymoo: true, showDeap: false, showNsga3: true, effectiveAlgorithm: 'nsga3', objectiveCount: 4, algorithm: 'nsga3' };
        __fields['opted-pymoo-population-mode'].value = 'value';
        __fields['opted-pymoo-population-size'].value = '5';
        __fields['opted-pymoo-ref-dir-npartitions-mode'].value = 'value';
        __fields['opted-pymoo-ref-dir-npartitions'].value = '2';
        updateOptimizeBackendSections();
        assert.equal(__fields['opted-pymoo-population-size'].value, '10');
        assert.equal(__fields['opted-pymoo-population-size'].min, '10');
        assert.equal(__fields['opted-pymoo-mutation-prob-value'].style.display, 'none');
        assert.equal(__fields['opted-pymoo-mutation-prob-auto'].style.display, '');
        assert.equal(__fields['optimize-pymoo-nsga3-section'].style.display, '');
        assert.equal(__scheduled, 1);

        __fields = buildFields();
        __scheduled = 0;
        __visibility = { showPymoo: true, showDeap: false, showNsga3: true, effectiveAlgorithm: 'nsga3', objectiveCount: 4, algorithm: 'auto' };
        __fields['opted-pymoo-population-mode'].value = 'auto';
        __fields['opted-pymoo-ref-dir-npartitions-mode'].value = 'auto';
        updateOptimizeBackendSections();
        assert.equal(__fields['opted-pymoo-population-mode'].autoOption.disabled, false);
        assert.equal(__fields['opted-pymoo-population-size'].style.display, 'none');
        assert.equal(__fields['opted-pymoo-population-size-auto'].style.display, '');
        assert.equal(__fields['opted-pymoo-ref-dir-npartitions'].style.display, 'none');
        assert.equal(__fields['opted-pymoo-ref-dir-npartitions-auto'].style.display, '');
        assert.equal(__fields['opted-pymoo-ref-dir-npartitions-auto'].value, 'refdir-auto');
        assert.equal(__scheduled, 0);
        """
        _run_node_assertions(
            [
                "normalizeOptimizePymooMutationProbMode",
                "normalizeOptimizePymooPopulationMode",
                "normalizeOptimizePymooRefDirPartitionsMode",
                "normalizeOptimizePositiveInteger",
                "resolveOptimizePymooRequestedPopulationSize",
                "optimizeObjectiveCount",
                "optimizeReferenceDirectionCount",
                "resolveOptimizePymooAutoRefDirPartitions",
                "resolveOptimizePymooEffectiveRefDirPartitions",
                "getCurrentOptimizePymooMinimumPopulationSize",
                "updateOptimizeBackendSections",
            ],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_nsga3_auto_population_label_shows_numeric_effective_value(self) -> None:
        """The read-only NSGA-III auto population label must expose the effective numeric size."""
        bootstrap = """
        var __fields = {
          'opted-pymoo-population-mode': { value: 'auto' },
          'opted-pymoo-population-size': { value: '' },
          'opted-pymoo-ref-dir-npartitions-mode': { value: 'value' },
          'opted-pymoo-ref-dir-npartitions': { value: '2' }
        };

        function el(id) { return Object.prototype.hasOwnProperty.call(__fields, id) ? __fields[id] : null; }
        function toNullableNumber(raw) {
          if (raw == null) return null;
          var text = String(raw).trim();
          if (!text) return null;
          var parsed = Number(text);
          return Number.isFinite(parsed) ? parsed : null;
        }
        """
        assertions = """
        assert.equal(
          formatOptimizePymooPopulationAutoLabel({ showPymoo: true, effectiveAlgorithm: 'nsga3', objectiveCount: 4 }),
          'auto (10 from NSGA-III ref_dirs)'
        );
        """
        _run_node_assertions(
            [
                "normalizeOptimizePymooPopulationMode",
                "normalizeOptimizePymooRefDirPartitionsMode",
                "normalizeOptimizePositiveInteger",
                "resolveOptimizePymooRequestedPopulationSize",
                "optimizeObjectiveCount",
                "optimizeReferenceDirectionCount",
                "resolveOptimizePymooAutoRefDirPartitions",
                "resolveOptimizePymooEffectiveRefDirPartitions",
                "resolveOptimizePymooEffectivePopulationSize",
                "formatOptimizePymooPopulationAutoLabel",
            ],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_end_date_helpers_preserve_now_semantics(self) -> None:
        """The optimize editor must keep the semantic `now` token instead of materializing today's date."""
        bootstrap = """
        const RealDate = Date;
        const frozenNow = new RealDate('2026-04-24T12:00:00Z');
        globalThis.Date = class extends RealDate {
          constructor(...args) {
            return args.length ? new RealDate(...args) : new RealDate(frozenNow);
          }
          static now() { return frozenNow.getTime(); }
          static parse(value) { return RealDate.parse(value); }
          static UTC(...args) { return RealDate.UTC(...args); }
        };
        """
        assertions = """
        assert.equal(normalizeOptimizeEndDateValue(' now '), 'now');
        assert.equal(normalizeOptimizeEndDateValue(' 2026-04-22 '), '2026-04-22');
        assert.equal(resolveOptimizeEditorEndDateValue('now'), 'now');
        assert.equal(resolveOptimizeEditorEndDateValue(''), '2026-04-24');
        """
        _run_node_assertions(
            ["normalizeOptimizeEndDateValue", "resolveOptimizeEditorEndDateValue"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_limit_numeric_input_keeps_partial_text_without_rerender(self) -> None:
        """Typing numeric limits must not normalize partial input to 0 on each keypress."""
        bootstrap = """
        var renderCount = 0;
        var state = {
          limitAddForm: {
            metric_type: 'all',
            base_metric: 'drawdown_worst',
            currency: '',
            enabled: true,
            penalize_if: 'greater_than',
            stat: '',
            value: 0,
            range_low: 0,
            range_high: 1
          },
          limitEditIndex: 0,
          limitEditForm: {
            metric_type: 'all',
            base_metric: 'drawdown_worst',
            currency: '',
            enabled: true,
            penalize_if: 'greater_than',
            stat: '',
            value: 0,
            range_low: 0,
            range_high: 1
          }
        };
        function currentLimitsMeta() {
          return {
            type_options: ['all'],
            stat_options: ['', 'mean', 'max'],
            currency_options: ['usd', 'btc']
          };
        }
        function limitMetricOptions(metricType, currentBaseMetric) {
          return currentBaseMetric ? [currentBaseMetric] : ['drawdown_worst'];
        }
        function isLimitCurrencyMetric(metricBase) { return false; }
        function renderOptimizeLimitsEditor() { renderCount += 1; }
        function escAttr(value) { return String(value == null ? '' : value); }
        """
        assertions = """
        assert.equal(normalizeLimitNumber('0,25', 0), 0.25);
        assert.equal(normalizeLimitNumber('0.', 0), 0);

        updateLimitEditField('value', '0.');
        assert.equal(state.limitEditForm.value, '0.');
        assert.equal(renderCount, 0);

        updateLimitEditField('value', '');
        assert.equal(state.limitEditForm.value, '');
        assert.equal(renderCount, 0);

        updateLimitAddField('range_low', '1.');
        assert.equal(state.limitAddForm.range_low, '1.');
        assert.equal(renderCount, 0);

        updateLimitEditField('penalize_if', 'less_than');
        assert.equal(state.limitEditForm.penalize_if, 'less_than');
        assert.equal(renderCount, 1);

        var inputHtml = renderInlineNumberInput('0,25', "updateLimitEditField('value', this.value)");
        assert.match(inputHtml, /type="text"/);
        assert.match(inputHtml, /inputmode="decimal"/);
        assert.doesNotMatch(inputHtml, /type="number"/);
        """
        _run_node_assertions(
            [
                "normalizeLimitPenalizeIf",
                "normalizeLimitNumber",
                "isLimitNumberFormField",
                "normalizeLimitForm",
                "renderInlineNumberInput",
                "updateLimitAddField",
                "updateLimitEditField",
            ],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_log_panel_result_lookup_prefers_latest_matching_result(self) -> None:
        """Pareto Front mini actions should target the newest result matching the log name."""
        bootstrap = """
        var state = {
          logFilename: 'queue-file',
          logResultName: 'alpha',
          selectedResultPath: '',
          selectedResultName: '',
          results: [
            { name: 'alpha', result: 'alpha', path: '/results/old_alpha', modified: '2026-01-01T00:00:00Z' },
            { name: 'alpha', result: 'alpha', path: '/results/new_alpha', modified: '2026-01-02T00:00:00Z' },
            { name: 'beta', result: 'beta', path: '/results/beta', modified: '2026-01-03T00:00:00Z' }
          ]
        };
        var __panel = '';
        var __loadedParetos = null;

        function setPanel(panel) { __panel = panel; }
        function loadParetos(path, name) {
          __loadedParetos = { path: path, name: name };
          return Promise.resolve();
        }
        function handleError(error) { throw error; }
        """
        assertions = """
        assert.equal(logPanelResultQuery(), 'alpha');
        assert.equal(findLogPanelResult().path, '/results/new_alpha');
        assert.equal(openLogPanelParetosForResult(findLogPanelResult()), true);
        assert.equal(__panel, 'paretos');
        assert.equal(state.selectedResultPath, '/results/new_alpha');
        assert.equal(__loadedParetos.path, '/results/new_alpha');

        state.logResultName = '';
        state.logFilename = 'beta';
        assert.equal(logPanelResultQuery(), 'beta');
        assert.equal(findLogPanelResult().path, '/results/beta');
        """
        _run_node_assertions(
            ["logPanelResultQuery", "findLogPanelResult", "openLogPanelParetosForResult"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_raw_json_sync_recomputes_backend_hint_from_parsed_config(self) -> None:
        """Raw JSON reparse must clear stale backend hints unless legacy DEAP keys still imply them."""
        bootstrap = """
        var OPT_LEGACY_DEAP_HINT_KEYS = [
          'crossover_probability',
          'mutation_probability',
          'mutation_indpb',
          'offspring_multiplier'
        ];
        var state = {
          editorDraftName: '',
          editorBackendHint: 'deap',
          editorLastConfig: null
        };
        var __capturedPopulate = null;
        var __restoredAnchor = null;

        function captureOptimizeRawAnchor() {
          return { raw: 'anchor' };
        }
        function restoreOptimizeRawAnchor(anchor) {
          __restoredAnchor = anchor;
        }
        function deepClone(value) {
          return JSON.parse(JSON.stringify(value));
        }
        function populateOptimizeEditor(cfg, opts) {
          __capturedPopulate = { cfg: cfg, opts: opts };
        }
        """
        assertions = """
        optSyncEditorFromParsed('explicit', { optimize: { backend: 'pymoo' } });
        assert.equal(state.editorBackendHint, '');
        assert.equal(state.editorDraftName, 'explicit');
        assert.equal(__capturedPopulate.opts.skipRawUpdate, true);
        assert.deepEqual(__restoredAnchor, { raw: 'anchor' });

        state.editorBackendHint = 'deap';
        optSyncEditorFromParsed('legacy', { optimize: { crossover_probability: 0.7 } });
        assert.equal(state.editorBackendHint, 'deap');

        state.editorBackendHint = 'deap';
        optSyncEditorFromParsed('cleared', { optimize: {} });
        assert.equal(state.editorBackendHint, '');
        """
        _run_node_assertions(
            [
                "hasExplicitOptimizeBackendValue",
                "inferOptimizeEditorBackendHint",
                "optSyncEditorFromParsed",
            ],
            bootstrap=bootstrap,
            assertions=assertions,
        )
