"""Offline Node regressions for Vast metrics and GPU recommendation rates."""

import json
from pathlib import Path
import subprocess

import pytest


@pytest.fixture(scope='module')
def vast_source():
    """Load the production component without executing its startup code."""
    return (Path(__file__).resolve().parents[2] / 'frontend/js/vast.js').read_text()


def function_source(source, start_marker, end_marker):
    """Extract a production function using its neighboring declarations."""
    start = source.index(start_marker)
    return source[start:source.index(end_marker, start)]


def run_node(code):
    """Execute isolated JavaScript assertions without browser or cloud access."""
    result = subprocess.run(['node', '-e', "'use strict';\n" + code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('value', [
    'null', 'undefined', 'NaN', 'Infinity', '-Infinity',
    "'abc'", "'NaN'", "'Infinity'",
    "''", "'  '", 'true', 'false', '[]', '[1]', '{}',
    "JSON.parse('{\"toString\":null,\"valueOf\":null}')",
])
def test_fmt_returns_unknown_for_missing_or_nonfinite_values(vast_source, value):
    """Missing metrics and failed numeric conversions never display NaN/Infinity."""
    formatter = function_source(vast_source, '  function fmt(', '\n  function offerDuration(')
    run_node(formatter + f"\nrequire('node:assert/strict').equal(fmt({value}), 'Unknown');")


@pytest.mark.parametrize('arguments, expected', [
    ('0', '0.00'),
    ('-1.25', '-1.25'),
    ('12.345', '12.35'),
    ("'1.5'", '1.50'),
    ('1.5, null', '1.50'),
    ('1.5, undefined', '1.50'),
    ('1.5, 2', '1.50'),
    ('1.5, 0', '2'),
    ('0, 0', '0'),
    ('1.23456, 4', '1.2346'),
    ("'1.5', 4", '1.5000'),
])
def test_fmt_preserves_finite_values_and_precision(vast_source, arguments, expected):
    """Zero, numeric strings, rounding and default/explicit precision stay valid."""
    formatter = function_source(vast_source, '  function fmt(', '\n  function offerDuration(')
    run_node(formatter + f"\nrequire('node:assert/strict').equal(fmt({arguments}), {json.dumps(expected)});")


def test_gpu_recommendation_formats_missing_and_nonfinite_rates(vast_source):
    """The real renderer uses Unknown for invalid rates and retains offer matching."""
    formatter = function_source(vast_source, '  function fmt(', '\n  function offerDuration(')
    renderer = function_source(vast_source, '  function renderGpuRecommendation(',
                               '\n  async function refreshGpuRecommendation(')
    run_node(formatter + renderer + r'''
const assert = require('node:assert/strict');
const elements = {
    'opted-gpu-recommendation': {},
    'opted-gpu-dispatch-preview': {},
    'opted-gpu-apply-measured': {},
};
const el = id => elements[id];
let selectedOffer = {id: 123};
const profile = {population_size: 7168, measured_power_limit_watts: 200};
let gpuRecommendation = {offerId: 123, result: {suggestion: {profile}}};
const status = el('opted-gpu-recommendation');
const button = el('opted-gpu-apply-measured');
// A stored profile can omit the rate entirely.
renderGpuRecommendation();
assert.equal(status.textContent, 'Measured 7,168 · 200 W · Unknown candidates/s');
for (const rate of [null, undefined, NaN, Infinity, -Infinity, 'abc']) {
    profile.candidates_per_second = rate;
    renderGpuRecommendation();
    assert.ok(status.textContent.endsWith('Unknown candidates/s'));
    assert.equal(status.hidden, false);
    assert.equal(button.disabled, false);
}
for (const [rate, expected] of [[0, '0.00'], [12.345, '12.35'], ['1.5', '1.50']]) {
    profile.candidates_per_second = rate;
    renderGpuRecommendation();
    assert.ok(status.textContent.endsWith(expected + ' candidates/s'));
}
selectedOffer = {id: 124};
renderGpuRecommendation();
assert.equal(status.textContent, '');
assert.equal(status.hidden, true);
assert.equal(button.hidden, true);
assert.equal(button.disabled, true);
''')
