"""Frontend contracts for secure Market Data Tiingo credential updates."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "market_data_main.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from the embedded page."""
    start = source.index(f"function {name}(")
    async_start = source.rfind("async ", max(0, start - 12), start)
    if async_start >= 0 and not source[async_start + len("async ") : start].strip():
        start = async_start
    brace_start = source.index("{", start)
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
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"Could not extract JavaScript function {name!r}")


def test_tiingo_token_save_reuses_profile_and_clears_secret_input() -> None:
    """Direct configuration should update the active vault profile without retaining the token."""
    source = PAGE.read_text(encoding="utf-8")
    function = _extract_function(source, "saveTiingoToken")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const input = {{value: 'new-vault-token', type: 'password', disabled: false, dataset: {{}}}};
        const button = {{disabled: false}};
        const document = {{getElementById(id) {{
          if (id === 'settings-tiingo-token') return input;
          if (id === 'btn-save-tiingo-token') return button;
          return null;
        }}}};
        const settingsState = {{
          tiingoConfigured: true,
          tiingoProfileId: 'tradfi_tiingo_1',
          tiingoSaveGeneration: 0
        }};
        const requests = [];
        const toasts = [];
        let settingsReloads = 0;
        async function fetchApiKeysJson(path, options) {{
          requests.push({{path, options: options || null}});
          if (path === '/tradfi/profiles') return {{profiles: [{{
            id: 'tradfi_tiingo_1', provider: 'tiingo', label: 'Existing Tiingo',
            active: true, pending: false, shared: true
          }}]}};
          return {{status: 'saved', profile: {{id: 'tradfi_tiingo_1', has_api_key: true}}}};
        }}
        async function loadSettings(exchange) {{
          assert.equal(exchange, 'hyperliquid');
          settingsReloads += 1;
        }}
        function showToast(message, level) {{ toasts.push({{message, level}}); }}
        {function}
        (async () => {{
          await saveTiingoToken();
          assert.equal(requests.length, 2);
          assert.equal(requests[1].path, '/tradfi/config');
          assert.equal(requests[1].options.method, 'PUT');
          assert.deepEqual(JSON.parse(requests[1].options.body), {{
            profile_id: 'tradfi_tiingo_1',
            provider: 'tiingo',
            label: 'Existing Tiingo',
            active: true,
            shared: true,
            api_key: 'new-vault-token',
            create_new: false
          }});
          assert.equal(input.value, '');
          assert.equal(input.disabled, false);
          assert.equal(button.disabled, false);
          assert.equal(settingsReloads, 1);
          assert.deepEqual(toasts, [{{
            message: 'Tiingo token saved to the credential vault.', level: 'success'
          }}]);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_tiingo_token_reveal_is_explicit_and_clears_when_hidden() -> None:
    """The eye action should POST one profile ID and remove the revealed value on hide."""
    source = PAGE.read_text(encoding="utf-8")
    clear_function = _extract_function(source, "clearTiingoRevealedToken")
    toggle_function = _extract_function(source, "toggleTiingoTokenVisible")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const button = {{
          disabled: false,
          textContent: '',
          classList: {{contains(value) {{ return value === 'pw-eye-btn'; }}}}
        }};
        const input = {{value: '', type: 'password', dataset: {{}}, nextElementSibling: button}};
        const document = {{getElementById(id) {{
          return id === 'settings-tiingo-token' ? input : null;
        }}}};
        const settingsState = {{
          tiingoConfigured: true,
          tiingoProfileId: 'tradfi_tiingo_1',
          tiingoRevealGeneration: 0
        }};
        const requests = [];
        const toasts = [];
        async function fetchApiKeysJson(path, options) {{
          requests.push({{path, options}});
          return {{value: 'stored-vault-token'}};
        }}
        function showToast(message, level) {{ toasts.push({{message, level}}); }}
        {clear_function}
        {toggle_function}
        (async () => {{
          await toggleTiingoTokenVisible(button);
          assert.equal(requests.length, 1);
          assert.equal(requests[0].path, '/tradfi/reveal');
          assert.equal(requests[0].options.method, 'POST');
          assert.deepEqual(JSON.parse(requests[0].options.body), {{profile_id: 'tradfi_tiingo_1'}});
          assert.equal(input.value, 'stored-vault-token');
          assert.equal(input.type, 'text');
          assert.equal(input.dataset.revealed, 'true');
          assert.equal(button.disabled, false);

          await toggleTiingoTokenVisible(button);
          assert.equal(input.value, '');
          assert.equal(input.type, 'password');
          assert.equal(input.dataset.revealed, undefined);
          assert.deepEqual(toasts, []);
        }})().catch(error => {{ console.error(error); process.exit(1); }});
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)
