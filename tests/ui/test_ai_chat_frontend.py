"""Security and entry-point contracts for the former full chat, now AI Setup."""
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parents[2]
HTML=(ROOT/'frontend/ai_chat.html').read_text()
JS=(ROOT/'frontend/js/ai_setup.js').read_text()
DRAWER=(ROOT/'frontend/js/ai_drawer.js').read_text()


def test_setup_uses_shared_page_shell_and_local_assets():
    """The setup destination fills the normal shared shell rather than a chat modal."""
    for identifier in ['sidebar','sidebar-sticky','sidebar-header','sidebar-toolbar','sidebar-inner','sidebar-resize','main-content']:
        assert 'id="'+identifier+'"' in HTML
    assert 'sidebar_layout.css' in HTML and 'sidebar_resize.js' in HTML
    assert '<title>AI Setup' in HTML
    assert 'credentials:\'same-origin\'' in JS
    assert 'Authorization' not in HTML+JS and 'Bearer' not in HTML+JS
    assert 'sessionStorage' not in JS and 'localStorage' not in JS
    assert '/app/css/data_tip.css?v=1' in HTML and '/app/js/data_tip.js?v=1' in HTML
    assert 'details class="field-help"' not in HTML
    assert 'data-tip=' in HTML
    assert not re.search(r'<button\b[^>]*\bdata-tip=',HTML)


def test_setup_has_no_second_chat_or_conversation_actions():
    """The drawer owns conversations; setup cannot create, delete, rewind or submit turns."""
    for identifier in ['prompt','composer','conversation-list','messages','delete-chat','proposal-list']:
        assert 'id="'+identifier+'"' not in HTML
    assert '/conversations' not in JS and '/turns' not in JS
    assert "document.getElementById('pbgui-ai-btn')" in JS
    assert 'buildSupportConnection' not in DRAWER


def test_setup_credentials_stay_in_password_fields_and_are_cleared():
    """Provider keys and GitHub tokens never become model prompts or browser state."""
    for identifier in ['go-key','openrouter-key','github-token']:
        assert 'id="'+identifier+'" type="password"' in HTML
    assert "input.value=''" in JS and 'clearSecrets()' in JS
    assert "history.replaceState" in JS and 'secret' not in JS.split('function remember()',1)[1].split('function clearSecrets',1)[0]
    assert 'innerHTML' not in HTML+JS and 'document.write' not in HTML+JS
    assert 'window.confirm' not in JS and 'window.alert' not in JS


def test_setup_retains_profiles_native_login_usage_and_model_configuration():
    """Existing providers retain login, profile management, usage and model-specific controls."""
    for identifier in ['chatgpt-profile','chatgpt-profile-add','chatgpt-profile-rename','chatgpt-profile-remove','chatgpt-device-connect','jev-max-cost','openrouter-usage','go-usage','effort-select','speed-select']:
        assert 'id="'+identifier+'"' in HTML
    assert "model.reasoning_variants" in JS and 'model.service_tiers' in JS
    assert "'/test-model'" in JS and "selection:chosen" in JS
    assert '/providers/chatgpt/' in JS and 'device-login' in JS and 'browser-login' in JS
    assert 'refresh-health' not in HTML and 'Refresh' not in HTML


def test_setup_suspends_work_and_rejects_late_results():
    """Automatic updates preserve selections and clean up on hidden/unloaded pages."""
    assert 'modelGeneration' in JS and 'githubGeneration' in JS and 'testGeneration' in JS
    assert "visibilitychange" in JS and "pagehide" in JS and "pageshow" in JS
    assert 'controllers.forEach' in JS and 'controller.abort()' in JS
    assert "state.nameDirty" in JS and "url.searchParams.set('section'" in JS


def test_claude_has_no_credentials_or_usable_connection_controls():
    """Subscription controls never request an Anthropic API key or store login codes."""
    assert 'Sign in with Claude' in HTML
    for identifier in ('claude-code-connect','claude-code-disconnect','claude-code-test','claude-code-login-code'):
        assert 'id="'+identifier+'"' in HTML
    assert 'Anthropic API key' not in HTML
    assert "api('/providers/claude-code/login-code',json('POST',{code:code,login_id:loginId}))" in JS
    assert "'claude-code-login-code'" in JS.split('function clearSecrets()',1)[1].split('function selectSection',1)[0]
