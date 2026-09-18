"""Browser regressions for separated queue and Vast administration settings."""

import json

from test_vast_issue_recovery import ROOT, cloud_page
from test_remaining_issue_regressions import function


def test_independent_saves_preserve_hidden_and_inflight_edits(cloud_page):
    """Each form submits its own group and late saves cannot discard further typing."""
    page, _, calls, overrides, held = cloud_page
    page.locator('[data-vast-view=rental]').click()
    page.locator('#job-budget').fill('7')
    page.locator('[data-vast-view=offers]').click()
    page.locator('#min-cpu').fill('16')
    with page.expect_request('**/gpu-preferences') as pending:
        page.locator('#save-gpu-preferences').click()
    assert pending.value.method == 'PATCH'
    assert pending.value.post_data_json['min_cpu'] == 16
    assert 'budget' not in pending.value.post_data_json
    page.wait_for_function("!document.getElementById('save-gpu-preferences').disabled")
    page.locator('[data-vast-view=rental]').click()
    assert page.locator('#job-budget').input_value() == '7'
    overrides['/api/vast/gpu-preferences'] = 'hold'
    with page.expect_request('**/gpu-preferences') as pending:
        page.locator('#save-rental-preferences').click()
    assert pending.value.post_data_json['budget'] == 7
    assert 'min_cpu' not in pending.value.post_data_json
    assert page.locator('#save-rental-preferences').is_disabled()
    page.locator('#job-budget').fill('9')
    page.locator('[data-vast-view=offers]').click()
    assert page.locator('#save-gpu-preferences').is_disabled()
    held.pop().fulfill(json={**pending.value.post_data_json, 'gpu_name': '', 'min_cpu': 16})
    page.wait_for_function("!document.getElementById('save-gpu-preferences').disabled")
    assert page.locator('#min-cpu').input_value() == '16'
    page.locator('[data-vast-view=rental]').click()
    assert page.locator('#job-budget').input_value() == '9'
    assert not any('/queue/start' in url for _, url in calls)


def test_first_offer_visit_opens_account_without_a_key(cloud_page):
    """An unconfigured account is reachable first, then all five areas work normally."""
    page, _, _, _, _ = cloud_page
    page.wait_for_function("document.getElementById('key-status').textContent === 'No API key saved.'")
    page.locator('[data-vast-view=offers]').click()
    assert page.locator('#vast-setup').is_visible()
    page.locator('#api-key').fill('unsaved-test-value')
    page.locator('[data-vast-view=offers]').click()
    assert page.locator('#vast-offers').is_visible()
    assert page.locator('#api-key').input_value() == ''


def test_queue_settings_and_five_vast_views_are_separate(cloud_page):
    """Exercise the real optimizer panel switcher and component in the shared sidebar."""
    page, _, _, _, _ = cloud_page
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    sidebar = source[source.index('  <div id="sidebar">'):source.index('  <div id="main-content">')]
    panels = source[source.index('    <div id="panel-settings"'):source.index('    <div id="panel-queue"')]
    switcher = function('v7_optimize.html', 'setPanel')
    setup = '''document.addEventListener('DOMContentLoaded', () => {
      const host = document.getElementById('vast-queue-host'); host.remove();
      const t = document.createElement('template'); t.innerHTML = SIDEBAR + PANELS;
      document.body.prepend(t.content);
      document.getElementById('vast-queue-host').replaceWith(host);
      window.el = id => document.getElementById(id);
      window.optimizeEditorAdapter = {isV8:true};
      window.PANEL_META = {configs:{},queue:{},results:{},paretos:{},settings:{},vast:{}};
      for (const name of ['configs','queue','results','paretos']) {
        const p = document.createElement('div');p.id='panel-'+name;p.className='view-panel';document.body.append(p);
      }
      window.syncQueueSettingsModalFields = () => {};
      window.refreshLiveResultsDuringRun = window.restoreSelectedOptimizeResultWhenReady = async () => {};
      window.handleError = e => {throw e;};
      window.setPanel = (0,eval)('(' + SWITCHER + ')');
      el('btn-queue-settings').onclick = () => setPanel('settings');
      setPanel('queue');
    });'''.replace('SIDEBAR', json.dumps(sidebar)).replace('PANELS', json.dumps(panels)).replace('SWITCHER', json.dumps(switcher))
    page.add_init_script(setup)
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.add_style_tag(content='.view-panel{display:none}.view-panel.active{display:block}[hidden]{display:none!important}')
    page.locator('#btn-queue-settings').click()
    assert page.locator('#settings-cpu-value').is_visible()
    assert page.locator('#vast-offers').is_hidden()
    assert page.locator('#vast-settings-nav [data-vast-view]').evaluate_all(
        '(buttons) => buttons.map(button => button.dataset.vastView)'
    ) == ['account', 'offers', 'hosts', 'rental', 'performance']
    for view, target in [('account','vast-setup'), ('offers','vast-offers'), ('rental','vast-rental'), ('hosts','vast-hosts-filter'), ('performance','vast-performance')]:
        page.locator('[data-vast-view='+view+']').click()
        assert page.locator('#'+target).is_visible()
        assert page.locator('#settings-cpu-value').is_hidden()
        assert page.evaluate('location.hash') == '#vast-' + view
        assert page.locator('#vast-settings-nav .active').count() == 1
        assert page.locator('.sb-section[data-panel="queue"]').evaluate(
            '(button) => !button.classList.contains("active")'
        )
        assert page.locator('#vast-sidebar-controls button').count() == 5
        assert page.locator('#sidebar #vast-performance-actions').count() == 0
        if view == 'performance':
            assert page.locator('#vast-performance #vast-performance-actions').is_visible()
            assert page.locator('#vast-performance #performance-refresh').is_visible()
    page.locator('#btn-queue-settings').click()
    assert page.locator('#settings-cpu-value').is_visible()
    assert page.locator('#vast-settings-nav .active').count() == 0
    page.evaluate("setPanel('queue')")
    assert page.locator('.sb-section[data-panel="queue"]').evaluate(
        '(button) => button.classList.contains("active")'
    )
    assert page.locator('#vast-settings-nav').is_visible()
