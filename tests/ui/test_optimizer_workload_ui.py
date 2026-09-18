"""Browser coverage for queue workload estimates and descriptive config comparison."""
import json

from test_vast_issue_recovery import ROOT, cloud_page, performance_rows


def test_cloud_queue_estimate_and_missing_metadata(cloud_page):
    """Queue estimates render as data, with a full-candidate explanation."""
    page, data, _, _, _ = cloud_page
    data['jobs'][0]['estimated_coin_candles'] = 11520000
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    cell = page.locator('#rows tr').first.locator('td').nth(6)
    assert '11' in cell.inner_text()
    assert 'not the total optimizer run' in cell.get_attribute('title')
    data['jobs'][0].pop('estimated_coin_candles')
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    assert page.locator('#rows tr').first.locator('td').nth(6).inner_text() == '—'


def test_different_configs_compare_without_unlocking_hardware(cloud_page):
    """Different workloads get explicit config comparison and measured-rate projections."""
    page, _, _, overrides, _ = cloud_page
    page.add_init_script(script=(ROOT/'frontend/js/vast_performance.js').read_text())
    rows = performance_rows()
    rows[1]['fingerprint'] = '0' * 64
    for row in rows:
        row['workload']['estimated_coin_candles'] = 11520000
        row['summary']['exact_per_minute'] = 100
        row['summary']['exact_per_usd'] = 2000
    overrides['/api/vast/performance'] = (200, json.dumps(dict(runs=rows,total=2)), {'Content-Type':'application/json'})
    overrides['/api/vast/performance/compare'] = (200, json.dumps(dict(runs=rows,mode='config')), {'Content-Type':'application/json'})
    page.reload()
    page.locator('[data-vast-view=performance]').click()
    page.locator('#performance-rows tr').nth(0).click()
    page.locator('#performance-rows tr').nth(1).click()
    assert page.locator('#performance-compare').is_disabled()
    assert page.locator('#performance-config-compare').is_enabled()
    with page.expect_request('**/performance/compare') as pending:
        page.locator('#performance-config-compare').click()
    assert pending.value.post_data_json['mode'] == 'config'
    page.locator('#performance-comparison').wait_for(state='visible')
    assert 'not an isolated hardware benchmark' in page.locator('#performance-status').inner_text()
    assert '10 minutes · $0.5000 compute' in page.locator('#performance-details').inner_text()
    assert page.locator('#performance-details img').count() == 0
