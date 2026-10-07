"""Copy Data schedule deletion requires confirmation of a displayed schedule."""

import json
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]


def _excerpt(source, start, end):
    """Extract actual page code between stable neighboring source markers."""
    offset = source.index(start)
    return source[offset:source.index(end, offset)]


@pytest.mark.parametrize('mode', [
    'accept', 'cancel', 'missing', 'disappeared', 'fallback', 'duplicate',
    'network', '404', '409', 'unavailable-dialog', 'other-editor',
])
def test_schedule_delete_confirmation_lifecycle(mode):
    """Defer dialog, DELETE and reconciliation to test guard ownership and state."""
    source = (ROOT / 'frontend/market_data_main.html').read_text()
    deletion = _excerpt(source, '      async function deleteCopyDataSchedule(',
                        '      function resetCopyDataDryRunSummary(')
    script = 'const mode = ' + json.dumps(mode) + ';\n' + deletion + r'''
const assert = require('node:assert/strict');
const id = 'schedule / &';
const schedule = {id, name:'Production Hourly Sync',target:'user@remote-vps',interval_hours:1};
if (mode === 'fallback') {delete schedule.name;delete schedule.target;delete schedule.interval_hours;}
const copyDataScheduleState = {schedules:mode === 'missing' ? [] : [schedule],
    deletePending:false,editingId:mode === 'other-editor' ? 'other' : id};
const editor = {draft:'unsaved input'}, confirmations=[], requests=[], loads=[], feedback=[], toasts=[];
let resets=0;
function showConfirmDialog(options) {
    if(mode === 'unavailable-dialog') return Promise.resolve(false);
    return new Promise(resolve=>confirmations.push({options,resolve}));
}
function fetchCopyDataScheduleJson(path,options) {return new Promise((resolve,reject)=>requests.push({path,options,resolve,reject}));}
function loadCopyDataSchedules(showErrors) {return new Promise(resolve=>loads.push({showErrors,resolve}));}
function resetCopyDataScheduleEditor() {resets++;editor.draft='';copyDataScheduleState.editingId='';}
function setCopyDataFeedback(message,type) {feedback.push({message,type});}
function showToast(message,type) {toasts.push({message,type});}
const tick = async()=>{await Promise.resolve();await Promise.resolve();};
(async()=>{
 const pending=deleteCopyDataSchedule(id);
 assert.equal(requests.length,0,'DELETE must wait for confirmation');
 if(mode === 'missing') {
   assert.equal(confirmations.length,0);assert.equal(loads.length,1);assert.equal(loads[0].showErrors,true);
   assert.match(feedback[0].message,/no longer available/);assert.equal(toasts[0].message,feedback[0].message);
   loads[0].resolve();await pending;
 } else if(mode === 'unavailable-dialog') {
   await pending;assert.equal(loads.length,0);
 } else {
   assert.equal(confirmations.length,1);assert.equal(copyDataScheduleState.deletePending,true);
   assert.equal(editor.draft,'unsaved input');
   const options=confirmations[0].options;
   assert.equal(options.title,'Delete schedule');assert.equal(options.confirmText,'Delete schedule');
   assert.equal(options.message,'Delete recurring Copy Data schedule "'+(schedule.name||id)+'"?');
   assert.equal(options.detail,mode === 'fallback' ? 'Target: Unknown target · Every ?h' : 'Target: user@remote-vps · Every 1h');
   assert.ok(!options.detail.includes('cannot be undone'),'existing warning must not be duplicated');
   await deleteCopyDataSchedule(id);assert.equal(confirmations.length,1);
   if(mode === 'disappeared') copyDataScheduleState.schedules=[];
   confirmations[0].resolve(mode !== 'cancel');await tick();
   if(mode === 'cancel') {await pending;assert.equal(requests.length,0);assert.equal(loads.length,0);}
   else if(mode === 'disappeared') {
     assert.equal(requests.length,0);assert.equal(loads.length,1);assert.equal(loads[0].showErrors,true);
     assert.match(feedback[0].message,/no longer available/);assert.equal(toasts[0].message,feedback[0].message);
     loads[0].resolve();await pending;
   } else {
     assert.equal(requests.length,1);assert.equal(requests[0].options.method,'DELETE');
     assert.equal(requests[0].path,'/copy-data/schedules/'+encodeURIComponent(id));
     await deleteCopyDataSchedule(id);assert.equal(requests.length,1);
     if(['network','404','409'].includes(mode)) {
       requests[0].reject(new Error(mode));await pending;
       assert.equal(resets,0);assert.equal(loads.length,0);
       assert.equal(feedback[0].message,mode);assert.equal(feedback[0].type,'error');
       assert.equal(toasts[0].message,mode);assert.equal(toasts[0].type,'error');
     } else {
       requests[0].resolve({success:true});await tick();
       assert.equal(loads.length,1);assert.equal(loads[0].showErrors,false);
       assert.equal(resets,mode === 'other-editor' ? 0 : 1);
       await deleteCopyDataSchedule(id);assert.equal(requests.length,1);assert.equal(confirmations.length,1);
       loads[0].resolve();await pending;
     }
   }
 }
 assert.equal(copyDataScheduleState.deletePending,false,'guard must release on every exit');
 if(resets === 0) assert.equal(editor.draft,'unsaved input');
 // A fresh invocation must work after cancellation, failure or reconciliation.
 copyDataScheduleState.schedules=[schedule];
 if(mode !== 'unavailable-dialog') {
   const count=confirmations.length;const retry=deleteCopyDataSchedule(id);
   assert.equal(confirmations.length,count+1);confirmations[count].resolve(false);await retry;
 }
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize('viewport', [{'width':1200,'height':800}, {'width':375,'height':667}])
def test_rendered_schedule_delete_uses_existing_dialog(viewport):
    """Click real rendered actions and measure the existing modal with isolated data."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/market_data_main.html').read_text()
    css = _excerpt(source, '<style>', '</style>') + '</style>'
    modal = _excerpt(source, '  <div id="confirm-ovl"', '  <div id="page-body">')
    renderer = _excerpt(source, '      function copyDataScheduleTime(', '      async function loadCopyDataSchedules(')
    deletion = _excerpt(source, '      async function deleteCopyDataSchedule(', '      function resetCopyDataDryRunSummary(')
    dialogs = _excerpt(source, '      function closeConfirmDialog(', '      function openInventoryDeleteOlderDialog(')
    clicks = _excerpt(source, "        document.getElementById('copy-data-schedule-list').addEventListener('click'",
                      "        document.getElementById('best1m-coin-filter').addEventListener('input'")
    buttons = _excerpt(source, "        document.getElementById('confirm-close').addEventListener('click'",
                       "        document.getElementById('btn-inventory-clear-dataset').addEventListener('click'")
    key_marker = "        document.addEventListener('keydown', function (event) {\n          var overlay = document.getElementById('confirm-ovl');"
    keys = _excerpt(source, key_marker, '        settingsSubsectionButtons.forEach(')
    setup = r'''
      var uiState = {}, requests = [], loads = [], actions = [];
      var copyDataScheduleState = {deletePending:false,editingId:'',schedules:[{
        id:'example-schedule',name:'Production hourly synchronization for optimizer systems',
        target:'user@remote-vps',interval_hours:1,enabled:true,destination_root:'/data/ohlcv',exchanges:['binance']
      }]};
      function esc(value) {return String(value).replace(/[&<>"']/g, ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));}
      function runCopyDataSchedule(id) {actions.push(['run',id]);}
      function editCopyDataSchedule(id) {actions.push(['edit',id]);}
      function resetCopyDataScheduleEditor() {throw new Error('unexpected editor reset');}
      function setCopyDataFeedback() {}
      function showToast() {}
      async function fetchCopyDataScheduleJson(path,options) {requests.push({path,options});return {success:true};}
      async function loadCopyDataSchedules(showErrors) {loads.push(showErrors);}
    '''
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport=viewport)
        page.route('**/*', lambda route: route.abort())
        page.set_content(css + modal + '<div class="copy-data-schedule-list" id="copy-data-schedule-list"></div>')
        page.evaluate(setup + renderer + deletion + dialogs + clicks + buttons + keys + 'renderCopyDataSchedules();')
        page.locator('[data-copy-schedule-action="run"]').click()
        page.locator('[data-copy-schedule-action="edit"]').click()
        assert page.evaluate('actions') == [['run','example-schedule'],['edit','example-schedule']]
        for dismissal in ('cancel', 'close', 'escape'):
            page.locator('[data-copy-schedule-action="delete"]').click()
            playwright.expect(page.locator('#confirm-ovl')).to_have_class('visible')
            assert page.evaluate('requests.length') == 0
            box = page.locator('#confirm-ovl .ovl-panel').bounding_box()
            assert abs(box['x'] + box['width']/2 - viewport['width']/2) <= 1
            assert abs(box['y'] + box['height']/2 - viewport['height']/2) <= 1
            assert box['x'] >= 0 and box['y'] >= 0
            assert box['x'] + box['width'] <= viewport['width']
            assert box['y'] + box['height'] <= viewport['height']
            assert page.locator('.confirm-body').evaluate('(el)=>el.scrollWidth <= el.clientWidth')
            playwright.expect(page.locator('#btn-confirm-accept')).to_be_visible()
            playwright.expect(page.locator('#btn-confirm-cancel')).to_be_visible()
            page.mouse.click(2, 2)
            playwright.expect(page.locator('#confirm-ovl')).to_have_class('visible')
            if dismissal == 'escape':
                page.keyboard.press('Escape')
            else:
                page.locator('#btn-confirm-cancel' if dismissal == 'cancel' else '#confirm-close').click()
            playwright.expect(page.locator('#confirm-ovl')).not_to_have_class('visible')
            assert page.evaluate('requests.length') == 0
            assert page.evaluate('copyDataScheduleState.deletePending') is False
        # Untrusted metadata must remain text in the real dialog, not markup.
        page.evaluate('copyDataScheduleState.schedules[0].name = \'<img src=x onerror="throw 1">\';')
        page.locator('[data-copy-schedule-action="delete"]').click()
        playwright.expect(page.locator('#confirm-message')).to_have_text('Delete recurring Copy Data schedule "<img src=x onerror="throw 1">"?')
        assert page.locator('#confirm-message img').count() == 0
        page.locator('#btn-confirm-accept').click()
        page.wait_for_function('!copyDataScheduleState.deletePending')
        assert page.evaluate('requests.length') == 1
        assert page.evaluate('loads') == [False]
        # Missing dialog elements must fail closed through the actual helper.
        page.locator('#confirm-title').evaluate('(el)=>el.remove()')
        page.locator('[data-copy-schedule-action="delete"]').click()
        page.wait_for_function('!copyDataScheduleState.deletePending')
        assert page.evaluate('requests.length') == 1
        browser.close()
