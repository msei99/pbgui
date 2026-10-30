"""Browser checks of the real polling function with isolated in-memory API replies."""

from pathlib import Path

import pytest


def test_inventory_warning_repair_and_stale_poll_preserve_context():
    """Only the latest poll changes the warning; healthy selection and drafts survive."""
    browser_api = __import__('browser_driver')
    source = Path('frontend/js/vast.js').read_text()
    function = source[source.index('  async function refreshJobs(preferred,'):
                      source.index('  async function pollJobs()')]
    with browser_api.sync_browser() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('''<p id="cloud-inventory-warning" hidden></p>
                <p id="cloud-inventory-detail-warning" hidden></p>
                <input id="draft" value="unsaved GPU setting"><select id="filter">
                <option>All</option><option selected>Running</option></select>''')
            page.add_script_tag(content='''
                const el=id=>document.getElementById(id);
                let jobGeneration=0, jobTimer=null, disposed=false, jobRows=[], workers=[], worker=null;
                let queueState={}, supervision=false, selectedJobId='healthy', unreadableJobs=[];
                let calibrationWorkerAvailable=false, resolvers=[];
                const stoppingJobs=new Map(), uncertainStops=new Map(), pollJobs=()=>{};
                const reconcileExcludedOffers=()=>{};
                const setTimeout=()=>1, clearTimeout=()=>{};
                const request=()=>new Promise(resolve=>resolvers.push(resolve));
                const renderJob=()=>{}, renderQueueOverview=()=>{}, refreshActiveGpuProfile=()=>{};
                const completedCalibration=()=>false, message=()=>{}, refreshHostHistory=()=>{};
                const renderCloudDashboard=()=>{}, renderObservedOptimizer=()=>{};
                const reply=bad=>({jobs:[{id:'other',status:'completed'},{id:'healthy',status:'ready'}],
                    workers:[],worker:null,queue:{},supervision_available:false,
                    unreadable:bad?[{id:'cccccccccccccccccccccccccccccccc',category:'record_unreadable'}]:[]});
                window.state={selectedQueue:new Set(['healthy']),panel:'queue'};
            ''' + function)
            page.locator('#draft').fill('keep this unfinished edit')
            page.locator('#draft').focus()
            page.evaluate('window.first=refreshJobs(); resolvers.shift()(reply(true))')
            page.wait_for_function("!document.getElementById('cloud-inventory-warning').hidden")
            assert '1 unreadable record(s)' in page.locator('#cloud-inventory-warning').inner_text()
            assert page.evaluate('selectedJobId') == 'healthy'
            assert page.locator('#draft').input_value() == 'keep this unfinished edit'
            assert page.evaluate('document.activeElement.id') == 'draft'

            page.evaluate('window.old=refreshJobs(); window.newer=refreshJobs(); void 0')
            page.evaluate('resolvers[1](reply(false))')
            page.wait_for_function("document.getElementById('cloud-inventory-warning').hidden")
            page.evaluate('resolvers[0](reply(true))')
            page.evaluate('Promise.all([old,newer])')
            assert page.locator('#cloud-inventory-warning').is_hidden()
            assert page.locator('#cloud-inventory-detail-warning').is_hidden()
            assert page.evaluate('selectedJobId') == 'healthy'
            assert page.evaluate("Array.from(state.selectedQueue)") == ['healthy']
            assert page.evaluate('state.panel') == 'queue'
            assert page.locator('#filter').input_value() == 'Running'
            assert page.locator('#draft').input_value() == 'keep this unfinished edit'
        finally:
            browser.close()
