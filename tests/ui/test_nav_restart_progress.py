"""Deterministic restart UI regressions without restarting real services."""

from javascript_checks import run_javascript
from pathlib import Path


def test_pending_inspection_does_not_invent_a_restart_requirement():
    """The real nav hides Restart for unknown scans while retaining proven updates."""
    source = Path('frontend/pbgui_nav.js').read_text()
    functions = []
    for name in ('visibleRestartServices', 'updateRestartButtonState'):
        begin = source.index('  function ' + name + '(')
        end = source.index('\n  }', begin) + 4
        functions.append(source[begin:end])
    code = r'''
const assert=browserAssert;
let _restartInFlight=false,_restartStatus={};
const attrs={},btn={style:{},classList:{toggle(){},add(){}},
 getAttribute:name=>attrs[name],setAttribute(name,value){attrs[name]=value}};
const document={getElementById:()=>btn};
const updateAuthModeState=()=>{},updateMasterName=()=>{};
const pending={needs_restart:false,api_restart_required:false,restart_services:[],
 restart_inspection_pending:true,restart_inspection_error:'inspection in progress'};
FUNCTIONS
updateRestartButtonState(pending);
assert.equal(btn.style.display,'none');
assert.equal(_restartStatus.restart_inspection_error,'inspection in progress');
updateRestartButtonState({...pending,needs_restart:true,api_restart_required:true});
assert.equal(btn.style.display,'flex');
assert.equal(btn.disabled,false);
updateRestartButtonState({needs_restart:false,restart_inspection_error:''});
assert.equal(btn.style.display,'none');
'''.replace('FUNCTIONS', '\n'.join(functions))
    result = run_javascript(['-e', code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_hidden_tabs_release_status_streams_and_ignore_late_messages():
    """Real watch callbacks release HTTP connections and restore current state on return."""
    source = Path('frontend/pbgui_nav.js').read_text()
    start = source.index('    function startRestartStatusWatch()')
    watch = source[start:source.index('\n  }\n', start)]
    functions = []
    for name in ('setupRestartSSE', 'stopRestartStatusWatch'):
        begin = source.index('  function ' + name + '(')
        end = source.index('\n  }', begin) + 4
        functions.append(source[begin:end])
    code = r'''
const assert=browserAssert;
let _restartInFlight=false,_restartEventSource=null,_restartRetryTimer=null,_restartPollTimer=null;
let requests=0,updates=0;
const apiOrigin='/prefix',listeners={},streams=[];
const document={hidden:true,addEventListener(name,fn){listeners[name]=fn}};
const window={addEventListener(){}};
const setInterval=()=>1,clearInterval=()=>{},setTimeout=()=>2,clearTimeout=()=>{};
const fetchRestartStatus=()=>requests++,updateRestartButtonState=()=>updates++;
const EventSource=class{constructor(url){this.url=url;this.closed=false;streams.push(this)}close(){this.closed=true}};
FUNCTIONS
WATCH
assert.equal(streams.length,0);assert.equal(requests,0);
document.hidden=false;listeners.visibilitychange();
assert.equal(streams.length,1);assert.equal(requests,1);
const old=streams[0];
document.hidden=true;listeners.visibilitychange();
assert.equal(old.closed,true);assert.equal(_restartEventSource,null);
old.onmessage({data:'{"needs_restart":true}'});assert.equal(updates,0);
document.hidden=false;listeners.visibilitychange();
assert.equal(streams.length,2);assert.equal(requests,2);
streams[1].onmessage({data:'{}'});assert.equal(updates,1);
_restartInFlight=true;listeners.visibilitychange();
assert.equal(streams[1].closed,true);assert.equal(streams.length,2);
'''.replace('FUNCTIONS', '\n'.join(functions)).replace('WATCH', watch)
    result = run_javascript(['-e', code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_restart_waits_for_new_instance_and_keeps_button_busy():
    """Status races cannot unlock restart or mistake the old API for a completed restart."""
    source = Path('frontend/pbgui_nav.js').read_text()
    functions = []
    for name in ('showRestartOverlay', 'visibleRestartServices', 'updateRestartButtonState'):
        start = source.index('  function ' + name + '(')
        end = source.index('\n  }', start) + 4
        functions.append(source[start:end])
    code = r'''
const assert=browserAssert;
let _restartInFlight=true,_restartStatus={},reloads=0,timers=[],requests=0;
const nodes={};
function node(){return {style:{},classList:{add(){},toggle(){}},setAttribute(){},getAttribute(){return ''},remove(){},appendChild(){},addEventListener(){}};}
nodes['pbgui-restart-btn']=node();nodes['pbgui-restart-status']=node();
const document={getElementById:id=>nodes[id],createElement:node,body:{appendChild(n){nodes[n.id]=n}}};
const window={location:{origin:'http://test',reload(){reloads++}}};
const updateAuthModeState=()=>{}, updateMasterName=()=>{}, authOptions=x=>x;
const setTimeout=(fn)=>{timers.push(fn);return fn};
const clearTimeout=(fn)=>{const index=timers.indexOf(fn);if(index>=0)timers.splice(index,1)};
let status={needs_restart:false,api_instance_id:'old'};
const fetch=async()=>{requests++;return {ok:true,json:async()=>status}};
const flush=()=>new Promise(r=>setImmediate(r));
(async()=>{
  updateRestartButtonState({needs_restart:false});
  assert.equal(nodes['pbgui-restart-btn'].disabled,true);
  assert.match(nodes['pbgui-restart-btn'].innerHTML,/Restarting/);
  showRestartOverlay('http://test',[],'old',true);
  assert.equal(timers.length,0);assert.equal(requests,0);
  assert.match(nodes['pbgui-restart-status'].textContent,/Requesting/);
  showRestartOverlay('http://test',[],'old');
  status={needs_restart:true,api_instance_id:'old'};
  timers.shift()();await flush();
  assert.equal(reloads,0);assert.equal(timers.length,1);
  status={needs_restart:false,api_instance_id:'old'};
  timers.shift()();await flush();assert.equal(reloads,0);
  status={needs_restart:false,api_instance_id:'new',restart_inspection_error:'unavailable'};
  timers.shift()();await flush();assert.equal(reloads,0);
  status={needs_restart:false,api_instance_id:'new'};
  timers.shift()();await flush();assert.equal(reloads,1);
})();
'''
    result = run_javascript(['-e', '\n'.join(functions) + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_confirmed_restart_is_visible_during_delayed_request_and_sent_once():
    """Duplicate clicks are ignored and rejected/lost replies have distinct recovery."""
    source = Path('frontend/pbgui_nav.js').read_text()
    block = source[source.index('    /* Restart button */'):source.index('    function startRestartStatusWatch()')]
    for name, indent in (('visibleRestartServices', '  '), ('vastRestartContext', '    ')):
        start = source.index(indent + 'function ' + name + '(')
        end = source.index('\n' + indent + '}', start) + len(indent) + 2
        block = source[start:end] + '\n' + block
    code = r'''
const assert=browserAssert;
let _restartInFlight=false,_restartConfirmPending=false,_restartStatus={api_instance_id:'old'};
let click,confirm,reply,requests=0,overlays=[],errors=[],removed=0,watchStops=0,watchStarts=0;
const stopRestartStatusWatch=()=>watchStops++,startRestartStatusWatch=()=>watchStarts++;
const setTimeout=()=>1,clearTimeout=()=>{};
const AbortController=class{constructor(){this.signal={}}abort(){}};
const btn={getAttribute(key){return key==='data-restart-blocked'?'0':''},classList:{add(){},remove(){}},addEventListener(_,fn){click=fn}};
const document={getElementById(id){return id==='pbgui-restart-btn'?btn:{remove(){removed++}}}};
const showNavConfirm=opts=>{
 if(opts.title==='Restart failed'){errors.push(opts);return Promise.resolve(true)}
 assert.equal(opts.title,'Restart PBGui services');
 assert.match(opts.detail,/API server restarts/);
 return new Promise(r=>confirm=r);
};
const _getAppBase=()=>'/prefix',authOptions=x=>x,fetchRestartStatus=()=>{};
const showRestartOverlay=(...args)=>overlays.push(args);
const fetch=()=>{requests++;return new Promise((resolve,reject)=>reply={resolve,reject})};
const flush=()=>new Promise(r=>setImmediate(r));
INSTALL
(async()=>{
 click();const firstConfirm=confirm;click();assert.equal(confirm,firstConfirm);
 confirm(true);await flush();
 assert.equal(requests,1);assert.equal(overlays[0][3],true);

 click();assert.equal(requests,1);
 reply.resolve({ok:false,json:async()=>({detail:'Protected job running'})});await flush();
 assert.equal(_restartInFlight,false);assert.equal(btn.disabled,false);assert.equal(removed,1);
 assert.equal(errors[0].detail,'Protected job running');

 click();confirm(true);await flush();
 reply.reject(new Error('Connection closed'));await flush();
 assert.equal(requests,2);assert.equal(_restartInFlight,true);
 assert.equal(errors.length,1);assert.equal(overlays.at(-1)[2],'old');
})();
'''.replace('INSTALL', block)
    result = run_javascript(['-e', code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_restart_button_is_visible_in_complete_navigation_script():
    """The actual navigation scope must render the restart requirement from the API."""
    import json
    import pytest

    browser_api = __import__('browser_driver')
    source = Path('frontend/pbgui_nav.js').read_text()
    payload = {'needs_restart': True, 'api_restart_required': True,
               'restart_services': [{'service': 'PBApiServer', 'label': 'PBGui API Server'}]}
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page()
        requests = []

        def respond(route):
            """Serve isolated navigation and API data without contacting any running service."""
            url = route.request.url
            requests.append((route.request.method, url))
            if url.endswith('/nav.js'):
                route.fulfill(body=source, content_type='text/javascript')
            elif url.endswith('/api/server-status/stream'):
                route.fulfill(body='data: ' + json.dumps(payload) + '\n\n', content_type='text/event-stream')
            elif url.endswith('/api/server-status'):
                route.fulfill(json=payload)
            elif '/api/' in url:
                route.fulfill(json={})
            else:
                route.fulfill(body='''<nav id="topnav"></nav><script>
                    window.PBGUI_NAV_CONFIG={authenticated:true,current:'system_services'};
                    </script><script src="/nav.js"></script>''', content_type='text/html')

        page.route('**/*', respond)
        page.goto('http://pbgui.test/')
        page.locator('#pbgui-restart-btn').wait_for(state='visible', timeout=1500)
        assert 'PBGui API Server' in page.locator('#pbgui-restart-btn').get_attribute('title')
        assert not any(method == 'POST' and 'restart' in url for method, url in requests)
        browser.close()




def test_restart_serial_change_restarts_stale_services_once():
    """A code update during restart gets one follow-up and waits for its new API."""
    source = Path('frontend/pbgui_nav.js').read_text()
    start = source.index('  function showRestartOverlay(')
    end = source.index('\n  }', start) + 4
    code = r'''
const assert=browserAssert;
let timers=[],posts=0,reloads=0;
const statusNode={};
const node=()=>({style:{},remove(){},appendChild(){},addEventListener(){}});
const document={getElementById:id=>id==='pbgui-restart-status'?statusNode:null,createElement:node,body:{appendChild(){}}};
const window={location:{origin:'http://test',reload(){reloads++}}};
const authOptions=x=>x;
const setTimeout=fn=>{timers.push(fn);return fn};
const clearTimeout=fn=>{const index=timers.indexOf(fn);if(index>=0)timers.splice(index,1)};
let status={needs_restart:true,api_instance_id:'old',current_serial:11,service_restart_required:true,
 restart_services:[{service:'PBRun',label:'PBRun',running_serial:10,current_serial:11}]};
const fetch=async(url)=>{
 if(url.endsWith('/server-restart')){posts++;return {ok:true,json:async()=>({api_instance_id:'new'})}}
 return {ok:true,json:async()=>status};
};
const flush=()=>new Promise(r=>setImmediate(r));
async function tick(){timers.shift()();await flush()}
(async()=>{
 showRestartOverlay('http://test',['PBRun'],'old',false,10);
 await tick();assert.equal(posts,0);assert.match(statusNode.textContent,/PBRun.*10 → 11/);
 status.api_instance_id='new';status.restart_inspection_error='not ready';
 await tick();assert.equal(posts,0);
 delete status.restart_inspection_error;
 await tick();assert.equal(posts,1);assert.equal(reloads,0);
 status.current_serial=12;
 await tick();assert.equal(posts,1);assert.equal(reloads,0);
 status={needs_restart:false,api_instance_id:'new',current_serial:12};
 await tick();assert.equal(reloads,0);
 status.api_instance_id='new2';await tick();assert.equal(reloads,1);
 // A failed same-version service never causes a blind repeat request.
 timers=[];status={needs_restart:true,api_instance_id:'new3',current_serial:12,
 service_restart_required:true,restart_services:[{label:'PBRun',running_serial:11,current_serial:12}]};
 showRestartOverlay('http://test',['PBRun'],'new2',false,12);
 for(let i=0;i<60;i++)await tick();
 assert.equal(posts,1);assert.match(statusNode.textContent,/could not be verified.*PBRun.*11 → 12/);
})();
'''
    result = run_javascript(['-e', source[start:end] + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr



def test_unanswered_restart_probe_expires_and_stale_overlay_stops():
    """A hanging GET cannot freeze attempt one or revive a removed overlay."""
    source = Path('frontend/pbgui_nav.js').read_text()
    start = source.index('  function showRestartOverlay(')
    end = source.index('\n  }', start) + 4
    code = r'''
const assert=browserAssert;
let timers=[],requests=0,reloads=0,overlay;
const statusNode={};
const node=()=>({style:{},isConnected:true,remove(){this.isConnected=false},appendChild(){},addEventListener(){}});
const document={getElementById:id=>id==='pbgui-restart-status'?statusNode:null,createElement:node,body:{appendChild(n){overlay=n}}};
const window={location:{origin:'http://test',reload(){reloads++}}};
const authOptions=x=>x;
const setTimeout=fn=>{timers.push(fn);return fn};
const clearTimeout=fn=>{const index=timers.indexOf(fn);if(index>=0)timers.splice(index,1)};
const fetch=(url,options)=>{requests++;return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new Error('aborted'))))};
const flush=()=>new Promise(r=>setImmediate(r));
(async()=>{
 showRestartOverlay('http://test',[],'old',false,10);
 timers.shift()();await flush();assert.equal(requests,1);
 assert.match(statusNode.textContent,/1\/60/);
 timers.shift()();await flush();assert.equal(timers.length,1);
 timers.shift()();await flush();assert.equal(requests,2);
 assert.match(statusNode.textContent,/2\/60/);
 overlay.remove();timers.shift()();await flush();
 assert.equal(timers.length,0);assert.equal(reloads,0);
})();
'''
    result = run_javascript(['-e', source[start:end] + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_protected_work_wait_does_not_expire_or_resubmit_restart():
    """A long native inspection consumes no reconnect budget and never sends another POST."""
    source = Path('frontend/pbgui_nav.js').read_text()
    start = source.index('  function showRestartOverlay(')
    end = source.index('\n  }', start) + 4
    code = r'''
const assert=browserAssert;
let timers=[],reloads=0,requests=0;
const statusNode={};
const node=()=>({style:{},remove(){},appendChild(){},addEventListener(){}});
const document={getElementById:id=>id==='pbgui-restart-status'?statusNode:null,createElement:node,body:{appendChild(){}}};
const window={location:{origin:'http://test',reload(){reloads++}}};
const authOptions=x=>x;
const setTimeout=fn=>{timers.push(fn);return fn};
const clearTimeout=fn=>{const index=timers.indexOf(fn);if(index>=0)timers.splice(index,1)};
const AbortController=class{constructor(){this.signal={}}abort(){}};
let status={needs_restart:true,api_instance_id:'old',restart_request:{state:'waiting',reason:'PB9 inspection in progress'}};
const fetch=async(_url,options)=>{assert.notEqual(options.method,'POST');requests++;return {ok:true,json:async()=>status}};
const flush=()=>new Promise(r=>setImmediate(r));
(async()=>{
 showRestartOverlay('http://test',[],'old');
 for(let i=0;i<125;i++){assert.equal(timers.length,1);timers.shift()();await flush();}
 assert.equal(reloads,0);assert.equal(requests,125);
 assert.match(statusNode.textContent,/queued.*PB9 inspection.*automatically/);
 status={needs_restart:false,api_instance_id:'new'};
 timers.shift()();await flush();assert.equal(reloads,1);
})();
'''
    result = run_javascript(['-e', source[start:end] + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_busy_restart_button_queues_and_browser_reload_restores_progress():
    """Click the actual shared nav control; a busy inspection never needs a second click."""
    import json
    from browser_driver import sync_browser
    source = Path('frontend/pbgui_nav.js').read_text()
    payload = {'needs_restart': True, 'api_restart_required': True, 'api_instance_id': 'old',
               'restart_blocked': True, 'restart_block_reason': 'PB9 inspection in progress',
               'restart_services': [{'service': 'PBAPIServer', 'label': 'PBGui API Server'}]}
    posts = []
    with sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page()
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))

        def serve(route):
            """Intercept every request, including restart, without contacting any live API."""
            url = route.request.url
            if url.endswith('/nav.js'):
                route.fulfill(body=source, content_type='text/javascript')
            elif url.endswith('/api/server-restart'):
                posts.append(url)
                payload['restart_request'] = {'state': 'waiting', 'reason': 'PB9 inspection in progress'}
                route.fulfill(json={'ok': True, 'api_instance_id': 'old', 'restart_request': payload['restart_request']})
            elif url.endswith('/api/server-status/stream'):
                route.fulfill(body='data: ' + json.dumps(payload) + '\n\n', content_type='text/event-stream')
            elif url.endswith('/api/server-status'):
                route.fulfill(json=payload)
            elif '/api/' in url and '/edit_page' not in url:
                route.fulfill(json={})
            else:
                route.fulfill(body='''<nav id="topnav"></nav><script>
                    window.PBGUI_NAV_CONFIG={authenticated:true,current:'v9_run'};
                    </script><script src="/nav.js"></script>''', content_type='text/html')

        page.route('**/*', serve)
        destination = 'http://pbgui.test/api/v9/edit_page?name=fixture#editor'
        page.goto(destination)
        button = page.locator('#pbgui-restart-btn')
        button.wait_for(state='visible', timeout=2000)
        assert button.get_attribute('aria-disabled') == 'false'
        button.click()
        page.locator('#pbgui-confirm-accept').click()
        page.wait_for_function("document.getElementById('pbgui-restart-status')?.textContent.includes('PB9 inspection')", timeout=8000)
        assert len(posts) == 1
        page.reload()
        page.wait_for_function("document.getElementById('pbgui-restart-status')?.textContent.includes('PB9 inspection')", timeout=8000)
        assert len(posts) == 1
        assert page.url == destination
        assert not errors
        browser.close()
