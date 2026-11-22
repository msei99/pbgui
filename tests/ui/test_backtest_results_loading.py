"""Isolated real-browser regressions for large Backtest result lists and failed reads."""
from pathlib import Path

import pytest

from javascript_checks import run_javascript
from tests.ui.test_backtest_v8_frontend_logic import _extract_function

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'frontend/v7_backtest.html'


def test_result_paging_yield_finishes_when_animation_frames_are_suspended():
    """A real browser with paused paints must still continue loading the next page."""
    function = _extract_function(SOURCE.read_text(), 'waitForResultsPaint')
    script = '''
    const assert=browserAssert;
    const originalFrame=window.requestAnimationFrame, originalCancel=window.cancelAnimationFrame;
    let cancelled=[],nextPageRead=false;
    window.requestAnimationFrame=()=>12345; // Never calls back: an occluded browser window.
    window.cancelAnimationFrame=id=>cancelled.push(id);
    ''' + function + '''
    (async()=>{
      try {
        const continued=await Promise.race([
          waitForResultsPaint().then(()=>{nextPageRead=true;return true;}),
          new Promise(resolve=>window.setTimeout(()=>resolve(false),1500))
        ]);
        assert.equal(continued,true,'A paused animation frame must not stall pagination');
        assert.equal(nextPageRead,true);
        assert.deepEqual(cancelled,[12345]);
      } finally {
        window.requestAnimationFrame=originalFrame;window.cancelAnimationFrame=originalCancel;
      }
    })().catch(error=>{console.error(error);finishTest(1);});
    '''
    result = run_javascript(['-e', script], cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout


@pytest.mark.parametrize('failure', ['none', 'timeout', 'busy', 'invalid', 'unavailable'])
def test_large_result_load_batches_table_rebuilds_but_keeps_all_rows(failure):
    """Small network pages must not repeatedly rebuild a growing large table."""
    source = SOURCE.read_text()
    functions = source[source.index('var backtestReads='):source.index("window.addEventListener('pagehide'", source.index('var backtestReads='))]
    functions += _extract_function(source, 'loadResults')
    script = '''
    const assert=browserAssert,backtestEditorAdapter={metadataDriven:false};
    document.body.innerHTML='<select id="results-config-filter"></select><div id="results-list"></div><div id="results-count-label"></div>';
    let results=[],RESULTS_PAGE_SIZE=100,_pendingResultFilter='',_resultsReadController=null;
    let _resultsLoadGeneration=0,_resultsEmptyRetryTimer=null,_resultsEmptyRetryCount=0,currentPanel='results';
    let paints=[],requests=[],notices=[],failed=false;const failure=FAILURE_MODE;
    function loopEvaluationSelection(){return null;}
    function selectedResultsVersion(){return 'v8';}
    function resultsForSelectedVersion(items){return items;}
    function backtestApiBase(version){return '/api/backtest-'+version;}
    function hideResultsCountLabel(){}
    function toast(message){notices.push(message);}
    function _applyResultsData(items){results=items;paints.push(items.length);return items;}
    function apiFetchFrom(base,path,options){
      const offset=Number(new URL('http://fixture'+path).searchParams.get('offset'));
      requests.push(offset);
      if(offset===100 && failure!=='none' && (!failed || failure==='unavailable')) {
        failed=true;const error=new Error('fixture failure');
        if(failure==='timeout')error.code='backtest_read_timeout';
        else error.status=failure==='invalid'?422:503;
        return Promise.reject(error);
      }
      return Promise.resolve({results:Array.from({length:100},(_,i)=>({path:'/result-'+(offset+i)})),
        pagination:{total:2500,has_more:offset+100<2500,next_offset:offset+100,snapshot:'a'.repeat(32)}});
    }
    ''' + functions + '''
    (async()=>{
      if(failure==='invalid' || failure==='unavailable') {
        let rejected=false;try{await loadResults();}catch(error){rejected=true;}
        assert.equal(rejected,true);assert.equal(results.length,100);
        assert.equal(requests.length,failure==='invalid'?2:4);
        assert.match(document.getElementById('results-count-label').textContent,/Loading stopped/);
        assert.equal(_resultsReadController,null);return;
      }
      await loadResults();
      assert.equal(results.length,2500);
      assert.equal(new Set(results.map(row=>row.path)).size,2500);
      assert.equal(requests.length,failure==='none'?25:26);
      if(failure!=='none')assert.equal(requests[1],requests[2]);
      assert.deepEqual(notices,[]);
      assert.equal(paints[0],100);assert.equal(paints[paints.length-1],2500);
      assert.ok(paints.length<=6,'Large lists need a few table updates, not one per network page');
      assert.equal(_resultsReadController,null);
      assert.equal(backtestReads.size,0);
    })().catch(error=>{console.error(error);finishTest(1);});
    '''
    script=script.replace('FAILURE_MODE',repr(failure))
    result = run_javascript(['-e', script], cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout


@pytest.mark.parametrize('honor_abort', [False, True])
def test_new_result_load_aborts_old_generation_and_ignores_late_responses(honor_abort):
    """An obsolete All request cannot keep work alive or replace the newer PB9 view."""
    source = SOURCE.read_text()
    functions = source[source.index('var backtestReads='):source.index("window.addEventListener('pagehide'", source.index('var backtestReads='))]
    functions += _extract_function(source, 'loadResults')
    script = '''
    const assert=browserAssert, backtestEditorAdapter={metadataDriven:false};
    const host=document.createElement('div');
    host.innerHTML='<select id="results-config-filter"></select><div id="results-list"></div><div id="results-count-label"></div>';
    document.body.appendChild(host);
    let results=[], RESULTS_PAGE_SIZE=100, _pendingResultFilter='', _resultsReadController=null;
    let _resultsLoadGeneration=0, _resultsEmptyRetryTimer=null, _resultsEmptyRetryCount=0, currentPanel='results';
    let selectedVersion='both', pending=[], applied=[], notices=[];
    function loopEvaluationSelection(){return null;}
    function selectedResultsVersion(){return selectedVersion;}
    function resultsForSelectedVersion(items){return items;}
    function backtestApiBase(version){return '/api/backtest-'+version;}
    function hideResultsCountLabel(){}
    function toast(message){notices.push(message);}
    function _applyResultsData(items){results=items;applied.push(items.map(item=>item.path));return items;}
    function apiFetchFrom(base,path,options){
      return new Promise((resolve,reject)=>{
        pending.push({base,path,signal:options.signal,resolve});
        if(HONOR_ABORT) options.signal.addEventListener('abort',()=>reject(new DOMException('cancelled','AbortError')),{once:true});
      });
    }
    '''.replace('HONOR_ABORT', 'true' if honor_abort else 'false') + functions + '''
    (async()=>{
      const old=loadResults();
      assert.equal(pending.length,2);
      const obsolete=pending.slice();
      selectedVersion='v9';
      const current=loadResults();
      assert.ok(obsolete.every(request=>request.signal.aborted));
      const latest=pending[pending.length-1];
      assert.match(latest.path,/limit=100/);
      latest.resolve({results:[{path:'/new-native',backtest_version:'v9'}],pagination:{total:1,has_more:false}});
      await current;
      if(!HONOR_ABORT)obsolete.forEach(request=>request.resolve({results:[{path:'/obsolete'}],pagination:{total:1,has_more:false}}));
      await old;
      assert.deepEqual(results.map(item=>item.path),['/new-native']);
      assert.deepEqual(applied,[['/new-native']]);
      assert.deepEqual(notices,[]);
      assert.equal(backtestReads.size,0);
      assert.equal(_resultsReadController,null);
      host.remove();
    })().catch(error=>{console.error(error);finishTest(1);});
    '''.replace('HONOR_ABORT', 'true' if honor_abort else 'false')
    result = run_javascript(['-e', script], cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout


def test_large_config_selector_is_updated_once_and_keeps_selection():
    """Thousands of names require one DOM replacement, preserve selection and remain inert text."""
    function = _extract_function(SOURCE.read_text(), '_applyResultsData')
    script = '''
    const assert = browserAssert;
    const selector = document.createElement('select');
    selector.id = 'results-config-filter'; selector.size = 5;
    document.body.appendChild(selector);
    const names = Array.from({length:2052}, (_,i)=>'config_'+String(i).padStart(5,'0'));
    names.push('__proto__', '<img src=x onerror="window.__unsafeConfig=true">');
    const data = names.flatMap(name=>[1,2].map(i=>({config_name:name, backtest_version:'v8', path:name+i})));
    let results = [], renders = 0, actions = 0, replacements = 0;
    const replace = selector.replaceChildren.bind(selector);
    selector.replaceChildren = (...nodes)=>{replacements++; return replace(...nodes);};
    function resultsForSelectedVersion(items) { return items.filter(r=>r.backtest_version==='v8'); }
    function updateVersionBoundResultActions() { actions++; }
    function renderResults() { renders++; }
    ''' + function + '''
    const started = performance.now();
    assert.equal(_applyResultsData(data, names[1000]), data);
    const elapsed = performance.now()-started;
    assert.equal(selector.options.length, names.length+1);
    assert.equal(selector.value, names[1000]);
    assert.equal(replacements, 1);
    assert.equal(renders, 1); assert.equal(actions, 1);
    assert.equal(selector.querySelectorAll('img').length, 0);
    assert.equal(window.__unsafeConfig, undefined);
    assert.equal(Array.from(selector.options).filter(o=>o.value==='__proto__').length, 1);
    selector.scrollTop = 37;
    const scrollTop = selector.scrollTop;
    _applyResultsData(data);
    assert.equal(selector.value, names[1000]);
    assert.equal(selector.scrollTop, scrollTop);
    _applyResultsData(data, 'removed config');
    assert.equal(selector.value, '');
    console.log(JSON.stringify({configs:names.length, result_count:data.length, selector_ms:elapsed}));
    selector.remove();
    '''
    result = run_javascript(['-e', script], cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout
    print(result.stdout.strip())


def test_pending_results_preserve_the_table_and_failed_initial_reads_end_loading():
    """A pending reload keeps visible data; a failed initial read cannot leave an endless loading label."""
    function = _extract_function(SOURCE.read_text(), 'loadResults')
    script = '''
    const assert = browserAssert;
    const host = document.createElement('div');
    const backtestEditorAdapter = {metadataDriven:false};
    function backtestTiming(){}
    host.innerHTML = '<select id="results-config-filter"><option value="target">target</option></select><div id="results-list"><table><tbody><tr><td>Existing result</td></tr></tbody></table></div><div id="results-count-label"></div>';
    document.body.appendChild(host);
    let results = [{config_name:'target',path:'/existing',backtest_version:'v8'}];
    let resultsByVersion = {v7:[],v8:[]}, RESULTS_PAGE_SIZE=0, _pendingResultFilter='';
    let _resultsLoadGeneration=0, _resultsReadController=null, _resultsEmptyRetryTimer=null, _resultsEmptyRetryCount=0;
    let currentPanel='results', rejectRead, notices=[], requests=[];
    function loopEvaluationSelection(){return null;}
    function selectedResultsVersion(){return 'v8';}
    function resultsForSelectedVersion(items){return items.filter(r=>r.backtest_version==='v8');}
    function backtestApiBase(){return '/api/backtest-v8';}
    function backtestResultRead(base,path){requests.push(path);return new Promise((resolve,reject)=>{rejectRead=reject;});}
    function hideResultsCountLabel(){}
    function toast(message){notices.push(message);}
    ''' + function + '''
    (async()=>{
      const list=document.getElementById('results-list');
      const table=list.querySelector('table');
      const pending=loadResults();
      assert.match(requests[0],/name=target/);
      assert.equal(list.querySelector('table'),table);
      rejectRead(new Error('Read timed out'));
      await assert.rejects(()=>pending,/Read timed out/);
      assert.equal(list.querySelector('table'),table);
      assert.equal(notices.length,1);
      results=[];
      const initial=loadResults('target');
      assert.match(list.textContent,/Loading results/);
      rejectRead(new Error('Read timed out <untrusted>'));
      await assert.rejects(()=>initial,/Read timed out/);
      assert.equal(list.textContent,'Could not load results: Read timed out <untrusted>');
      assert.equal(list.querySelector('untrusted'),null);
      assert.doesNotMatch(list.textContent,/Loading results/);
      host.remove();
    })().catch(error=>{console.error(error);finishTest(1);});
    '''
    result = run_javascript(['-e', script], cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout
