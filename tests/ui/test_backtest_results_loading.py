"""Isolated real-browser regressions for large Backtest result lists and failed reads."""
from pathlib import Path

from javascript_checks import run_javascript
from tests.ui.test_backtest_v8_frontend_logic import _extract_function

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'frontend/v7_backtest.html'


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
    host.innerHTML = '<select id="results-config-filter"><option value="target">target</option></select><div id="results-list"><table><tbody><tr><td>Existing result</td></tr></tbody></table></div><div id="results-count-label"></div>';
    document.body.appendChild(host);
    let results = [{config_name:'target',path:'/existing',backtest_version:'v8'}];
    let resultsByVersion = {v7:[],v8:[]}, RESULTS_PAGE_SIZE=0, _pendingResultFilter='';
    let _resultsLoadGeneration=0, _resultsEmptyRetryTimer=null, _resultsEmptyRetryCount=0;
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
