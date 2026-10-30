"""Help keeps the most recent topic and language despite delayed responses."""

from javascript_checks import run_javascript

from pathlib import Path

import pytest

SOURCE = (Path(__file__).resolve().parents[2] / 'frontend/help.html').read_text()
ROOT = Path(__file__).resolve().parents[2]


def _guide_loader_script(loader):
    """Extract the actual production loaders, including the otherwise inactive VPS loader."""
    path = ROOT / ('frontend/js/shared_help_overlay.js' if loader == 'shared' else 'frontend/vps_manager.html')
    source = path.read_text()
    start = source.index('  function showHelpError(' if loader == 'shared' else '      function showHelpError(')
    end = source.index('  function setLang(', start) if loader == 'shared' else source.index("      helpFilter.addEventListener", start)
    production = source[start:end]
    assert 'function loadTopic(' in production and 'function loadHelpIndex(' in production
    return 'const loader=%r;\n' % loader + r'''
const assert=browserAssert;
const requests=[],errors=[],events=[];
class Element {
 constructor(){this.children=[];this.value='';this.scrollTop=0;this.attrs={};}
 set innerHTML(value){this.html=value;this.children=[];this.text='';}
 get innerHTML(){return this.html||'';}
 set textContent(value){this.text=value;this.children=[];this.html='';}
 get textContent(){return this.text||'';}
 setAttribute(name,value){this.attrs[name]=value;}
 appendChild(child){this.children.push(child);events.push('display');}
}
const document={createElement:()=>new Element()};
const console={error:(...args)=>{errors.push(args);events.push('log');}};
const nodes={};
function dom(id){return nodes[id]||(nodes[id]=new Element());}
const helpContent=dom('pbgui-shared-help-content'),helpTocList=dom('pbgui-shared-help-toc-list');
let helpLang='EN',helpTopics=[{file:'A',title:'A'},{file:'B',title:'B'}],helpSel=0,helpCache={},helpCurrentTopic='A';
let helpTopicRequestSeq=0,helpIndexRequestSeq=0;
const state={lang:'EN',topics:helpTopics,topicCache:{},indexRequestSeq:0,topicRequestSeq:0,
 selectedIndex:0,pendingAnchor:'',currentKeyword:'A',globalMode:false};
const window={setTimeout:()=>{}};
function appPath(path){return path;}
function renderToc(){}
function renderMarkdown(text){return '<p>'+text+'</p>';}
function mdToHtml(text){return renderMarkdown(text);}
function fetch(url,options){
 assert.equal(options.credentials,'same-origin');
 return new Promise((resolve,reject)=>requests.push({url,resolve,reject}));
}
function language(lang){helpLang=lang;state.lang=lang;}
function cache(){return loader==='shared'?state.topicCache:helpCache;}
function topics(){return loader==='shared'?state.topics:helpTopics;}
async function flush(){await new Promise(resolve=>setImmediate(resolve));}
async function reply(index,data){requests[index].resolve({ok:true,json:async()=>data});await flush();}
const failure=new TypeError('Synthetic network failure');
const invalid=new SyntaxError('Synthetic invalid JSON');
async function fail(index,kind){
 const request=requests[index];
 if(kind==='network')request.reject(failure);
 else request.resolve({ok:kind!=='http',status:500,json:async()=>{
   assert.notEqual(kind,'http','HTTP errors must precede JSON parsing');throw invalid;
 }});
 await flush();
}
function checkError(kind,area){
 assert.equal(errors.length,1);
 assert.equal(errors[0][0],area==='index'?'Error loading help index:':'Error loading help topic:');
 if(kind==='http')assert.equal(errors[0][1].message,'HTTP 500');
 else assert.equal(errors[0][1],kind==='network'?failure:invalid,'original error object');
 assert.deepEqual(events,['log','display'],'one log before the single alert');
 assert.equal(helpContent.children.length,1);
 assert.equal(helpContent.children[0].attrs.role,'alert');
 assert.equal(helpContent.children[0].textContent,area==='index'?'Failed to load help topics.':'Failed to load help content.');
 assert.equal(helpContent.innerHTML,'','old document/loading removed');
 if(area==='index')assert.equal(helpTocList.textContent,'Failed to load help topics.');
}
''' + production


@pytest.mark.parametrize('loader', ['shared', 'vps'])
@pytest.mark.parametrize('area', ['content', 'index'])
@pytest.mark.parametrize('kind', ['http', 'network', 'json'])
def test_guide_failures_log_then_alert_and_recover(loader, area, kind):
    """Current errors log the original exception once, alert once and recover normally."""
    script = _guide_loader_script(loader) + 'const area=%r,kind=%r;\n' % (area, kind) + r'''
(async()=>{
 helpContent.innerHTML='<p>Old document</p>';
 if(area==='content')loadTopic(0);else loadHelpIndex('A');
 assert.match(helpContent.innerHTML,/Loading/);
 await fail(0,kind);checkError(kind,area);
 if(area==='content'){
   loadTopic(0);await reply(1,{content:'Recovered'});
 }else{
   loadHelpIndex('A');await reply(1,[{file:'A',title:'A'}]);
   await reply(2,{content:'Recovered'});
 }
 assert.equal(helpContent.innerHTML,'<p>Recovered</p>');
 assert.equal(helpContent.children.length,0,'success replaces the alert');
 assert.equal(errors.length,1);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
'''
    result = run_javascript(['-e', script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('loader', ['shared', 'vps'])
@pytest.mark.parametrize('area', ['content', 'index'])
@pytest.mark.parametrize('stale', ['success', 'http', 'network', 'json', 'pending_json'])
def test_guide_ignores_superseded_responses(loader, area, stale):
    """Superseded requests cannot write DOM/cache, log errors or reset reading position."""
    script = _guide_loader_script(loader) + 'const area=%r,stale=%r;\n' % (area, stale) + r'''
(async()=>{
 loadTopic(0);
 if(area==='index')loadHelpIndex('A');
 const older=area==='index'?1:0;
 let finishJson;
 if(stale==='pending_json'){
   requests[older].resolve({ok:true,json:()=>new Promise(resolve=>finishJson=resolve)});
   await flush();
 }
 if(area==='content'){
   loadTopic(1);await reply(1,{content:'Newest'});
 }else{
   language('DE');loadHelpIndex('B');await reply(2,[{file:'B',title:'Deutsch'}]);
   assert.match(requests[3].url,/lang=DE/);
   await reply(3,{content:'Newest'});
   await reply(0,{content:'Old English topic'});
 }
 helpContent.scrollTop=123;
 const before=JSON.stringify(cache());
 if(stale==='success')await reply(older,area==='index'?[{file:'A',title:'Old'}]:{content:'Old'});
 else if(stale==='pending_json'){
   finishJson(area==='index'?[{file:'A',title:'Old'}]:{content:'Old'});await flush();
 }else await fail(older,stale);
 assert.equal(helpContent.innerHTML,'<p>Newest</p>');
 assert.equal(helpContent.scrollTop,123);
 assert.equal(JSON.stringify(cache()),before);
 assert.equal(errors.length,0);assert.equal(events.length,0);
 if(area==='index'){
   assert.equal(topics()[0].file,'B');assert.equal(requests.length,4);
 }
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
'''
    result = run_javascript(['-e', script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('stale', ['success', 'http', 'network', 'json'])
def test_vps_cached_topic_invalidates_pending_request(stale):
    """A real VPS cache hit supersedes older fetches before they can change cache or DOM."""
    script = _guide_loader_script('vps') + 'const stale=%r;\n' % stale + r'''
(async()=>{
 helpCache['EN:B']='<p>Cached B</p>';
 loadTopic(0);loadTopic(1);
 assert.equal(requests.length,1);helpContent.scrollTop=123;
 if(stale==='success')await reply(0,{content:'Old A'});else await fail(0,stale);
 assert.equal(helpContent.innerHTML,'<p>Cached B</p>');
 assert.equal(helpContent.scrollTop,123);
 assert.deepEqual(helpCache,{'EN:B':'<p>Cached B</p>'});
 assert.equal(errors.length,0);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
'''
    result = run_javascript(['-e', script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_vps_rapid_language_switch_retains_selected_topic():
    """Execute real VPS language handlers while older indexes and content are pending."""
    source = (ROOT / 'frontend/vps_manager.html').read_text()
    handlers = source[source.index("      helpLangEN.addEventListener('click'"):
                      source.index('      window.closeHelp = function')]
    script = _guide_loader_script('vps') + r'''
const helpDefaultTopic='vps_manager';
function languageButton(){return {classList:{add(){},remove(){}},addEventListener(_event,fn){this.click=fn;}};}
const helpLangEN=languageButton(),helpLangDE=languageButton();
''' + handlers + r'''
(async()=>{
 loadTopic(1);loadHelpIndex('B');
 helpLangDE.click();helpLangEN.click();
 assert.match(requests[2].url,/lang=DE/);assert.match(requests[3].url,/lang=EN/);
 await reply(3,[{file:'A',title:'A'},{file:'B',title:'B'}]);
 assert.match(requests[4].url,/file=B&lang=EN/);
 await reply(4,{content:'Latest B'});
 await reply(0,{content:'Old B'});await reply(1,[{file:'A',title:'A'}]);
 await fail(2,'network');
 assert.equal(helpContent.innerHTML,'<p>Latest B</p>');
 assert.equal(helpSel,1);assert.equal(errors.length,0);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
'''
    result = run_javascript(['-e', script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('mode', ['topic', 'topic_error', 'language', 'index_error', 'cache'])
def test_out_of_order_help_responses(mode):
    """Execute actual topic/index/cache loaders with manually resolved requests."""
    topic = SOURCE[SOURCE.index('      function loadTopic('):SOURCE.index('      function openHelp(')]
    cache = SOURCE[SOURCE.index('      function ensureTopicCached('):SOURCE.index('      function renderGlobalResults(')]
    script = 'const mode=%r;\n' % mode + topic + cache + r'''
const assert=browserAssert;
let topicLoadGeneration=0,helpIndexGeneration=0,helpLang='EN',helpTopics=[{file:'A',title:'A'},{file:'B',title:'B'}];
let helpSel=0,rawHtml=null,topicCache={},searchMarks=[],searchIndex=-1,helpLoaded=false,globalMode=false;
const helpContent={innerHTML:''},helpTocList={innerHTML:''},helpSearchCount={},helpSearch={value:''};
const requests=[];
function appPath(path){return path;}
function renderToc(){}
function saveHelpLocation(){}
function renderMarkdown(text){return text;}
function fetch(url){return new Promise((resolve,reject)=>requests.push({url,resolve,reject}));}
async function reply(index,data){requests[index].resolve({ok:true,json:async()=>data});await flush();}
async function flush(){await new Promise(resolve=>setImmediate(resolve));}
(async()=>{
 if(mode === 'topic' || mode === 'topic_error'){
  loadTopic(0);loadTopic(1);await reply(1,{content:'B'});
  if(mode === 'topic_error'){requests[0].reject(new Error('old failure'));await flush();}
  else await reply(0,{content:'A'});
  assert.equal(helpContent.innerHTML,'B');assert.equal(helpSel,1);
 }else if(mode === 'cache'){
  let called=false;ensureTopicCached(0,()=>called=true);
  helpLang='DE';loadHelpIndex();
  await reply(0,{content:'stale English'});
  assert.equal(called,false);assert.deepEqual(topicCache,{});
 }else{
  loadTopic(0);loadHelpIndex();helpLang='DE';loadHelpIndex();
  await reply(2,[{file:'de',title:'Deutsch'}]);
  assert.match(requests[3].url,/lang=DE/);
  await reply(3,{content:'Deutsch'});
  await reply(0,{content:'old topic'});
  if(mode === 'index_error'){requests[1].reject(new Error('old index'));await flush();}
  else await reply(1,[{file:'en',title:'English'}]);
  assert.equal(helpContent.innerHTML,'Deutsch');assert.equal(helpTopics[0].file,'de');
  assert.equal(requests.length,4);
 }
})().catch(error=>{console.error(error);testResult.exitCode=1;});
'''
    run_javascript(['-e',script],check=True,capture_output=True,text=True,timeout=10)
