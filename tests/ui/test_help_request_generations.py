"""Help keeps the most recent topic and language despite delayed responses."""

from pathlib import Path
import subprocess

import pytest

SOURCE = (Path(__file__).resolve().parents[2] / 'frontend/help.html').read_text()


@pytest.mark.parametrize('mode', ['topic', 'topic_error', 'language', 'index_error', 'cache'])
def test_out_of_order_help_responses(mode):
    """Execute actual topic/index/cache loaders with manually resolved requests."""
    topic = SOURCE[SOURCE.index('      function loadTopic('):SOURCE.index('      function setOverlayVisible(')]
    cache = SOURCE[SOURCE.index('      function ensureTopicCached('):SOURCE.index('      function renderGlobalResults(')]
    script = 'const mode=%r;\n' % mode + topic + cache + r'''
const assert=require('node:assert/strict');
let topicLoadGeneration=0,helpIndexGeneration=0,helpLang='EN',helpTopics=[{file:'A',title:'A'},{file:'B',title:'B'}];
let helpSel=0,rawHtml=null,topicCache={},searchMarks=[],searchIndex=-1,helpLoaded=false,globalMode=false;
const helpContent={innerHTML:''},helpTocList={innerHTML:''},helpSearchCount={},helpSearch={value:''};
const requests=[];
function appPath(path){return path;}
function renderToc(){}
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
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
