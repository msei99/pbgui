"""PB8 Optimize stop deadlines release controls despite stalled backend work."""

from pathlib import Path
import re
import subprocess

import pytest


@pytest.mark.parametrize('scenario', ['stalled', 'running', 'terminal', 'reset', 'late', 'duplicate'])
def test_start_date_stop_deadline(scenario):
    """Run real stop/poll/cleanup functions with deterministic timers and requests."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/v7_optimize.html').read_text()
    functions = []
    for name in ['stopOptimizeStartDateLookup','clearOptimizeStartDateProgress','refreshOptimizeStartDateLookup','scheduleOptimizeStartDatePoll']:
        start = re.search(r'^(?:async )?function '+name+r'\(',source,re.M).start()
        end = source.index('\n}',start)
        functions.append(source[start:end+2])
    script = 'const scenario = %r;\n' % scenario + '\n'.join(functions)
    script += r'''
const assert = require('node:assert/strict');
let timers = new Map(), next = 0, requests = 0, pendingResolve;
const window = {setTimeout:(fn,ms)=>{timers.set(++next,{fn,ms});return next;}, clearTimeout:id=>timers.delete(id)};
let _optOhlcvStartDateRun = {jobId:'old'}, _optOhlcvStartDatePollTimer = null;
let locked = true, notices = [], rendered = 0;
const container = {style:{display:'block'}};
function el() { return container; }
function setOptimizeStartDateControlsRunning(value) { locked=value; }
function renderOptimizeStartDateProgress() { rendered++; }
function toast(message,kind) { notices.push([message,kind]); }
function applyOptimizeStartDateResult() { throw new Error('must not apply stopping result'); }
function apiFetch() { requests++; return new Promise(resolve=>pendingResolve=resolve); }
(async()=>{
 const stopping = stopOptimizeStartDateLookup(false);
 const deadline = timers.get(_optOhlcvStartDateRun.stopTimer);
 assert.equal(deadline.ms,10000);
 if (scenario === 'duplicate') { await stopOptimizeStartDateLookup(false); assert.equal(requests,1); }
 if (scenario === 'terminal') {
  pendingResolve({status:'stopped'}); await stopping;
  assert.equal(timers.size,0);
 } else if (scenario === 'reset') {
  clearOptimizeStartDateProgress(); assert.equal(timers.size,0);
 } else {
  if (scenario === 'running') {
   pendingResolve({status:'running'}); await stopping;
   const polling = refreshOptimizeStartDateLookup();
   pendingResolve({status:'running'}); await polling;
  }
  deadline.fn();
  assert.equal(notices.at(-1)[1],'err');
  assert.match(notices.at(-1)[0],/Timed out/);
 }
 assert.equal(_optOhlcvStartDateRun,null);
 assert.equal(locked,false); assert.equal(container.style.display,'none');
 if (scenario === 'late') {
  const newer = {jobId:'new'}; _optOhlcvStartDateRun=newer;
  const before=rendered;
  pendingResolve({status:'completed'}); await stopping;
  assert.equal(_optOhlcvStartDateRun,newer); assert.equal(rendered,before);
 }
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
