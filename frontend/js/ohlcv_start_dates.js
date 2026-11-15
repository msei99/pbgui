/* Shared native editor inception lookup: same request, polling, stop and stale-result rules. */
(function (global) {
  'use strict';
  function create(options) {
    var state = options.state || {run:null, timer:null, requestId:0};
function clear() {
  if (state.run && state.run.stopTimer) {
    window.clearTimeout(state.run.stopTimer);
    state.run.stopTimer = null;
  }
  if (state.timer) {
    window.clearTimeout(state.timer);
    state.timer = null;
  }
  state.run = null;
  options.running(false);
  options.clearProgress();
}

function reset() {
  state.requestId += 1;
  var run = state.run;
  clear();
  options.status('');
  if (run && run.jobId) {
    options.fetch('/ohlcv-start-dates/' + encodeURIComponent(run.jobId), {method: 'DELETE'}).catch(function() {});
  }
}

function schedule(delayMs) {
  if (state.timer) window.clearTimeout(state.timer);
  state.timer = window.setTimeout(refresh, Math.max(100, delayMs || 500));
}

function deadline(promise, ms) {
  var timer;
  return Promise.race([promise, new Promise(function(resolve, reject) {
    timer = window.setTimeout(function() { reject(new Error('Request timed out')); }, ms);
  })]).finally(function() { window.clearTimeout(timer); });
}

async function refresh() {
  var run = state.run;
  if (!run || !run.jobId) return;
  try {
    var response = await deadline(options.fetch('/ohlcv-start-dates/' + encodeURIComponent(run.jobId)), 10000);
    if (state.run !== run) return;
    if (run.stopping) {
      if (response.status === 'completed' || response.status === 'error' || response.status === 'stopped') {
        if (!run.silentStop) options.status('Lookup stopped. Retry when ready.');
        clear();
      } else schedule(250);
      return;
    }
    options.progress(response);
    if (response.status === 'completed') {
      await options.apply(run, response);
      if (state.run === run && !run.stopping) clear();
      return;
    }
    if (response.status === 'error' || response.status === 'stopped') {
      options.status(response.status === 'error'
        ? 'Lookup unsuccessful: ' + (response.error || 'OHLCV lookup failed.') + ' Retry after correcting the problem.'
        : 'Lookup stopped. Retry when ready.', response.status === 'error' ? 'error' : '');
      clear();
      return;
    }
    schedule(500);
  } catch (error) {
    if (state.run !== run) return;
    options.status('Lookup unsuccessful: request failed or timed out. The backend may still be stopping. Retry.', 'error');
    options.fetch('/ohlcv-start-dates/' + encodeURIComponent(run.jobId), {method: 'DELETE'}).catch(function() {});
    clear();
  }
}

async function start(mode, triggerButton) {
  if (!(options.enabled ? options.enabled() : options.isV8()) || state.run) return;
  if ((mode !== 'earliest' && mode !== 'all_markets') || !triggerButton) return;
  var requestId = ++state.requestId;
  var run = {jobId: null, mode: mode, requestId: requestId, silentStop: false};
  state.run = run;
  options.status('');
  options.running(true);
  options.progress({status: 'queued', progress: {percent: 0, message: 'Starting OHLCV lookup...'}});
  try {
    var config = await options.collect();
    if (state.run !== run) return;
    run.signature = options.signature(config);
    var post = options.fetch('/ohlcv-start-dates', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ config: config })
    });
    // A late POST can still create a job after the UI times out or is closed.
    post.then(function(response) {
      if ((state.run !== run || run.stopping || !options.visible()) && response && response.job_id) {
        options.fetch('/ohlcv-start-dates/' + encodeURIComponent(response.job_id), {method: 'DELETE'}).catch(function() {});
      }
    }).catch(function() {});
    var response = await deadline(post, 10000);
    if (state.run !== run || run.stopping || !options.visible()) return;
    if (!response || !response.job_id) throw new Error('Lookup returned no job ID. Retry.');
    run.jobId = response.job_id;
    options.progress(response);
    schedule(100);
  } catch (error) {
    if (state.run !== run) return;
    var message = error && error.message === 'Request timed out'
      ? 'Request timed out before a job ID was received. The backend may still be starting or stopping; retry.'
      : (error && error.message ? error.message : 'Lookup failed. Retry.');
    options.status('Lookup unsuccessful: ' + message, 'error');
    clear();
  }
}

async function stop(silent) {
  var run = state.run;
  if (!run || run.stopping) return;
  run.silentStop = !!silent;
  run.stopping = true;
  if (!run.jobId) {
    if (!silent) options.status('Lookup stopped while starting. The backend may still be stopping. Retry when ready.');
    clear();
    return;
  }
  run.stopTimer = window.setTimeout(function () {
    if (state.run !== run) return;
    if (!run.silentStop) options.status('Timed out waiting for lookup to stop. The backend may still be stopping.', 'error');
    clear();
  }, 10000);
  if (state.timer) {
    window.clearTimeout(state.timer);
    state.timer = null;
  }
  options.progress({status: 'stopping', stop_requested: true,
    progress: Object.assign({}, run.lastProgress || {}, {message: 'Stopping OHLCV lookup...'})});
  try {
    var response = await options.fetch('/ohlcv-start-dates/' + encodeURIComponent(run.jobId), {method: 'DELETE'});
    if (state.run !== run) return;
    options.progress(response);
    if (response.status === 'completed' || response.status === 'error' || response.status === 'stopped') {
      if (!run.silentStop) options.status('Lookup stopped. Retry when ready.');
      clear();
    } else schedule(250);
  } catch (error) {
    if (state.run !== run) return;
    if (!run.silentStop) options.status('Could not stop lookup. The backend may still be stopping.', 'error');
    clear();
  }
}
    return {start:start, stop:stop, reset:reset, clear:clear, refresh:refresh, schedule:schedule, deadline:deadline, state:state};
  }

function renderField(id, label, value, tip, span, options) {
  var dateValue = String(value || '').replace(/[T ].*$/, '');
  var inputHtml = '<div>' +
    '<div style="display:flex;gap:3px;align-items:center">' +
      PBGuiDateInput.render(id, dateValue) +
      '<button type="button" class="act-btn opted-ohlcv-start-btn" style="padding:0 5px;white-space:nowrap" onclick="setOptimizeStartDateFromOhlcv(\'earliest\',this)" title="Set to the earliest selected market with OHLCV coverage" aria-label="Set earliest OHLCV start date">1st</button>' +
      '<button type="button" class="act-btn opted-ohlcv-start-btn" style="padding:0 5px;white-space:nowrap" onclick="setOptimizeStartDateFromOhlcv(\'all_markets\',this)" title="Set to the first date available for every selected exchange and coin" aria-label="Set common OHLCV start date">All</button>' +
    '</div>' +
    '<div id="opted-ohlcv-start-status" role="status" aria-live="polite" style="margin-top:4px;font-size:var(--fs-xs);overflow-wrap:anywhere"></div>' +
    '<div id="opted-ohlcv-start-progress" style="display:none;margin-top:4px">' +
      '<div style="display:flex;align-items:center;gap:4px">' +
        '<div style="height:5px;flex:1;overflow:hidden;border-radius:3px;background:var(--bg3)"><div id="opted-ohlcv-start-progress-fill" style="height:100%;width:0;background:var(--accent);transition:width .2s"></div></div>' +
        '<button type="button" class="act-btn act-btn-danger" id="opted-ohlcv-start-stop" style="padding:0 5px" onclick="stopOptimizeStartDateLookup(false)">Stop</button>' +
      '</div>' +
      '<div id="opted-ohlcv-start-progress-label" style="margin-top:2px;font-size:var(--fs-xs);color:var(--text-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="">Preparing lookup...</div>' +
    '</div>' +
  '</div>';
  inputHtml = inputHtml.replace(/opted-/g, options.prefix + '-')
    .replace(/setOptimizeStartDateFromOhlcv/g, options.startAction)
    .replace(/stopOptimizeStartDateLookup/g, options.stopAction);
  return options.group(id, label, inputHtml, tip, span || 1);
}
  global.PBGuiOhlcvStartDates = {create:create, renderField:renderField};
})(window);
