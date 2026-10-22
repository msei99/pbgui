/* Shared explicit review for same-Loop GPU replacement. */
(function () {
  'use strict';
  var busy = new Set();
  async function post(base, path, body) {
    var controller = new AbortController(), timer = setTimeout(function () { controller.abort(); }, 30000);
    try {
      var response = await fetch(base + path, {method:'POST', credentials:'same-origin', cache:'no-store',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify(body), signal:controller.signal});
      var data = await response.json();
      if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'GPU replacement request rejected');
      return data;
    } finally { clearTimeout(timer); }
  }
  async function run(options) {
    var key = options.loop_id + ':' + options.worker_id;
    if (busy.has(key)) return;
    if (!window.PBGuiDialogs || !window.PBGuiDialogs.confirm) throw new Error('GPU replacement review is unavailable');
    busy.add(key);
    try {
      var path = '/' + encodeURIComponent(options.loop_id) + '/rental/';
      var review = await post(options.base, path + 'replace-preview', {worker_id:options.worker_id});
      if (options.isCurrent && !options.isCurrent()) return;
      var accepted = await window.PBGuiDialogs.confirm({title:'Replace GPU and continue loop?',
        message:'End only rental ' + options.worker_id + ' and wait for confirmed provider deletion. '
          + 'Exclude machine ' + review.excluded_machine_ids.join(', ') + ' within this Loop replacement chain only. '
          + 'Restart the same frozen input and logical round; in-flight work is lost and no checkpoint is assumed compatible. '
          + 'Attempts: ' + review.attempts_used + '/' + review.max_runs + ' already used or reserved; the retry consumes another attempt. '
          + 'Remaining budget at review: $' + Number(review.remaining_budget_usd).toFixed(4) + '. '
          + 'Deadline: ' + new Date(review.deadline * 1000).toISOString() + '. '
          + 'Keep the same Loop, baseline and incumbent. No limit reset or global Auto rent change. '
          + 'If capacity or limits prevent replacement, the Loop waits with a reason.', confirmText:'Replace GPU and continue loop'});
      if (!accepted || (options.isCurrent && !options.isCurrent())) return;
      return await post(options.base, path + 'replace', {worker_id:options.worker_id,
        expected_generation:review.snapshot.generation, snapshot_digest:review.snapshot_digest,
        operation_id:review.operation_id, authorization:true});
    } finally { busy.delete(key); }
  }
  window.PBGuiLoopGPUReplacement = {run:run};
})();
