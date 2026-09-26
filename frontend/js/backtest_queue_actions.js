/* Queue candidates without navigating away from their source page. */
(function () {
  'use strict';
  const completed = new Map(), operations = new Map();
  let refreshGeneration = 0, pendingBatches = 0, pendingJobs = 0, activeActions = 0;
  let queueTail = Promise.resolve(), progressMessage = '';
  const version = () => window.OPTIMIZE_VERSION === 'v8' ? 'v8' : 'v7';
  const base = () => '/api/backtest-' + version();
  const nodes = selector => document.querySelectorAll(selector);
  function message(text) {
    progressMessage = text;
    const waiting = pendingBatches > 1 ? ' · ' + (pendingBatches - 1) + ' more batch(es) waiting' : '';
    nodes('[data-backtest-queue-status]').forEach(node => { node.textContent = text + waiting; });
  }
  function enqueueBatch(items, action) {
    if (pendingJobs + items.length > 256) throw new Error('Too many pending jobs. Wait for a batch to finish before adding more.');
    pendingJobs += items.length;
    pendingBatches++;
    message(pendingBatches === 1 ? 'Preparing ' + items.length + ' jobs · ' + items[0].name : progressMessage);
    const task = queueTail.then(action);
    // A failed batch must not prevent later candidates from being submitted.
    queueTail = task.catch(() => {});
    return task.finally(() => {
      pendingJobs -= items.length;
      pendingBatches--;
      message(progressMessage);
    });
  }
  async function request(path, body) {
    const response = await fetch(base() + path, body ? {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)} : {});
    if (!response.ok) throw new Error(await response.text() || ('HTTP ' + response.status));
    return response.json();
  }
  async function refresh() {
    const generation = ++refreshGeneration;
    try {
      const [queue, settings, batches] = await Promise.all([request('/queue'), request('/settings'), version() === 'v8' ? request('/queue/batches') : Promise.resolve({batches:[]})]);
      if (generation !== refreshGeneration) return;
      const items = queue.items || [];
      const pending = items.filter(item => ['queued','running','starting'].includes(item.status)).length;
      nodes('[data-backtest-open-queue]').forEach(node => { node.textContent = 'Open Queue (' + pending + ')'; });
      nodes('[data-backtest-autostart]').forEach(node => { node.textContent = settings.autostart ? 'Autostart ON — queued jobs may start automatically' : 'Autostart OFF — start jobs from the queue'; });
      const latest = (batches.batches || [])[0];
      if (latest && latest.status === 'running') message(latest.confirmed + ' / ' + latest.total + ' jobs queued by PBGui server');
      else if (latest && latest.status === 'queued') message('Backend queue batch waiting · ' + latest.total + ' jobs');
      else if (latest && latest.status === 'error') message('Backend queue stopped: ' + latest.error);
      else if (latest && latest.status === 'complete') message(latest.total + ' / ' + latest.total + ' jobs queued by PBGui server');

    } catch (error) {
      if (generation === refreshGeneration) nodes('[data-backtest-autostart]').forEach(node => { node.textContent = 'Queue status unavailable: ' + error.message; });
    }
  }
  function stable(value) {
    if (Array.isArray(value)) return value.map(stable);
    if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, stable(value[key])]));
    return value;
  }
  async function keyFor(item) {
    const identity = JSON.parse(JSON.stringify(item));
    if (identity.config && identity.config.pbgui) delete identity.config.pbgui.backtest_result_group;
    const bytes = new TextEncoder().encode(JSON.stringify(stable(identity)));
    if (!crypto.subtle) return new TextDecoder().decode(bytes);
    return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)), x => x.toString(16).padStart(2,'0')).join('');
  }
  async function submit(items) {
    if (!items.length) throw new Error('No backtest jobs to queue.');
    items = JSON.parse(JSON.stringify(items));
    return enqueueBatch(items, async () => {
      const keys = await Promise.all(items.map(keyFor));
      const newKeys = new Set(keys.map(key => version() + ':' + key).filter(key => !operations.has(key)));
      if (operations.size + newKeys.size > 256) throw new Error('Queue session limit reached. Reload the page before adding more jobs.');
      keys.forEach(key => {
        const scoped = version() + ':' + key;
        if (!operations.has(scoped)) {
          const id = Array.from(crypto.getRandomValues(new Uint8Array(16)), x => x.toString(16).padStart(2,'0')).join('');
          operations.set(scoped, 'explorer-' + id);
        }
      });
      const groups = new Map();
      items.forEach((item, index) => {
        const group = item.config && item.config.pbgui && item.config.pbgui.backtest_result_group;
        if (group) {
          if (!groups.has(group.id)) groups.set(group.id, operations.get(version() + ':' + keys[index]));
          group.id = groups.get(group.id);
        }
      });
      if (version() === 'v8') {
        const prepared = items.map((item, index) => ({...item, operation_id:operations.get(version() + ':' + keys[index])}));
        const batch = await request('/queue/batches', {items:prepared});
        if (batch.status === 'error') {
          throw new Error(batch.error + ' Submit again after checking the queue.');
        }
        message(batch.confirmed + ' / ' + batch.total + ' jobs queued by PBGui server');
        return {added:batch.confirmed, queued:batch.total - batch.confirmed, batch_id:batch.batch_id};
      }
      let added = 0, skipped = 0;
      // Retain operation IDs after errors: a retry must not duplicate an accepted POST.
      for (const [index, item] of items.entries()) {
        const key = version() + ':' + keys[index];
        if (completed.has(key)) { skipped++; continue; }
        try {
          const result = await request('/queue', {...item, operation_id:operations.get(key)});
          completed.set(key, result.filename);
          added++;
          message((added + skipped) + ' / ' + items.length + ' jobs confirmed · ' + items[0].name);
        } catch (error) {
          await refresh();
          throw new Error(added + ' jobs added; remaining jobs were not confirmed. Retry to continue. ' + error.message);
        }
      }
      message(added + ' jobs added' + (skipped ? ' · ' + skipped + ' already queued' : '') + ' · ' + items[0].name);
      await refresh();
      return {added, skipped};
    });
  }
  async function perform(button, action) {
    activeActions++;
    if (!pendingBatches) message('Preparing queue jobs…');
    try { return await action(); }
    catch (error) { message('Queue failed: ' + error.message); throw error; }
    finally { activeActions--; }
  }
  window.PBGuiBacktestQueue = {submit, perform, refresh};
  function init() {
    nodes('[data-backtest-open-queue]').forEach(button => button.addEventListener('click', () => {
      const target = base() + '/main_page?panel=queue';
      if (pendingBatches || activeActions) window.open(target, '_blank', 'noopener');
      else window.location.href = target;
    }));
    refresh();
    if (typeof window.setInterval === 'function') window.setInterval(refresh, 5000);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, {once:true}); else init();
}());
