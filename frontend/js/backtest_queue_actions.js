/* Queue candidates without navigating away from their source page. */
(function () {
  'use strict';
  const completed = new Map(), operations = new Map();
  let busy = false, refreshGeneration = 0;
  const version = () => window.OPTIMIZE_VERSION === 'v8' ? 'v8' : 'v7';
  const base = () => '/api/backtest-' + version();
  const nodes = selector => document.querySelectorAll(selector);
  function message(text) { nodes('[data-backtest-queue-status]').forEach(node => { node.textContent = text; }); }
  async function request(path, body) {
    const response = await fetch(base() + path, body ? {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)} : {});
    if (!response.ok) throw new Error(await response.text() || ('HTTP ' + response.status));
    return response.json();
  }
  async function refresh() {
    const generation = ++refreshGeneration;
    try {
      const [queue, settings] = await Promise.all([request('/queue'), request('/settings')]);
      if (generation !== refreshGeneration) return;
      const items = queue.items || [];
      const pending = items.filter(item => ['queued','running','starting'].includes(item.status)).length;
      nodes('[data-backtest-open-queue]').forEach(node => { node.textContent = 'Open Queue (' + pending + ')'; });
      nodes('[data-backtest-autostart]').forEach(node => { node.textContent = settings.autostart ? 'Autostart ON — queued jobs may start automatically' : 'Autostart OFF — start jobs from the queue'; });
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
    if (operations.size + items.length > 256) throw new Error('Queue session limit reached. Reload the page before adding more jobs.');
    items = JSON.parse(JSON.stringify(items));
    const keys = await Promise.all(items.map(keyFor));
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
    let added = 0, skipped = 0;
    // Retain operation IDs after errors: a retry must not duplicate an accepted POST.
    for (const [index, item] of items.entries()) {
      const key = version() + ':' + keys[index];
      if (completed.has(key)) { skipped++; continue; }
      try {
        const result = await request('/queue', {...item, operation_id:operations.get(key)});
        completed.set(key, result.filename);
        added++;
        message(added + ' / ' + items.length + ' jobs added…');
      } catch (error) {
        await refresh();
        throw new Error(added + ' jobs added; remaining jobs were not confirmed. Retry to continue. ' + error.message);
      }
    }
    message(added + ' jobs added' + (skipped ? ' · ' + skipped + ' already queued' : '') + ' · ' + items[0].name);
    await refresh();
    return {added, skipped};
  }
  async function perform(button, action) {
    if (busy) return;
    busy = true;
    const label = button.textContent;
    button.disabled = true; button.textContent = 'Adding…';
    message('Preparing queue jobs…');
    try { return await action(); }
    catch (error) { message('Queue failed: ' + error.message); throw error; }
    finally { busy = false; button.disabled = false; button.textContent = label; }
  }
  window.PBGuiBacktestQueue = {submit, perform, refresh};
  function init() {
    nodes('[data-backtest-open-queue]').forEach(button => button.addEventListener('click', () => { window.location.href = base() + '/main_page?panel=queue'; }));
    refresh();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, {once:true}); else init();
}());
