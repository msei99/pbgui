/* Presentation and lifetime of the shared PB7/PB8 optimizer log window. */
(function () {
  'use strict';
  const el = id => document.getElementById(id);
  const panel = el('log-panel');
  if (!panel || !el('optlog-details')) return;
  const storageKey = 'pbgui.optimize-log.' + window.OPTIMIZE_VERSION + '.v1';
  const groups = ['rental', 'run', 'utilization', 'convergence'];
  let saved = {};
  try { saved = JSON.parse(sessionStorage.getItem(storageKey) || '{}') || {}; } catch (_) {}
  if (typeof saved !== 'object' || Array.isArray(saved)) saved = {};
  let active = false, frame = 0, observer = null, resizeObserver = null;
  let target = saved.target || null, returnFocus = null;
  let restoring = false, queueReady = false, cloudReady = false;
  let suspended = false, intentGeneration = 0;
  const divider = el('optlog-divider');
  const details = el('optlog-details');

  function persist() {
    const rect = panel.getBoundingClientRect();
    if (active) saved.geometry = {left:rect.left, top:rect.top, width:rect.width, height:rect.height};
    saved.target = target;
    delete saved.detailHeight;
    saved.groups = Object.fromEntries(groups.map(key => [key, el('optlog-' + key + '-group').open]));
    try { sessionStorage.setItem(storageKey, JSON.stringify(saved)); } catch (_) {}
  }
  groups.forEach(key => {
    const group = el('optlog-' + key + '-group');
    group.open = saved.groups?.[key] === true;
    group.addEventListener('toggle', () => { schedule(); persist(); });
  });

  function putText(node, value) {
    if (node && node.textContent !== value) node.textContent = value;
    if (node?.classList.contains('optlog-hint')) node.title = value;
  }
  function hide(node, hidden) {
    if (node && node.hidden !== hidden) node.hidden = hidden;
  }
  function cloudVisible() {
    return !!window.state?.cloudLogId && !!el('cloud-job-details') && !el('cloud-job-details').hidden;
  }
  function sync() {
    const optimizing = el('opt-log-dashboard').classList.contains('visible');
    const cloud = cloudVisible();
    hide(el('optlog-open-results'), !optimizing);
    hide(el('optlog-open-pareto-explorer'), !optimizing);
    hide(el('optlog-cloud-actions'), !cloud);
    hide(el('optlog-cloud-notices'), !cloud);
    hide(el('optlog-rental-group'), !optimizing || !cloud || (el('optlog-rental').hidden && el('cloud-calibration-result')?.hidden !== false));
    hide(el('optlog-run-group'), !optimizing);
    hide(el('optlog-utilization-group'), !optimizing || !cloud);
    hide(el('optlog-convergence-group'), !optimizing || !cloud || el('cloud-convergence')?.hidden !== false);
    hide(details, !groups.some(key => !el('optlog-' + key + '-group').hidden));
    hide(divider, details.hidden);
    const error = optimizing ? el('optlog-error').textContent.trim() : '';
    putText(el('optlog-run-error'), error && error !== '-' ? error : '');
    hide(el('optlog-run-error'), !error || error === '-');
    const notices = el('optlog-notices');
    hide(notices, ![...notices.children].some(node => !node.hidden && node.textContent.trim()));
  }

  function attachCloud() {
    // Move existing nodes once; their IDs, inputs, events and security gates survive.
    const cloud = el('cloud-job-details');
    if (!cloud || cloud.dataset.optlogAttached) return;
    cloud.dataset.optlogAttached = 'true';
    const move = (id, host) => { if (el(id)) el(host).append(el(id)); };
    ['cloud-calibration-result'].forEach(id => move(id, 'optlog-rental-body'));
    ['cloud-utilization', 'cloud-throughput', 'cloud-throughput-sample'].forEach(id => move(id, 'optlog-utilization-body'));
    ['cloud-convergence', 'convergence-sample'].forEach(id => move(id, 'optlog-convergence-body'));
    ['queue-message', 'supervision-status'].forEach(id => move(id, 'optlog-cloud-notices'));
    const actions = cloud.querySelector('.fields');
    if (actions) el('optlog-cloud-actions').append(actions);
    ['cloud-calibration-accept', 'job-results'].forEach(id => move(id, 'optlog-cloud-actions'));
    schedule();
  }

  function schedule() {
    if (!active || frame) return;
    frame = requestAnimationFrame(() => { frame = 0; layout(); });
  }
  function fitGeometry() {
    if (!active) return;
    const rect = panel.getBoundingClientRect();
    const width = Math.min(rect.width, Math.max(1, innerWidth - 32));
    const height = Math.min(rect.height, Math.max(1, innerHeight - 32));
    panel.style.width = width + 'px'; panel.style.height = height + 'px';
    panel.style.left = Math.max(16, Math.min(rect.left, innerWidth - width - 16)) + 'px';
    panel.style.top = Math.max(16, Math.min(rect.top, innerHeight - height - 16)) + 'px';
    panel.style.right = 'auto'; panel.style.bottom = 'auto';
  }
  function layout() {
    if (!active) return;
    attachCloud(); sync(); fitGeometry();
    const viewer = el('log-viewer-target');
    const viewerBody = viewer.querySelector('.lvp-viewer');
    if (viewerBody && !viewerBody.querySelector('.optlog-viewer-controls')) {
      const wrapper = document.createElement('div'); wrapper.className = 'optlog-viewer-controls';
      const toolbar = viewerBody.querySelector('.lvp-toolbar');
      const search = viewerBody.querySelector('.lvp-searchbar');
      if (toolbar) viewerBody.insertBefore(wrapper, toolbar);
      else viewerBody.prepend(wrapper);
      if (toolbar) wrapper.append(toolbar);
      if (search) wrapper.append(search);
    }
    const inner = panel.clientHeight;
    const titleHeight = el('log-panel-header').getBoundingClientRect().height;
    const minimum = innerHeight >= 600 ? 160 : 120;
    const dividerHeight = divider.hidden ? 0 : divider.getBoundingClientRect().height;
    const available = Math.max(0, inner - titleHeight - dividerHeight);
    const head = el('optlog-head');
    const notices = el('optlog-notices');
    const spare = Math.max(0, available - minimum);
    const noticeReserve = notices.hidden ? 0 : Math.min(70, notices.scrollHeight, spare * .4);
    head.style.maxHeight = Math.min(inner * .35, Math.max(0, spare - noticeReserve)) + 'px';
    let headHeight = head.getBoundingClientRect().height;
    notices.style.maxHeight = Math.min(70, Math.max(0, spare - headHeight)) + 'px';
    let noticeHeight = notices.hidden ? 0 : notices.getBoundingClientRect().height;
    let maximum = Math.max(0, available - headHeight - noticeHeight - minimum);
    const detailsScroll = details.scrollTop;
    // Always measure the current content: expanding moves the log down and
    // collapsing returns the space without retaining an old manual split.
    details.style.height = 'auto'; details.style.maxHeight = 'none';
    const naturalHeight = details.hidden ? 0 : details.scrollHeight;
    // At very short window sizes, retain reachable controls via window scrolling.
    const short = available < minimum || (!details.hidden && maximum < 24);
    panel.classList.toggle('optlog-short', short);
    if (short) {
      head.style.maxHeight = '32px';
      if (!notices.hidden) notices.style.maxHeight = '24px';
      headHeight = head.getBoundingClientRect().height;
      noticeHeight = notices.hidden ? 0 : notices.getBoundingClientRect().height;
      maximum = Math.max(24, available - headHeight - noticeHeight - minimum);
    }
    const height = details.hidden ? 0 : Math.min(naturalHeight, maximum);
    details.style.height = height + 'px';
    details.style.maxHeight = maximum + 'px';
    details.scrollTop = detailsScroll;
    const remaining = Math.max(minimum, available - headHeight - noticeHeight - height);
    viewer.style.minHeight = remaining + 'px';
    el('log-waiting').style.minHeight = remaining + 'px';
    const controls = viewerBody?.querySelector('.optlog-viewer-controls');
    if (controls) {
      // The viewer's own 6px gap and terminal box are included in the budget.
      controls.style.maxHeight = Math.max(0, viewerBody.clientHeight - 48 - 6) + 'px';
    }
  }
  function keydown(event) {
    if (!active) return;
    if (document.querySelector('#pbgui-dialog-ovl.visible, #pbgui-shared-help-ovl.visible, #help-ovl.visible, .pbg-modal-overlay.is-open, .modal.open')) {
      event.pbguiKeepLogOpen = true;
      return;
    }
    if (event.key === 'Escape' && panel.contains(event.target)) {
      event.preventDefault(); event.stopPropagation(); window.closeLogPanel();
    }
  }
  function resized() { fitGeometry(); schedule(); persist(); }
  function interactionEnded() { if (active) { fitGeometry(); schedule(); persist(); } }
  function resetLayout() {
    if (!active) return;
    if (panel._stopInteraction) panel._stopInteraction();
    groups.forEach(key => { el('optlog-' + key + '-group').open = false; });
    const width = Math.min(1200, innerWidth * .85);
    const height = innerHeight * .75;
    panel.style.width = width + 'px'; panel.style.height = height + 'px';
    panel.style.left = (innerWidth - width) / 2 + 'px';
    panel.style.top = (innerHeight - height) / 2 + 'px';
    [panel, details, el('optlog-head'), el('optlog-notices')].forEach(node => { node.scrollTop = 0; });
    layout(); persist();
  }
  el('optlog-reset-layout').addEventListener('click', resetLayout);
  el('optlog-reset-layout').addEventListener('mousedown', event => event.stopPropagation());
  function begin(filename, name, options) {
    intentGeneration++;
    const wasActive = active;
    if (active) stop(true);
    active = true;
    if (!wasActive) returnFocus = document.activeElement;
    target = {filename, name, cloud:!!options.cloud, backtest:!!options.backtest, panel:window.state?.panel};
    const g = saved.geometry;
    if (!panel.style.left) {
      const width = g && Number.isFinite(g.width) ? g.width : Math.min(1200, innerWidth * .85);
      const height = g && Number.isFinite(g.height) ? g.height : innerHeight * .75;
      panel.style.width = Math.max(1, width) + 'px'; panel.style.height = Math.max(1, height) + 'px';
      panel.style.left = (g && Number.isFinite(g.left) ? g.left : (innerWidth - width) / 2) + 'px';
      panel.style.top = (g && Number.isFinite(g.top) ? g.top : (innerHeight - height) / 2) + 'px';
    }
    observer = new MutationObserver(records => {
      if (records.some(record => !(record.target.nodeType === 1 ? record.target : record.target.parentElement)?.closest('.lvp-terminal'))) schedule();
    });
    observer.observe(panel, {subtree:true, childList:true, characterData:true, attributes:true, attributeFilter:['hidden', 'open']});
    resizeObserver = new ResizeObserver(schedule);
    [panel, el('optlog-head'), el('optlog-notices'), el('log-viewer-target')].forEach(node => resizeObserver.observe(node));
    window.addEventListener('resize', resized);
    document.addEventListener('keydown', keydown, true);
    document.addEventListener('mouseup', interactionEnded);
    layout(); persist(); el('log-panel-close').focus({preventScroll:true});
  }
  function stop(preserveTarget) {
    intentGeneration++;
    const restoreFocus = active && panel.contains(document.activeElement);
    if (active) persist();
    active = false;
    if (frame) cancelAnimationFrame(frame); frame = 0;
    observer?.disconnect(); resizeObserver?.disconnect(); observer = resizeObserver = null;
    window.removeEventListener('resize', resized);
    document.removeEventListener('keydown', keydown, true);
    document.removeEventListener('mouseup', interactionEnded);
    if (!preserveTarget) { target = null; persist(); if (restoreFocus && returnFocus?.isConnected) returnFocus.focus({preventScroll:true}); }
  }
  async function restore() {
    if (restoring || active || suspended || !saved.target || !queueReady) return;
    // AI Loop has its own validated native-log restoration and return context.
    const url = new URL(location.href);
    if (url.searchParams.has('loop_log') || url.searchParams.has('open_config') || url.searchParams.has('open_queue_config') || url.searchParams.has('open_draft') || url.searchParams.has('migration_draft_id')) {
      target = null; persist();
      return;
    }
    const wanted = saved.target;
    if (wanted.cloud && !cloudReady) return;
    restoring = true;
    const intent = intentGeneration;
    let navigation = window.state?.navigationSeq;
    const current = () => !active && !suspended && intent === intentGeneration && saved.target === wanted && navigation === window.state?.navigationSeq;
    const discardAfterNavigation = () => {
      if (!suspended && intent === intentGeneration && saved.target === wanted && navigation !== window.state?.navigationSeq) {
        target = null; persist();
      }
    };
    try {
      const valid = typeof wanted.filename === 'string' && !/[\\/\x00-\x1f\x7f]/.test(wanted.filename) && !['.', '..', ''].includes(wanted.filename);
      const row = valid && !wanted.cloud && !wanted.backtest && window.state.queue.find(item => item.filename === wanted.filename);
      if (wanted.panel === 'queue') window.setPanel('queue');
      navigation = window.state?.navigationSeq;
      if (wanted.cloud && valid && /^[0-9a-f]{32}$/.test(wanted.filename)) {
        if (await window.PBGuiVast.openJobLog(wanted.filename, current)) return;
        if (!current()) { discardAfterNavigation(); return; }
      }
      if (row) { window.openLogPanel(row.filename, row.name); return; }
      // Backtests/AI jobs restore through their owning view rather than unverified IDs.
      target = null; saved.target = null; persist();
      window.toast('The selected log is no longer available. Open a log from its queue.', 'warn');
    } catch (error) {
      if (!current()) { discardAfterNavigation(); return; }
      target = null; saved.target = null; persist();
      if (window.handleError) window.handleError(error);
    } finally { restoring = false; }
  }
  window.PBGuiOptimizeLog = {
    begin, stop, sync:schedule, attachCloud,
    queueReady() { queueReady = true; void restore(); },
    cloudReady() { cloudReady = true; attachCloud(); void restore(); },
    rental(rental) {
      const o = rental?.offer || {};
      const warnings = [rental?.deadline_error, rental?.budget_error, rental?.deadline_pending ? 'Awaiting confirmation' : ''].filter(Boolean);
      const hint = el('optlog-rental-hint');
      const rate = Number.isFinite(o.price_hour_usd) ? '$' + o.price_hour_usd.toFixed(4) + '/h' : 'Rate unavailable';
      putText(hint, rental ? [o.gpu_name || 'GPU unavailable', rate, ...warnings].join(' · ') : '');
      hint.classList.toggle('is-warning', warnings.length > 0); schedule();
    },
    cloud(job) {
      const hint = el('optlog-utilization-hint');
      const sample = job.throughput;
      const age = Number.isFinite(sample?.sampled_at) ? Math.max(0, Math.floor(Date.now() / 1000 - sample.sampled_at)) : null;
      const terminal = ['completed', 'cancelled', 'failed'].includes(job.status);
      const current = !terminal && ['running', 'collecting'].includes(job.status) && age !== null && age <= 90;
      putText(hint, 'GPU ' + (el('cloud-gpu-util')?.textContent || '—') + ' · Exact/min ' + (el('cloud-exact-rate')?.textContent || '—') + ' · ' + (age === null ? 'No sample' : (current ? 'Sample ' : 'Last interval ') + age + 's ago'));
      putText(el('optlog-convergence-hint'), (el('convergence-phase')?.textContent || '') + ' · ' + (el('convergence-progress')?.textContent || ''));
      schedule();
    }
  };
  window.addEventListener('pagehide', () => {
    suspended = true; queueReady = cloudReady = false;
    stop(true);
    panel.classList.remove('visible');
    if (panel._stopInteraction) panel._stopInteraction();
    if (window.stopOptimizeLogStatusPolling) window.stopOptimizeLogStatusPolling();
    window.state?.logViewer?.close();
  });
  window.addEventListener('pageshow', event => {
    if (!event.persisted) return;
    suspended = false;
    // The queue owners signal readiness after revalidating their current data.
    void restore();
  });
})();
