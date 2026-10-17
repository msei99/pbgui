(function () {
  'use strict';
  var report = null, fullReport = null, reportText = null, generation = 0, controller = null;
  var attachmentContext = new URL(location.href).searchParams.get('context') || new URL(location.href).searchParams.get('loop_id') || 'manual';
  if (attachmentContext !== 'manual' && !/^[a-f0-9]{32}$/.test(attachmentContext)) attachmentContext = 'manual';
  var selection = window.PBGuiSupportAttachments.create(selectionChanged, {storageKey:'pbgui.debug.attachments.' + attachmentContext});
  var preview = window.PBGuiSupportReportView.create(document.getElementById('debug-structured'), {storageKey: 'pbgui.debug.preview.navigation'});
  function selectionChanged() {
    report = selection.getData(); reportText = null;
    if (preview) preview.setReport(report);
    if (report) renderSummary(report);
    window.dispatchEvent(new Event('pbgui-debug-selection'));
  }
  function serializedReport() { if (!report) return null; if (reportText === null) reportText = JSON.stringify(report, null, 2); return reportText; }
  var contextId = new URL(location.href).searchParams.get('context');
  if (contextId && !/^[a-f0-9]{32}$/.test(contextId)) contextId = null;
  var loopId = new URL(location.href).searchParams.get('loop_id');
  if (loopId && !/^[a-f0-9]{32}$/.test(loopId)) loopId = null;
  function el(id) { return document.getElementById(id); }
  if (loopId) el('debug-context').textContent = 'Selected Loop: ' + loopId.slice(0, 8);
  function tab(value) {
    ['summary', 'report'].forEach(function (name) {
      el('debug-' + name).hidden = name !== value;
      el('debug-tab-' + name).setAttribute('aria-selected', String(name === value));
    });
    history.replaceState(null, '', location.pathname + location.search + '#' + value);
  }
  ['summary', 'report'].forEach(function (name) { el('debug-tab-' + name).addEventListener('click', function () { tab(name); }); });
  tab(location.hash === '#report' ? 'report' : 'summary');
  async function collectReport() {
    var current = ++generation;
    if (controller) controller.abort();
    controller = new AbortController();
    window.dispatchEvent(new Event('pbgui-debug-changing'));
    el('debug-collect').disabled = true; el('debug-download').disabled = true; report = null; fullReport = null; reportText = null; selection.setReport(null); el('debug-counts').replaceChildren(); el('debug-omitted').replaceChildren(); el('debug-error').textContent = ''; el('debug-status').textContent = 'Collecting…';
    try {
      var response = await fetch(window.API_BASE + '/collect', {method:'POST', credentials:'same-origin', cache:'no-store',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify(Object.assign({loop_id:loopId}, contextId ? {context_id:contextId} : {})), signal:controller.signal});
      var data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'Collection failed');
      if (current !== generation) return;
      if (window.PBGuiSupportDiagnostics) data.browser = window.PBGuiSupportDiagnostics.snapshot();
      fullReport = data; selection.setReport(data);
      el('debug-status').textContent = 'Report ready. Download and send it to support.';
      window.dispatchEvent(new Event('pbgui-debug-ready'));
    } catch (error) { if (current === generation && error.name !== 'AbortError') {el('debug-error').textContent = error.message; el('debug-status').textContent = 'Collection failed.';} }
    finally { if (current === generation) el('debug-collect').disabled = false; }
    return report;
  }
  function renderSummary(data) {
      el('debug-download').disabled = false; el('debug-counts').replaceChildren();
      [['Attached application routes', data.coverage.registered_routes.length], ['Attached operational records', Object.keys(data.records).length],
        ['Attached service / job logs', Object.keys(data.logs).length], ['Scanned sources', (fullReport || data).coverage.sources.length],
        ['File tails / truncated sources', (fullReport || data).coverage.sources.filter(function (s) {return s.truncated;}).length],
        ['Selected JSON size', Math.ceil(selection.getSize() / 1024) + ' KiB']].forEach(function (pair) {
          var row = document.createElement('tr'); pair.forEach(function (value) {var cell = document.createElement('td'); cell.textContent = value; row.appendChild(cell);}); el('debug-counts').appendChild(row);
        });
      el('debug-omitted').replaceChildren();
      var omittedCounts = new Map();
      (fullReport || data).coverage.omitted.forEach(function (item) { omittedCounts.set(item.reason, (omittedCounts.get(item.reason) || 0) + 1); });
      omittedCounts.forEach(function (count, reason) {var li = document.createElement('li'); li.textContent = reason + ': ' + count + ' sources'; el('debug-omitted').appendChild(li);});
      el('debug-limits-heading').hidden = !(fullReport || data).coverage.omitted.length;
  }
  el('debug-collect').addEventListener('click', collectReport);
  window.PBGuiSupportExport = {collect: collectReport, getReport: serializedReport, getData: function () { return report; }, attachPicker: selection.attach};
  selection.attach(el('debug-attachment-picker'));
  el('debug-download').addEventListener('click', function () {
    if (!report) return;
    var url = URL.createObjectURL(new Blob([serializedReport()], {type:'application/json'}));
    var link = document.createElement('a'); link.href = url; link.download = 'pbgui-debug-' + new Date().toISOString().replace(/[:.]/g,'-') + '.json';
    document.body.appendChild(link); link.click(); link.remove(); setTimeout(function () {URL.revokeObjectURL(url);}, 1000);
  });
  window.addEventListener('pagehide', function () { generation++; if (controller) controller.abort(); });
})();
