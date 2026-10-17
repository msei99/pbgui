/* Guided GitHub web submission: no GitHub credentials or automatic publishing. */
(function () {
  'use strict';
  function node(tag, text) {
    var result = document.createElement(tag);
    if (text != null) result.textContent = text;
    return result;
  }
  var openKey = 'pbgui.support.issue.open';
  var overlay = node('div'); overlay.className = 'pbg-modal-overlay'; overlay.setAttribute('aria-hidden', 'true');
  var dialog = node('section'); dialog.className = 'pbg-modal-window pbg-modal-viewport support-issue-window';
  dialog.setAttribute('role', 'dialog'); dialog.setAttribute('aria-modal', 'true'); dialog.setAttribute('aria-labelledby', 'support-issue-title');
  var head = node('div'); head.className = 'pbg-modal-head';
  var heading = node('h3', 'Report issue on GitHub'); heading.id = 'support-issue-title';
  var close = node('button', '×'); close.type = 'button'; close.className = 'pbg-modal-close'; close.setAttribute('aria-label', 'Close issue report');
  head.append(heading, close);
  var body = node('div'); body.className = 'pbg-modal-body support-issue-body';
  var notice = node('p', 'The issue description is public. The debug attachment is encrypted for PBGui support.'); notice.className = 'support-issue-notice'; body.appendChild(notice);
  var layout = node('div'); layout.className = 'support-issue-layout';
  var formColumn = node('div'); formColumn.className = 'support-issue-fields';
  var optionsColumn = node('div'); optionsColumn.className = 'support-issue-options';
  layout.append(formColumn, optionsColumn); body.appendChild(layout);
  var fields = {};
  var typeLabel = node('label', 'Issue type'), issueType = node('select'); issueType.id = 'support-issue-type';
  issueType.setAttribute('aria-label', 'Issue type');
  [['problem', 'Problem'], ['feature', 'Feature request']].forEach(function (item) {
    var option = node('option', item[1]); option.value = item[0]; issueType.appendChild(option);
  });
  typeLabel.appendChild(issueType); formColumn.appendChild(typeLabel);
  try { if (sessionStorage.getItem('pbgui.support.issue.type') === 'feature') issueType.value = 'feature'; } catch (_) {}
  var fieldLabels = {};
  function field(name, label, max, multiline) {
    var wrap = node('label'), caption = node('span', label), input = node(multiline ? 'textarea' : 'input');
    fieldLabels[name] = caption; wrap.appendChild(caption);
    input.id = 'support-issue-' + name; input.maxLength = max; fields[name] = input;
    wrap.appendChild(input); formColumn.appendChild(wrap);
    input.addEventListener('input', function () { if (aiDraft) aiDraft.invalidate(); invalidate(); updatePublicPreview(); });
  }
  field('subject', 'Title', 120, false);
  field('description', 'What happened?', 1200, true);
  field('steps', 'Steps to reproduce (optional)', 800, true);
  field('expected', 'Expected behavior (optional)', 500, true);
  var aiRoot = node('div'); aiRoot.className = 'support-issue-ai'; formColumn.insertBefore(aiRoot, fields.subject.parentElement);
  var aiDraft = window.PBGuiSupportIssueAI.create(aiRoot, {
    isOpen:function () { return opened; },
    getScreenshots:function () { return screenshots ? screenshots.forAI() : []; },
    getDiagnostics:function () { return window.PBGuiSupportExport.getData(); },
    collectDiagnostics:function () { return window.PBGuiSupportExport.collect(); },
    read:function () {
      var value = {issue_type:issueType.value};
      Object.keys(fields).forEach(function (key) { value[key] = fields[key].value; }); return value;
    },
    apply:function (draft, analysis) {
      Object.keys(fields).forEach(function (key) { fields[key].value = draft[key]; });
      analysisText.value = analysis || ''; analysisDetails.open = Boolean(analysis);
      invalidate(); updatePublicPreview();
    }
  });
  function preview(label, id) {
    var details = node('details'), summary = node('summary', label), text = node('textarea');
    text.id = id; text.readOnly = true; text.setAttribute('aria-label', label);
    details.open = true; details.append(summary, text); optionsColumn.appendChild(details); return text;
  }
  var publicPreview = preview('Public issue preview', 'support-issue-public-preview');
  var analysisDetails = node('details'); analysisDetails.appendChild(node('summary','Analysis and proposed fix (public)'));
  var analysisText = node('textarea'); analysisText.maxLength = 24000; analysisText.setAttribute('aria-label','Analysis and proposed fix (public)');
  analysisDetails.appendChild(analysisText); optionsColumn.appendChild(analysisDetails);
  analysisText.addEventListener('input',function () { invalidate(); updatePublicPreview(); });
  var attachmentLabel = node('label', 'Diagnostic attachment'), attachmentMode = node('select');
  attachmentMode.id = 'support-issue-attachment-mode';
  attachmentMode.setAttribute('aria-label', 'Diagnostic attachment');
  [['none', 'No debug report'], ['debug', 'Include debug report']].forEach(function (item) {
    var option = node('option', item[1]); option.value = item[0]; attachmentMode.appendChild(option);
  });
  attachmentLabel.appendChild(attachmentMode); optionsColumn.appendChild(attachmentLabel);
  var choices = node('details'); choices.className = 'support-attachment-choices';
  choices.appendChild(node('summary', 'Attachment contents'));
  var picker = node('div'); choices.open = true; choices.appendChild(picker); optionsColumn.appendChild(choices);
  window.PBGuiSupportExport.attachPicker(picker);
  var screenshotRoot = node('div'); optionsColumn.appendChild(screenshotRoot);
  var screenshots = window.PBGuiSupportScreenshots.create(screenshotRoot, {surface:dialog, isOpen:function () { return opened; },
    getReport:screenshotReport,
    onChange:function () { invalidate(); updatePublicPreview(); aiDraft.screenshotsChanged(); if (githubPublisher) githubPublisher.imagesChanged(); },
    onBusy:function (value) { if (value) invalidate(); prepare.disabled = busy || value; }});
  var status = node('p'); status.className = 'support-issue-status'; status.setAttribute('role', 'status');
  var error = node('div'); error.className = 'support-issue-error'; error.setAttribute('role', 'alert');
  optionsColumn.append(status, error);
  var actions = node('div'); actions.className = 'support-issue-actions';
  var directPublish = node('button','Review GitHub publication'); directPublish.type = 'button'; directPublish.className = 'primary';
  var prepare = node('button', 'Prepare encrypted attachment'); prepare.type = 'button'; prepare.className = 'primary';
  var proceed = node('button', 'Continue to GitHub'); proceed.type = 'button'; proceed.className = 'primary'; proceed.hidden = true;
  var download = node('a', 'Download attachment again'); download.hidden = true;
  var github = node('a', 'Open GitHub issue'); github.target = '_blank'; github.rel = 'noopener noreferrer'; github.hidden = true;
  actions.append(directPublish, prepare, proceed, download, github);
  var githubRoot = node('div'); optionsColumn.appendChild(githubRoot);
  var githubPublisher = window.PBGuiSupportGitHub.create(githubRoot, {
    isBusy:function () { return busy || screenshots.isBusy(); },
    getScreenshots:function () { return screenshots.forAI(); },
    preview:function (value) { publicPreview.value = value; publicPreview.parentElement.open = true; },
    resetPreview:updatePublicPreview,
    payload:function () {
      if (!fields.subject.value.trim() || !fields.description.value.trim()) throw new Error('Enter or apply an issue title and description first.');
      return {title:fields.subject.value.trim(),body:publicBody(true),report:needsAttachment() ? JSON.parse(screenshots.snapshot()) : null};
    }
  });
  directPublish.addEventListener('click',function () { githubPublisher.open(); });
  dialog.append(head, body, actions); overlay.appendChild(dialog); document.body.appendChild(overlay);
  var floating = window.PBGuiFloatingWindow.attach(dialog, head, {storage_key: 'pbgui.support.issue.geometry.v2'});
  var attachmentUrl = null, request = null, generation = 0, opened = false, busy = false;
  function isFeature() { return issueType.value === 'feature'; }
  function includeDebug() { return !isFeature() || attachmentMode.value === 'debug'; }
  function needsAttachment() { return includeDebug() || screenshots.count() > 0; }
  function screenshotReport() {
    var report = window.PBGuiSupportExport.getData();
    return includeDebug() ? report : {format:'pbgui-support-debug-v1', system:{pbgui_version:((report || {}).system || {}).pbgui_version || 'Unavailable'}};
  }
  function updateMode() {
    var feature = isFeature();
    heading.textContent = feature ? 'Request a feature on GitHub' : 'Report issue on GitHub';
    fieldLabels.description.textContent = feature ? 'What would you like and why?' : 'What happened?';
    fieldLabels.steps.textContent = feature ? 'Suggested solution (optional)' : 'Steps to reproduce (optional)';
    fieldLabels.expected.textContent = feature ? 'Alternatives considered (optional)' : 'Expected behavior (optional)';
    attachmentLabel.hidden = !feature; choices.hidden = !includeDebug();
    invalidate(); updatePublicPreview();
    if (!busy) status.textContent = readyMessage();
  }
  function readyMessage() { return needsAttachment() ? 'Check the public issue preview, then prepare the encrypted attachment.' : 'Check the public feature request preview, then continue to GitHub.'; }
  function savedOpen(value) { try { sessionStorage.setItem(openKey, value ? '1' : '0'); } catch (_) {} }
  function invalidate(preservePublication) {
    if (githubPublisher && !preservePublication) githubPublisher.invalidate();
    generation++;
    if (request) request.abort(); request = null;
    if (attachmentUrl) URL.revokeObjectURL(attachmentUrl); attachmentUrl = null;
    proceed.hidden = true; download.hidden = true; github.hidden = true;
    download.removeAttribute('href'); github.removeAttribute('href');
    prepare.hidden = false; prepare.disabled = busy || (screenshots && screenshots.isBusy()); error.textContent = '';
    prepare.textContent = needsAttachment() ? 'Prepare encrypted attachment' : 'Continue to GitHub';
    if (opened && !busy) status.textContent = readyMessage();
  }
  function publicBody(direct) {
    var report = window.PBGuiSupportExport.getData(), version = 'Unavailable';
    if (report) version = String((report.system || {}).pbgui_version || version);
    return '### ' + (isFeature() ? 'Feature request' : 'What happened?') + '\n' + fields.description.value.trim() +
      '\n\n### ' + (isFeature() ? 'Suggested solution' : 'Steps to reproduce') + '\n' + (fields.steps.value.trim() || 'Not provided') +
      '\n\n### ' + (isFeature() ? 'Alternatives considered' : 'Expected behavior') + '\n' + (fields.expected.value.trim() || 'Not provided') +
      '\n\n### PBGui version\n' + version +
      (analysisText.value.trim() ? '\n\n' + analysisText.value.trim() : '') +
      (needsAttachment() && !direct ? '\n\n### Encrypted attachment\nAttach the downloaded pbgui-debug-encrypted ZIP before submitting this issue. Only PBGui support can decrypt its contents.' : '') + (!direct && screenshots.count() ? '\n\nScreenshots: ' + screenshots.count() + ' (included in the encrypted attachment).' : '');
  }
  function prefillContext() {
    var data = window.PBGuiSupportExport.getData();
    if (!isFeature() && data && data.error_context && !fields.subject.value) fields.subject.value = ('Problem in ' + ({vps_manager:'VPS Manager',v8_optimize_ai_loop:'PB8 AI Loop',v8_optimize:'PB8 Optimize',v7_optimize:'PB7 Optimize'}[data.error_context.area] || data.error_context.area.replace(/_/g,' '))).slice(0, 120);
  }
  function updatePublicPreview() { publicPreview.value = publicBody(); }
  function issueUrl() {
    var url = new URL('https://github.com/msei99/pbgui/issues/new');
    url.searchParams.set('title', fields.subject.value.trim()); url.searchParams.set('body', publicBody());
    if (url.href.length > 8000) throw new Error('The issue description is too long for a GitHub link. Shorten the text.');
    return url.href;
  }
  async function collectReport() {
    if (busy) return;
    invalidate(); busy = true; prepare.disabled = true;
    status.textContent = 'Collecting…';
    try {
      var report = await window.PBGuiSupportExport.collect();
      updatePublicPreview();
      status.textContent = report ? readyMessage() : 'Collection failed. Try again.';
    } finally { busy = false; prepare.disabled = screenshots.isBusy(); }
  }
  function show() {
    opened = true; savedOpen(true); overlay.classList.add('is-open'); overlay.setAttribute('aria-hidden', 'false'); floating.fit();
    prefillContext(); fields.subject.focus(); updatePublicPreview();
    if (includeDebug() && !window.PBGuiSupportExport.getData()) collectReport();
    else status.textContent = readyMessage();
  }
  function hide() {
    aiDraft.invalidate();
    githubPublisher.hide();
    opened = false; savedOpen(false); invalidate(true); overlay.classList.remove('is-open'); overlay.setAttribute('aria-hidden', 'true');
    document.getElementById('debug-issue').focus();
  }
  document.getElementById('debug-issue').addEventListener('click', show);
  close.addEventListener('click', hide);
  issueType.addEventListener('change', function () {
    aiDraft.invalidate();
    try { sessionStorage.setItem('pbgui.support.issue.type', issueType.value); } catch (_) {}
    updateMode();
    if (includeDebug() && !window.PBGuiSupportExport.getData()) collectReport();
  });
  attachmentMode.addEventListener('change', function () {
    updateMode();
    if (includeDebug() && !window.PBGuiSupportExport.getData()) collectReport();
  });
  prepare.addEventListener('click', async function () {
    if (busy || screenshots.isBusy()) return;
    error.textContent = '';
    if (!fields.subject.value.trim() || !fields.description.value.trim()) { error.textContent = isFeature() ? 'Enter a title and describe the feature request.' : 'Enter a title and describe the problem.'; return; }
    var destination;
    try { destination = issueUrl(); } catch (exc) { error.textContent = exc.message; return; }
    if (!needsAttachment()) {
      github.href = destination; github.hidden = false;
      window.open(destination, '_blank', 'noopener,noreferrer');
      status.textContent = 'Review and submit the feature request on GitHub. If the new tab was blocked, use Open GitHub issue.';
      return;
    }
    var snapshot;
    try { snapshot = screenshotReport() ? screenshots.snapshot() : null; }
    catch (exc) { error.textContent = exc.message; return; }
    if (!snapshot) { error.textContent = 'Collect and review a debug report first.'; return; }
    var current = ++generation; request = new AbortController(); prepare.disabled = true;
    status.textContent = 'Encrypting attachment…';
    try {
      var response = await fetch(window.API_BASE + '/issue-attachment', {method: 'POST', credentials: 'same-origin', cache: 'no-store',
        headers: {'Content-Type': 'application/json'}, body: snapshot, signal: request.signal});
      if (!response.ok) { var failure = await response.json(); throw new Error(failure.detail || 'Attachment preparation failed'); }
      var blob = await response.blob();
      if (current !== generation || !opened) return;
      attachmentUrl = URL.createObjectURL(blob); download.href = attachmentUrl;
      download.download = 'pbgui-debug-encrypted-' + new Date().toISOString().replace(/[:.]/g, '-') + '.zip';
      github.href = destination; prepare.hidden = true; proceed.hidden = false;
      status.textContent = 'Continue downloads the encrypted ZIP and opens GitHub. Sign in if needed, attach the ZIP to the description, then submit the issue.';
    } catch (exc) { if (current === generation && exc.name !== 'AbortError') { error.textContent = exc.message; status.textContent = 'Attachment preparation failed.'; } }
    finally { if (current === generation) { request = null; prepare.disabled = false; } }
  });
  proceed.addEventListener('click', function () {
    if (!attachmentUrl) return;
    download.click(); window.open(github.href, '_blank', 'noopener,noreferrer');
    download.hidden = false; github.hidden = false;
    status.textContent = 'On GitHub: attach the downloaded ZIP, then submit. If the new tab or download was blocked, use the links below.';
  });
  overlay.addEventListener('keydown', function (event) {
    if (event.key === 'Escape') { event.preventDefault(); hide(); return; }
    if (event.key !== 'Tab') return;
    var controls = Array.from(dialog.querySelectorAll('button,input,select,textarea,a[href],summary')).filter(function (n) { return !n.disabled && n.getClientRects().length; });
    var first = controls[0], last = controls[controls.length - 1];
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
  });
  document.addEventListener('focusin', function (event) { if (opened && !dialog.contains(event.target)) close.focus(); });
  window.addEventListener('pbgui-debug-changing', function () { invalidate(); });
  window.addEventListener('pagehide', function () { invalidate(true); floating.cancel(); });
  window.addEventListener('pageshow', function () { floating.fit(); });
  window.addEventListener('pbgui-debug-ready', function () { if (opened) { prefillContext(); updatePublicPreview(); } });
  window.addEventListener('pbgui-debug-selection', function () { aiDraft.invalidate(); invalidate(); if (opened) updatePublicPreview(); });
  if (/^[a-f0-9]{32}$/.test(new URL(location.href).searchParams.get('context') || '')) issueType.value = 'problem';
  updateMode();
  try { sessionStorage.removeItem(openKey + '.review'); if (sessionStorage.getItem(openKey) === '1' || /^[a-f0-9]{32}$/.test(new URL(location.href).searchParams.get('context') || '')) show(); } catch (_) {}
})();
