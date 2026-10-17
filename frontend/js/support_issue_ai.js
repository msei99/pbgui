/* Explicit tool-free drafting; selected screenshots require separate user selection. */
(function () {
  'use strict';
  function node(tag, text) {
    var item = document.createElement(tag); if (text != null) item.textContent = text; return item;
  }
  function create(root, options) {
    var open = node('button', 'Draft with AI'); open.type = 'button'; open.className = 'support-screenshot-button';
    open.setAttribute('aria-expanded', 'false');
    var panel = node('div'); panel.className = 'support-ai-editor'; panel.hidden = true;
    var sourceLabel = node('label', 'Describe your problem or idea (any language)'), source = node('textarea');
    source.setAttribute('aria-label', 'Describe your problem or idea (any language)'); source.maxLength = 1200;
    source.placeholder = 'Describe what happened or what you would like. The AI will turn this into an English issue.';
    sourceLabel.appendChild(source);
    var controls = node('div'); controls.className = 'support-ai-actions';
    var generate = node('button', 'Generate English draft'), cancel = node('button', 'Cancel AI draft');
    var investigate = node('button', 'Investigate with AI'); investigate.type = 'button'; investigate.disabled = true;
    generate.type = cancel.type = 'button'; generate.disabled = true; cancel.hidden = true; controls.append(generate, investigate, cancel);
    function select(label, values) {
      var wrap = node('label', label), input = node('select'); input.setAttribute('aria-label', label);
      values.forEach(function (item) { var option = node('option',item[1]); option.value = item[0]; input.appendChild(option); });
      wrap.appendChild(input); return {wrap:wrap,input:input};
    }
    var diagnostics = select('Diagnostic data for AI', [['none','Do not send diagnostics'],['selected','Send selected report contents']]);
    var sourceAccess = select('PBGui source inspection', [['read','Allow read-only installed source inspection'],['none','Do not inspect source']]);
    var note = node('p', 'Uses your AI Chat model. Investigation also reads permitted source and selected diagnostics. Nothing is changed or published.'); note.className = 'support-issue-notice';
    var imageSection = node('div'); imageSection.hidden = true;
    imageSection.appendChild(node('p', 'Select screenshots for AI analysis. Selected images are sent to your AI provider; GitHub attachments remain encrypted.'));
    var imageChoices = node('div'); imageChoices.className = 'support-ai-image-choices'; imageChoices.setAttribute('aria-label', 'Screenshots for AI'); imageSection.appendChild(imageChoices);
    var selectedImages = new Set();
    var status = node('p'); status.setAttribute('role', 'status');
    var error = node('p'); error.className = 'support-issue-error'; error.setAttribute('role', 'alert');
    var suggestion = node('details'), summary = node('summary', 'English AI draft'); suggestion.hidden = true; suggestion.open = true;
    var preview = node('textarea'); preview.readOnly = true; preview.setAttribute('aria-label', 'English AI draft');
    var apply = node('button', 'Apply English draft'); apply.type = 'button'; apply.className = 'support-screenshot-button';
    suggestion.append(summary, preview, apply); panel.append(sourceLabel, imageSection, diagnostics.wrap, sourceAccess.wrap, controls, note, status, error, suggestion); root.append(open, panel);
    var controller = null, timer = null, generation = 0, draft = null, analysis = '';
    function invalidate() {
      generation++;
      if (controller) controller.abort(); controller = null;
      clearTimeout(timer); timer = null;
      draft = null; analysis = ''; suggestion.hidden = true; preview.value = '';
      generate.disabled = !source.value.trim(); cancel.hidden = true; status.textContent = ''; error.textContent = '';
      investigate.disabled = generate.disabled;
    }
    function renderImages() {
      var images = options.getScreenshots();
      selectedImages.forEach(function (id) { if (!images.some(function (item) { return item.id === id; })) selectedImages.delete(id); });
      imageSection.hidden = !images.length; imageChoices.replaceChildren();
      images.forEach(function (item, index) {
        var choice = node('button'); choice.type = 'button'; choice.className = 'support-ai-image';
        choice.setAttribute('aria-label', 'Use screenshot ' + (index + 1) + ' for AI');
        choice.setAttribute('aria-pressed', String(selectedImages.has(item.id))); choice.classList.toggle('selected', selectedImages.has(item.id));
        var thumb = node('img'); thumb.src = item.url; thumb.alt = '';
        choice.append(thumb, node('span', item.name));
        choice.addEventListener('click', function () {
          invalidate(); if (selectedImages.has(item.id)) selectedImages.delete(item.id); else selectedImages.add(item.id); renderImages();
        });
        imageChoices.appendChild(choice);
      });
    }
    open.addEventListener('click', function () {
      panel.hidden = false; open.setAttribute('aria-expanded', 'true');
      renderImages();
      if (!source.value.trim()) source.value = options.read().description || '';
      generate.disabled = Boolean(controller) || !source.value.trim(); source.focus();
      investigate.disabled = generate.disabled;
    });
    source.addEventListener('input', invalidate);
    diagnostics.input.addEventListener('change',invalidate); sourceAccess.input.addEventListener('change',invalidate);
    async function run(mode) {
      if (controller) return;
      var input = options.read();
      input.description = source.value.trim();
      var images = options.getScreenshots().filter(function (item) { return selectedImages.has(item.id); }).map(function (item) { return item.image; });
      if (images.length) input.screenshots = images;
      invalidate();
      if (!input.description) { source.focus(); return; }
      var current = generation;
      controller = new AbortController(); var signal = controller.signal;
      generate.disabled = investigate.disabled = true; cancel.hidden = false; status.textContent = mode === 'diagnose' ? 'Investigating read-only evidence…' : 'Drafting in English…';
      timer = setTimeout(function () {
        if (current !== generation) return;
        invalidate(); error.textContent = 'AI drafting timed out. Your text was kept.';
      }, mode === 'diagnose' ? 305000 : 125000);
      try {
        if (mode === 'diagnose') {
          input.inspect_source = sourceAccess.input.value === 'read';
          if (diagnostics.input.value === 'selected') input.diagnostics = options.getDiagnostics() || await options.collectDiagnostics();
          if (current !== generation) return;
        }
        var payload = JSON.stringify(input);
        if (new Blob([payload]).size > 12 * 1024 * 1024) throw new Error('Text, selected diagnostics and screenshots exceed 12 MiB. Reduce the selection.');
        var response = await fetch(window.API_BASE + (mode === 'diagnose' ? '/diagnose' : '/issue-draft'), {method:'POST', credentials:'same-origin', cache:'no-store',
          headers:{'Content-Type':'application/json'}, body:payload, signal:signal});
        var result = await response.json();
        if (current !== generation || !options.isOpen()) return;
        if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : 'AI issue drafting failed. Your text was kept.');
        var limits = {subject:120, description:1200, steps:800, expected:500};
        if (!result.draft || Object.keys(limits).some(function (key) {
          return typeof result.draft[key] !== 'string' || result.draft[key].length > limits[key];
        }) || !result.draft.subject.trim() || !result.draft.description.trim()) throw new Error('AI returned an invalid draft. Your text was kept.');
        draft = result.draft;
        if (mode === 'diagnose') {
          if (typeof result.analysis_markdown !== 'string' || result.analysis_markdown.length > 24000) throw new Error('AI returned invalid analysis. Your text was kept.');
          analysis = result.analysis_markdown;
        }
        preview.value = '### Title\n' + draft.subject + '\n\n### ' + (input.issue_type === 'feature' ? 'Feature request' : 'What happened?') + '\n' + draft.description +
          (draft.steps ? '\n\n### ' + (input.issue_type === 'feature' ? 'Suggested solution' : 'Steps to reproduce') + '\n' + draft.steps : '') +
          (draft.expected ? '\n\n### ' + (input.issue_type === 'feature' ? 'Alternatives considered' : 'Expected behavior') + '\n' + draft.expected : '');
        preview.value += analysis ? '\n\n' + analysis : '';
        if (Array.isArray(result.questions) && result.questions.length) preview.value += '\n\n### Follow-up questions\n' + result.questions.join('\n');
        suggestion.hidden = false;
        status.textContent = mode === 'diagnose' ? 'Read-only analysis complete. No tests or changes were executed. Review it here; applying an issue draft is optional.' : 'Review the English draft before applying it.';
      } catch (exc) {
        if (current === generation && exc.name !== 'AbortError') { error.textContent = exc.message; status.textContent = ''; }
      } finally {
        if (current === generation) { clearTimeout(timer); timer = null; controller = null; generate.disabled = investigate.disabled = !source.value.trim(); cancel.hidden = true; }
      }
    }
    generate.addEventListener('click',function () { run('draft'); });
    investigate.addEventListener('click',function () { run('diagnose'); });
    cancel.addEventListener('click', invalidate);
    apply.addEventListener('click', function () {
      if (!draft) return;
      var value = draft, details = analysis; invalidate(); options.apply(value,details); status.textContent = 'English draft applied. Review the public issue preview before submitting.';
    });
    window.addEventListener('pagehide', invalidate);
    return {invalidate:invalidate, screenshotsChanged:function () { invalidate(); renderImages(); }};
  }
  window.PBGuiSupportIssueAI = {create:create};
})();
