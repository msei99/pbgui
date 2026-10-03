/* Owner-private Loop instruction versions; runs retain their own prompt snapshot. */
(function () {
  'use strict';
  var base, view, select, name, text, status, message, save, use, remove, catalog;
  var selected = '', run = '', loaded = '', loadedDigest = '', loadedText = '', dirty = false, busy = false;
  var listGeneration = 0, detailGeneration = 0, listRequest, detailRequest, drafts = new Map();
  function node(tag, label, css) {
    var item = document.createElement(tag);
    if (label !== undefined) item.textContent = label;
    if (css) item.className = css;
    return item;
  }
  function active() { return window.state && state.panel === 'loops-instructions'; }
  function key() { return run ? 'run:' + run : selected; }
  function locationState() {
    var url = new URL(location.href);
    if (run) { url.searchParams.set('loop_instructions_run', run); url.searchParams.delete('loop_instruction'); }
    else { url.searchParams.delete('loop_instructions_run'); if (selected) url.searchParams.set('loop_instruction', selected); }
    history.replaceState(null, '', url);
  }
  async function request(path, body, signal, method) {
    var response = await fetch(base + path, {method: method || (body ? 'POST' : 'GET'), credentials: 'same-origin',
      headers: body ? {'Content-Type':'application/json'} : {}, body: body ? JSON.stringify(body) : undefined, signal: signal});
    var data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'Could not load or save Loop instructions.');
    return data;
  }
  function error(exc) { if (exc.name !== 'AbortError') { message.className = 'loop-error'; message.textContent = exc.message; } }
  function controls() {
    var proposed = name.value.trim().toLowerCase();
    var duplicate = !!catalog && catalog.versions.some(function (item) { return item.name.trim().toLowerCase() === proposed; });
    var newName = !!proposed && !duplicate;
    name.setCustomValidity(duplicate ? 'Choose a new, unique version name.' : '');
    save.disabled = busy || !catalog || !loaded || !newName || !text.value.trim();
    text.readOnly = !newName;
    remove.disabled = busy || !catalog || !loaded || !!run || selected === 'default' || dirty || !catalog.versions.some(function (item) { return item.id === selected; });
    use.disabled = busy || !catalog || !!run || dirty || selected === catalog.active || !loaded;
    select.disabled = busy || !catalog;
    name.disabled = text.disabled = busy || !loaded;
    if (catalog) {
      var current = catalog.versions.find(function (item) { return item.id === catalog.active; });
      status.textContent = 'Active for new runs: ' + (current ? current.name : 'Unavailable') +
        (run ? ' · Viewing run ' + run.slice(0, 8) : '') + (dirty ? ' · New version draft' : ' · Read-only');
    }
  }
  function remember() { if (loaded) drafts.set(loaded, {name:name.value, text:text.value, dirty:dirty, digest:loadedDigest}); }
  async function loadSelected() {
    var target = key(), summary = catalog && catalog.versions.find(function (item) { return item.id === selected; });
    if (loaded === target && (run || dirty || summary && summary.digest === loadedDigest)) { controls(); return; }
    var token = ++detailGeneration;
    if (detailRequest) detailRequest.abort();
    detailRequest = new AbortController();
    loaded = ''; controls();
    try {
      var data = await request(run ? '/' + run + '/instructions' : '/instructions/' + encodeURIComponent(selected), null, detailRequest.signal);
      if (token !== detailGeneration || target !== key() || !active()) return;
      var draft = drafts.get(target);
      loaded = target; loadedDigest = data.digest; loadedText = data.text;
      name.value = draft && draft.dirty ? draft.name : '';
      text.value = draft && draft.dirty ? draft.text : data.text;
      dirty = !!(draft && draft.dirty);
      message.className = 'muted-line';
      message.textContent = data.legacy ? 'No historical snapshot was recorded. Showing the current built-in fallback.' : '';
      controls();
    } catch (exc) { if (token === detailGeneration && active()) { error(exc); controls(); } }
  }
  async function refresh() {
    if (!view || !active()) return;
    var token = ++listGeneration;
    if (listRequest) listRequest.abort();
    listRequest = new AbortController();
    try {
      var data = await request('/instructions', null, listRequest.signal);
      if (token !== listGeneration || !active()) return;
      catalog = data;
      if (!selected && !run) selected = data.active;
      var entries = data.versions.map(function (item) { return {id:item.id, label:item.name + (item.id === data.active ? ' · active' : '') + (item.id === 'default' ? '' : ' · ' + item.id.slice(0, 8))}; });
      if (run) entries.unshift({id:'run', label:'Run ' + run.slice(0, 8) + ' · saved instructions'});
      var unavailable = !run && !entries.some(function (item) { return item.id === selected; });
      if (unavailable) {
        if (loaded === selected && dirty) entries.unshift({id:selected, label:'Deleted version · unsaved draft'});
        else { selected = data.active; loaded = ''; }
      }
      var signature = JSON.stringify(entries);
      if (select.dataset.signature !== signature) {
        select.replaceChildren();
        entries.forEach(function (item) { var option = node('option', item.label); option.value = item.id; select.appendChild(option); });
        select.dataset.signature = signature;
      }
      select.value = run ? 'run' : selected;
      locationState(); controls(); await loadSelected();
      if (unavailable) message.textContent = dirty ? 'Source version was deleted. Your unsaved draft is retained.' : 'The requested version is unavailable. Showing the active version.';
    } catch (exc) { if (token === listGeneration && active()) error(exc); }
  }
  async function persist(activateOnly) {
    if (busy || !catalog || !loaded || (activateOnly ? use.disabled : save.disabled)) return;
    if (!activateOnly && (!name.reportValidity() || !text.reportValidity())) return;
    busy = true; controls(); message.textContent = '';
    var target = key();
    try {
      var saved = await request(activateOnly ? '/instructions/active' : '/instructions', activateOnly ?
        {version_id:selected, revision:catalog.revision} : {name:name.value, text:text.value, revision:catalog.revision});
      drafts.delete(target);
      if (key() !== target) { await refresh(); return; }
      selected = saved.id; run = ''; loaded = selected; loadedDigest = saved.digest; dirty = false;
      name.value = ''; text.value = saved.text; loadedText = saved.text;
      locationState(); message.className = 'muted-line'; message.textContent = 'Selected for new runs and Continue. Existing runs retain their instructions.';
      await refresh();
    } catch (exc) { error(exc); await refresh(); }
    finally { busy = false; controls(); }
  }
  async function deleteSelected() {
    if (remove.disabled) return;
    var target = selected, revision = catalog.revision;
    busy = true; controls();
    try {
      var accepted = await PBGuiDialogs.confirm({title:'Delete instruction version',
        message:'Delete this personal version? Existing runs retain their instructions.' +
          (catalog.active === target ? ' New runs will use PBGui default.' : ''), confirmText:'Delete', danger:true});
      if (!accepted) return;
      var result = await request('/instructions/' + encodeURIComponent(target), {revision:revision}, null, 'DELETE');
      drafts.delete(target);
      if (!run && selected === target) { selected = result.active; loaded = ''; dirty = false; }
      await refresh();
    } catch (exc) { error(exc); }
    finally { busy = false; controls(); }
  }
  function mount(apiBase) {
    if (view) return;
    base = apiBase;
    PANEL_META['loops-instructions'] = {title:'AI Loop Instructions', subtitle:''};
    view = node('div', undefined, 'view-panel loop-panel'); view.id = 'panel-loops-instructions';
    document.getElementById('panel-configs').parentNode.appendChild(view);
    var nav = node('button', 'AI Instructions', 'sb-section'); nav.type = 'button'; nav.dataset.panel = 'loops-instructions';
    nav.addEventListener('click', function () { selectPanel('loops-instructions'); });
    document.getElementById('sidebar-inner').insertBefore(nav, document.querySelector('#sidebar-inner .sb-sep'));
    var form = node('form', undefined, 'editor-shell loop-form'); view.appendChild(form);
    form.appendChild(node('h3', 'AI Loop Instructions', 'section-title'));
    var row = node('div', undefined, 'form-row cols-8 loop-grid'); form.appendChild(row);
    function field(id, label, tag, parent, span) {
      var group = node('div', undefined, 'form-group span-' + span), heading = node('label', label);
      var input = node(tag); input.id = id; heading.htmlFor = id; group.append(heading, input); parent.appendChild(group); return input;
    }
    select = field('loop-instruction-version', 'Version', 'select', row, 4);
    name = field('loop-instruction-name', 'New version name', 'input', row, 4); name.maxLength = 80; name.required = true;
    status = node('p', 'Loading instructions…', 'muted-line'); form.appendChild(status);
    row = node('div', undefined, 'form-row cols-8 loop-grid'); form.appendChild(row);
    text = field('loop-instruction-text', 'Instructions', 'textarea', row, 8); text.required = true; text.maxLength = 64000;
    text.style.minHeight = '360px'; text.spellcheck = false;
    var actions = node('div', undefined, 'panel-toolbar'); form.appendChild(actions);
    save = node('button', 'Save & use new version', 'btn primary'); save.type = 'submit'; actions.appendChild(save);
    use = node('button', 'Use selected version', 'btn'); use.type = 'button'; use.addEventListener('click', function () { persist(true); }); actions.appendChild(use);
    remove = node('button', 'Delete selected version', 'btn'); remove.type = 'button'; remove.addEventListener('click', deleteSelected); actions.appendChild(remove);
    message = node('p', '', 'muted-line'); message.setAttribute('role', 'status'); form.appendChild(message);
    form.addEventListener('submit', function (event) { event.preventDefault(); persist(false); });
    [name,text].forEach(function (input) { input.addEventListener('input', function () { dirty = !!name.value.trim() || text.value !== loadedText; remember(); controls(); }); });
    select.addEventListener('change', function () { remember(); if (select.value !== 'run') { run = ''; selected = select.value; } loaded = ''; dirty = false; locationState(); loadSelected(); });
    var url = new URL(location.href); selected = url.searchParams.get('loop_instruction') || ''; run = url.searchParams.get('loop_instructions_run') || '';
    if (run && !/^[0-9a-f]{32}$/.test(run)) { run = ''; message.textContent = 'Invalid run reference.'; }
    controls();
    window.addEventListener('pagehide', function () { listGeneration++; detailGeneration++; if(listRequest)listRequest.abort(); if(detailRequest)detailRequest.abort(); });
  }
  function openRun(id) {
    if (!/^[0-9a-f]{32}$/.test(id)) return;
    remember(); run = id; selected = ''; loaded = ''; dirty = false; locationState(); selectPanel('loops-instructions');
  }
  window.PBGuiLoopInstructions = {mount:mount, refresh:refresh, openRun:openRun};
}());
