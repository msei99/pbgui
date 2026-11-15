/* PB9 authored JSON editor. Runtime paths and config storage remain server-owned. */
(function () {
  'use strict';
  const byId = id => document.getElementById(id);
  const editor = byId('config-json'), filename = byId('config-name');
  const state = {name:'', revision:null, clean:'', version:0, generation:0, busy:false,
    detected:false, pathsDirty:false, uncertain:'', pendingSave:null, conflict:false, conflictName:'', files:[], rejectedRevision:'', runtimeKey:'', disposed:false};
  let polling = null, timer = null, controller = new AbortController(), allowNavigation = false, listSignature = "";
  const initial = new URL(window.location.href);
  byId('config-filter').value = initial.searchParams.get('search') || '';
  const selected = initial.searchParams.get('file') || '';
  if (initial.searchParams.get('paths') === '1') { byId('runtime-fields').hidden = false; byId('runtime-toggle').setAttribute('aria-expanded', 'true'); }
  const dirty = () => editor.value !== state.clean || (!!editor.value && filename.value.trim() !== state.name);
  const errorMessages = {
    runtime_paths_invalid:'PB9 paths were rejected. Check the separate checkout and environment.',
    runtime_not_detected:'A separate PB9 runtime is required. Check Runtime paths.',
    config_directory_not_private:'The PB9 config directory must be owned by this user with permissions 0700.',
    config_file_unavailable:'The PB9 config file or directory is unavailable.',
    config_library_limit:'The PB9 config directory contains too many entries.',
    invalid_document:'Invalid JSON document, duplicate key, Unicode, depth or size limit.',
    config_version_mismatch:'The document schema must match the configured PB9 runtime.',
    external_reference_rejected:'This editor supports inline configs; external file references are not supported.',
    native_config_rejected:'The PB9 runtime rejected this configuration.',
    invalid_config:'The configuration contains unsupported values or internal metadata.',
    invalid_config_name:'Use a visible JSON filename, without paths, up to 255 UTF-8 bytes.',
    helper_timeout:'PB9 validation timed out.', helper_cancelled:'The PB9 operation was stopped.',
    pb9_busy:'PB9 is busy. Wait for the current operation to finish.',
    config_storage_busy:'The config is being saved elsewhere. Your edits are kept.',
    api_restart_pending:'An API restart is pending.',
    config_file_not_found:'The selected config no longer exists. Your edits are kept.'
  };
  function message(text, kind) {
    byId('message').textContent = text;
    byId('message').dataset.kind = kind || '';
  }
  function updateControls() {
    byId('new-template').disabled = state.busy || !state.detected;
    byId('validate-config').disabled = state.busy || !state.detected || !editor.value.trim();
    byId('save-config').disabled = state.busy || !state.detected || !editor.value.trim() ||
      !filename.value.trim() || state.uncertain === filename.value.trim();
    byId('save-paths').disabled = state.busy || !state.pathsDirty;
    byId('open-stored').hidden = !state.conflict;
    byId('open-stored').disabled = state.busy;
    byId('document-status').textContent = state.busy ? 'Working…' :
      editor.value ? (dirty() ? 'Unsaved changes' : 'Saved') : 'No document loaded';
    document.querySelectorAll('#sidebar-inner button').forEach(button => { button.disabled = state.busy; });
  }
  function locationState() {
    const url = new URL(window.location.href);
    if (state.name) url.searchParams.set('file', state.name); else url.searchParams.delete('file');
    const search = byId('config-filter').value;
    if (search) url.searchParams.set('search', search); else url.searchParams.delete('search');
    if (!byId('runtime-fields').hidden) url.searchParams.set('paths', '1'); else url.searchParams.delete('paths');
    history.replaceState(null, '', url.pathname + url.search + url.hash);
  }
  async function request(path, options) {
    const response = await fetch(window.API_BASE + path, Object.assign({credentials:'same-origin', cache:'no-store',
      signal:controller.signal}, options || {}));
    let result = {};
    try { result = await response.json(); } catch (error) {
      if (error.name === 'AbortError') throw error;
      if (response.ok) throw new Error('PB9 returned an invalid response. Your edits are kept.');
    }
    if (!response.ok) {
      const failure = new Error(errorMessages[response.headers.get('X-PBGUI-Error-Codes')] || 'PB9 request failed. Your edits are kept.');
      if (response.status === 401) failure.message = 'Your session expired. Log in in another tab; your edits are kept.';
      failure.code = response.headers.get('X-PBGUI-Error-Codes') || '';
      failure.status = response.status;
      throw failure;
    }
    return result;
  }
  function post(path, body) {
    return request(path, {method:'POST', headers:{'Content-Type':'application/json'}, body});
  }
  function parseEditor() {
    const config = JSON.parse(editor.value);
    if (!config || typeof config !== 'object' || Array.isArray(config)) throw new Error('The config must be a JSON object.');
    return config;
  }
  function applyDocument(config, name, revision, text, preservePosition) {
    const position = preservePosition ? [editor.selectionStart, editor.selectionEnd, editor.scrollTop] : null;
    editor.value = typeof text === 'string' ? text : JSON.stringify(config, null, 4) + '\n';
    if (position) { editor.setSelectionRange(Math.min(position[0], editor.value.length), Math.min(position[1], editor.value.length)); editor.scrollTop = position[2]; }
    state.clean = editor.value;
    state.name = name || '';
    state.revision = revision || null;
    filename.value = name || '';
    state.conflict = false; state.conflictName = ''; state.rejectedRevision = '';
    state.uncertain = ''; state.pendingSave = null;
    state.version++;
    locationState();
    renderFiles();
    updateControls();
  }
  function reconcileSaved(loaded, pending, warning) {
    if (loaded.matches !== true) {
      state.uncertain = ''; state.pendingSave = null;
      state.conflict = true; state.conflictName = pending.name;
      message('The stored file differs from the submitted version. Your edits are kept.', 'error');
      updateControls(); return false;
    }
    const unchanged = state.version === pending.version;
    state.name = pending.name; state.revision = loaded.revision; state.uncertain = ''; state.pendingSave = null;
    state.conflict = false; state.conflictName = '';
    if (unchanged) applyDocument(loaded.config, pending.name, loaded.revision, loaded.config_text);
    else state.clean = loaded.config_text || JSON.stringify(loaded.config, null, 4) + '\n';
    locationState();
    message(warning || (unchanged ? 'Saved ' + pending.name : 'Saved the submitted version; newer edits are kept.'), warning ? 'error' : 'success');
    if (!state.files.includes(pending.name)) { state.files.push(pending.name); state.files.sort(); renderFiles(); }
    updateControls();
    return true;
  }
  function renderFiles() {
    const host = byId('sidebar-inner'), filter = byId('config-filter').value.toLowerCase();
    const signature = JSON.stringify([state.files, filter]);
    if (signature === listSignature) {
      host.querySelectorAll('button').forEach(button => {
        const active = button.textContent === state.name;
        button.classList.toggle('active', active);
        if (active) button.setAttribute('aria-current', 'page'); else button.removeAttribute('aria-current');
      });
      updateControls(); return;
    }
    listSignature = signature;
    const focused = host.contains(document.activeElement) ? document.activeElement.textContent : null;
    host.replaceChildren();
    const visible = state.files.filter(name => name.toLowerCase().includes(filter));
    for (const name of visible) {
      const button = document.createElement('button');
      button.type = 'button'; button.className = 'sb-section'; button.textContent = name;
      button.classList.toggle('active', name === state.name);
      if (name === state.name) button.setAttribute('aria-current', 'page');
      button.addEventListener('click', () => openFile(name));
      host.appendChild(button);
      if (focused === name) button.focus();
    }
    if (!visible.length) {
      const empty = document.createElement('p'); empty.className = 'muted';
      empty.textContent = state.files.length ? 'No matching configs' : 'No saved configs'; host.appendChild(empty);
    }
    updateControls();
  }
  function applyRuntime(status) {
    const key = JSON.stringify([status.pb9dir, status.pb9venv, status.config_schema, status.commit]);
    if (key !== state.runtimeKey) {
      if (state.runtimeKey && editor.value && !state.busy) message('PB9 runtime changed; validate the document again.');
      state.rejectedRevision = ''; state.runtimeKey = key;
    }
    state.detected = !!status.detected;
    byId('runtime-status').textContent = status.detected ? 'PB9 detected · ' + status.config_schema :
      status.configured ? 'PB9 runtime unavailable' : 'PB9 runtime not configured';
    if (!state.pathsDirty) {
      byId('pb9-source').value = status.pb9dir || '';
      byId('pb9-python').value = status.pb9venv || '';
    }
    updateControls();
  }
  async function discardAllowed(navigation) {
    return !(dirty() || (navigation && state.pathsDirty)) || window.PBGuiDialogs.confirm({title:'Unsaved changes',
      message:'Discard unsaved changes and continue?', confirmText:'Discard edits'});
  }
  async function action(work) {
    if (state.busy || state.disposed) return;
    state.busy = true; updateControls();
    const generation = ++state.generation;
    try {
      if (polling) await polling;
      if (!state.disposed && generation === state.generation) await work(generation);
    } catch (error) {
      if (!state.disposed && generation === state.generation && error.name !== 'AbortError') message(error.message, 'error');
    } finally {
      if (generation === state.generation) { state.busy = false; updateControls(); }
    }
  }
  async function openFile(name) {
    if (state.busy || !await discardAllowed()) return;
    return action(async generation => {
      const version = state.version;
      const loaded = await post('/configs/load', JSON.stringify({name}));
      if (state.disposed || generation !== state.generation) return;
      if (version !== state.version) { message('Your newer edits were kept; the file was not opened.', 'error'); return; }
      applyDocument(loaded.config, name, loaded.revision, loaded.config_text);
      message('Opened ' + name, 'success');
    });
  }
  async function syncView(forceDocument) {
    if (state.disposed || state.busy || polling || document.hidden) return polling;
    const generation = state.generation;
    const running = (async () => {
      try {
        const runtime = await request('/runtime');
        if (state.disposed || generation !== state.generation) return;
        applyRuntime(runtime);
        const inventory = await request('/configs');
        if (state.disposed || generation !== state.generation) return;
        const files = inventory.configs || [];
        if (JSON.stringify(files) !== JSON.stringify(state.files)) { state.files = files; renderFiles(); }
        if (state.pendingSave && state.uncertain && !files.includes(state.pendingSave.name)) {
          const name = state.pendingSave.name; state.uncertain = ''; state.pendingSave = null; state.conflict = false; state.conflictName = '';
          message(name === state.name && state.revision ? 'The saved target is missing. Your edits are kept; save with another filename.' :
            'No saved target was found. Your edits are kept; you can retry Save.', 'error');
          updateControls(); return;
        }
        if (state.pendingSave && state.uncertain && files.includes(state.pendingSave.name)) {
          const pending = state.pendingSave;
          let loaded;
          try { loaded = await post('/configs/reconcile', '{"name":' + JSON.stringify(pending.name) + ',"config":' + pending.text + '}'); }
          catch (error) {
            if (!state.disposed && generation === state.generation && [404,422].includes(error.status)) {
              state.pendingSave = null; state.uncertain = ''; state.conflict = error.status !== 404;
              state.conflictName = pending.name; updateControls();
            }
            throw error;
          }
          if (!state.disposed && generation === state.generation && pending === state.pendingSave) {
            if (reconcileSaved(loaded, pending, pending.warning || '') && !pending.warning) {
              message('Stored saved version found. Your current text is kept.', 'success');
            }
          }
          return;
        }
        if (state.name && !files.includes(state.name)) {
          state.conflict = false; state.conflictName = '';
          message('The selected file no longer exists. Your edits are kept; save with another filename.', 'error'); updateControls();
        } else if (state.name && state.detected) {
          const name = state.name, version = state.version;
          const observed = await post('/configs/revision', JSON.stringify({name}));
          if (state.disposed || generation !== state.generation || state.name !== name) return;
          if (observed.revision !== state.revision) {
            if (!dirty() && state.version === version) {
              if (state.rejectedRevision === observed.revision && forceDocument !== true) return;
              try {
                const loaded = await post('/configs/load', JSON.stringify({name}));
                if (state.disposed || generation !== state.generation || state.name !== name) return;
                if (!dirty() && state.version === version) applyDocument(loaded.config, name, loaded.revision, loaded.config_text, true);
                else { state.conflict = true; state.conflictName = name; message('The file changed on disk. Your edits are kept.', 'error'); updateControls(); }
              } catch (error) {
                if (error.status === 422) state.rejectedRevision = observed.revision;
                throw error;
              }
            } else { state.conflict = true; state.conflictName = name; message('The file changed on disk. Your edits are kept.', 'error'); updateControls(); }
          }
        }
      } catch (error) {
        if (!state.disposed && generation === state.generation && error.name !== 'AbortError') message(error.message, 'error');
      }
    })();
    polling = running;
    try { await running; } finally { if (polling === running) polling = null; }
  }
  byId('new-template').addEventListener('click', async () => {
    if (state.busy || !await discardAllowed()) return;
    action(async generation => {
      const version = state.version;
      const result = await post('/template', '{}');
      if (state.disposed || generation !== state.generation) return;
      if (version !== state.version) { message('Your newer edits were kept; the template was not opened.', 'error'); return; }
      applyDocument(result.config, '', null, result.config_text);
      // A new template is a draft even before the first keystroke.
      state.clean = ''; updateControls(); message('New PB9 template', 'success');
    });
  });
  byId('validate-config').addEventListener('click', () => action(async generation => {
    parseEditor();
    const text = editor.value, version = state.version;
    await post('/validate', '{"config":' + text + '}');
    if (!state.disposed && generation === state.generation) message(state.version === version ? 'Configuration is valid.' : 'The submitted version is valid; your newer edits are not validated.', 'success');
  }));
  byId('save-config').addEventListener('click', () => action(async generation => {
    parseEditor();
    const text = editor.value, version = state.version, name = filename.value.trim();
    const revision = name === state.name ? state.revision : null;
    const pending = {name, text, version, acknowledged:false, warning:''};
    // A new explicit filename/intent must not inherit a previous uncertainty lock.
    state.pendingSave = pending; state.uncertain = '';
    let warning = '';
    try {
      await post('/configs/save', '{"name":' + JSON.stringify(name) + ',"revision":' + JSON.stringify(revision) + ',"config":' + text + '}');
      pending.acknowledged = true;
    } catch (error) {
      if (state.disposed || generation !== state.generation) return;
      if ([401,403].includes(error.status)) { state.pendingSave = null; throw error; }
      if (['config_file_exists','config_revision_conflict'].includes(error.code)) { state.pendingSave = null; state.conflict = true; state.conflictName = name; throw new Error('The file already exists or changed. Your edits are kept; open its stored version or save with another filename.'); }
      if (!['config_file_created_io_failed','config_file_updated_io_failed'].includes(error.code)) {
        if (!error.code || ['api_operation_failed','api_response_failed'].includes(error.code)) {
          state.uncertain = name; state.conflict = true; state.conflictName = name;
          throw new Error('Save may have completed. Your text is kept; the stored version will be checked automatically.');
        }
        state.pendingSave = null; throw error;
      }
      warning = 'The file was published, but its storage acknowledgement failed. Check storage before saving again.';
      pending.acknowledged = true; pending.warning = warning;
    }
    if (state.disposed || generation !== state.generation) return;
    state.uncertain = name; state.conflictName = name;
    try {
      const loaded = await post('/configs/reconcile', '{"name":' + JSON.stringify(name) + ',"config":' + text + '}');
      if (state.disposed || generation !== state.generation) return;
      reconcileSaved(loaded, pending, warning);
    } catch (error) {
      if (state.disposed || generation !== state.generation) return;
      state.conflict = true;
      throw new Error(warning || (pending.acknowledged ? 'Saved; the stored revision could not be confirmed. Your text is kept and will be checked automatically.' : 'Save may have completed. Your text is kept; the stored version will be checked automatically.'));
    }
  }));
  byId('open-stored').addEventListener('click', () => { const name = state.conflictName || state.name; if (name) openFile(name); });
  byId('runtime-toggle').addEventListener('click', () => {
    const panel = byId('runtime-fields'); panel.hidden = !panel.hidden;
    byId('runtime-toggle').setAttribute('aria-expanded', String(!panel.hidden)); locationState();
  });
  ['pb9-source','pb9-python'].forEach(id => byId(id).addEventListener('input', () => { state.pathsDirty = true; updateControls(); }));
  byId('save-paths').addEventListener('click', () => action(async generation => {
    const source = byId('pb9-source').value, python = byId('pb9-python').value;
    const result = await post('/runtime/paths', JSON.stringify({pb9dir:source, pb9venv:python}));
    if (state.disposed || generation !== state.generation) return;
    if (source === byId('pb9-source').value && python === byId('pb9-python').value) state.pathsDirty = false;
    applyRuntime(result); message('PB9 paths saved.', 'success');
  }));
  editor.addEventListener('input', () => { state.version++; updateControls(); });
  filename.addEventListener('input', () => { state.version++; state.conflict = false; state.conflictName = ''; updateControls(); });
  byId('config-filter').addEventListener('input', () => { renderFiles(); locationState(); });
  window.addEventListener('beforeunload', event => { if ((dirty() || state.pathsDirty) && !allowNavigation) { event.preventDefault(); event.returnValue = ''; } });
  document.addEventListener('click', async event => {
    const target = event.target instanceof Element ? event.target.closest('.nav-item[data-page]') : null;
    if (!target || !(dirty() || state.pathsDirty) || allowNavigation) return;
    event.preventDefault(); event.stopImmediatePropagation();
    if (await discardAllowed(true)) { allowNavigation = true; target.click(); setTimeout(() => { allowNavigation = false; }, 0); }
  }, true);
  window.addEventListener('focus', () => syncView(true));
  document.addEventListener('visibilitychange', () => { if (!document.hidden) syncView(true); });
  window.addEventListener('pagehide', () => { state.disposed = true; state.generation++; state.busy = false; polling = null; clearInterval(timer); controller.abort(); });
  window.addEventListener('pageshow', event => {
    if (event.persisted) { clearInterval(timer); allowNavigation = false; state.disposed = false; controller = new AbortController(); timer = setInterval(syncView, 10000); syncView(); }
  });
  updateControls();
  (async () => {
    await syncView();
    if (selected && !state.disposed) { await openFile(selected); if (!state.name) locationState(); }
    if (!state.disposed) { clearInterval(timer); timer = setInterval(syncView, 10000); }
  })();
})();
