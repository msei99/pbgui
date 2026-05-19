(function () {
  'use strict';

  var STYLE_ID = 'pbgui-dialogs-style';
  var OVERLAY_ID = 'pbgui-dialog-ovl';
  var resolveDialog = null;
  var returnFocus = null;
  var currentMode = 'confirm';

  function ensureStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement('style');
    style.id = STYLE_ID;
    style.textContent = [
      ':root{--fs-xs:11px;--fs-sm:13px;--fs-base:14px;--fs-md:15px;--fs-lg:18px;--sp-xs:4px;--sp-sm:8px;--sp-md:12px;--sp-lg:20px;--input-h:32px;--btn-h:32px;}',
      '#' + OVERLAY_ID + '{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:20000;align-items:center;justify-content:center;backdrop-filter:blur(2px);padding:var(--sp-lg);}',
      '#' + OVERLAY_ID + '.visible{display:flex;}',
      '#pbgui-dialog-box{background:#131b2b;border:1px solid #2d3748;border-radius:14px;box-shadow:0 20px 70px rgba(0,0,0,.9);overflow:hidden;width:min(480px,92vw);max-width:92vw;}',
      '#pbgui-dialog-header{display:flex;justify-content:space-between;align-items:center;gap:var(--sp-sm);padding:.85rem 1.1rem;border-bottom:1px solid #1e2736;background:#111827;}',
      '#pbgui-dialog-title{font-size:var(--fs-md);font-weight:700;color:#e2e8f0;}',
      '#pbgui-dialog-close{background:transparent;border:none;color:#64748b;font-size:var(--fs-lg);cursor:pointer;padding:.2rem .35rem;border-radius:5px;line-height:1;}',
      '#pbgui-dialog-close:hover{color:#e2e8f0;background:rgba(255,255,255,.06);}',
      '#pbgui-dialog-body{display:grid;gap:var(--sp-md);padding:var(--sp-lg);}',
      '#pbgui-dialog-message{font-size:var(--fs-base);line-height:1.5;color:#e2e8f0;white-space:pre-wrap;}',
      '#pbgui-dialog-detail{font-size:var(--fs-sm);line-height:1.45;color:#94a3b8;white-space:pre-wrap;}',
      '#pbgui-dialog-detail[hidden],#pbgui-dialog-field[hidden],#pbgui-dialog-cancel[hidden]{display:none!important;}',
      '#pbgui-dialog-field{display:grid;gap:var(--sp-xs);}',
      '#pbgui-dialog-field label{font-size:var(--fs-sm);font-weight:600;color:#cbd5e1;}',
      '#pbgui-dialog-input{width:100%;height:var(--input-h);padding:0 var(--sp-sm);border-radius:8px;border:1px solid #2d3748;background:#0f172a;color:#e2e8f0;font-size:var(--fs-base);outline:none;}',
      '#pbgui-dialog-input:focus{border-color:#63b3ed;box-shadow:0 0 0 1px rgba(99,179,237,.4);}',
      '#pbgui-dialog-actions{display:flex;justify-content:flex-end;gap:var(--sp-sm);flex-wrap:wrap;}',
      '.pbgui-dialog-btn{display:inline-flex;align-items:center;justify-content:center;height:var(--btn-h);padding:0 var(--sp-md);border-radius:8px;border:1px solid transparent;font-size:var(--fs-base);font-weight:600;cursor:pointer;transition:background .15s,border-color .15s,color .15s;}',
      '.pbgui-dialog-btn.secondary{background:rgba(99,179,237,.08);border-color:rgba(99,179,237,.25);color:#e2e8f0;}',
      '.pbgui-dialog-btn.secondary:hover{background:rgba(99,179,237,.16);border-color:#63b3ed;}',
      '.pbgui-dialog-btn.primary{background:#63b3ed;border-color:#63b3ed;color:#0b1220;}',
      '.pbgui-dialog-btn.primary:hover{background:#7cc4f5;}'
    ].join('');
    document.head.appendChild(style);
  }

  function ensureOverlay() {
    if (document.getElementById(OVERLAY_ID)) return;
    ensureStyles();
    var wrapper = document.createElement('div');
    wrapper.innerHTML = ''
      + '<div id="' + OVERLAY_ID + '" aria-hidden="true">'
      +   '<div id="pbgui-dialog-box" role="dialog" aria-modal="true" aria-labelledby="pbgui-dialog-title">'
      +     '<div id="pbgui-dialog-header">'
      +       '<div id="pbgui-dialog-title">Confirm action</div>'
      +       '<button type="button" id="pbgui-dialog-close" aria-label="Close">&#x2715;</button>'
      +     '</div>'
      +     '<div id="pbgui-dialog-body">'
      +       '<div id="pbgui-dialog-message"></div>'
      +       '<div id="pbgui-dialog-detail" hidden></div>'
      +       '<div id="pbgui-dialog-field" hidden>'
      +         '<label for="pbgui-dialog-input" id="pbgui-dialog-label">Value</label>'
      +         '<input type="text" id="pbgui-dialog-input" autocomplete="off">'
      +       '</div>'
      +       '<div id="pbgui-dialog-actions">'
      +         '<button type="button" class="pbgui-dialog-btn secondary" id="pbgui-dialog-cancel">Cancel</button>'
      +         '<button type="button" class="pbgui-dialog-btn primary" id="pbgui-dialog-accept">Confirm</button>'
      +       '</div>'
      +     '</div>'
      +   '</div>'
      + '</div>';
    document.body.appendChild(wrapper.firstChild);

    var overlay = document.getElementById(OVERLAY_ID);
    var closeBtn = document.getElementById('pbgui-dialog-close');
    var cancelBtn = document.getElementById('pbgui-dialog-cancel');
    var acceptBtn = document.getElementById('pbgui-dialog-accept');
    var input = document.getElementById('pbgui-dialog-input');

    overlay.addEventListener('click', function (event) {
      if (event.target === overlay) close(false);
    });
    closeBtn.addEventListener('click', function () { close(false); });
    cancelBtn.addEventListener('click', function () { close(false); });
    acceptBtn.addEventListener('click', function () { close(true); });
    input.addEventListener('keydown', function (event) {
      if (event.key === 'Enter') {
        event.preventDefault();
        close(true);
      }
    });
    document.addEventListener('keydown', function (event) {
      var visible = overlay.classList.contains('visible');
      if (!visible) return;
      if (event.key === 'Escape') {
        event.preventDefault();
        close(false);
      } else if (event.key === 'Enter' && currentMode !== 'prompt') {
        if (event.target && event.target.id === 'pbgui-dialog-cancel') return;
        event.preventDefault();
        close(true);
      }
    });
  }

  function close(accepted) {
    var overlay = document.getElementById(OVERLAY_ID);
    var input = document.getElementById('pbgui-dialog-input');
    if (overlay) {
      overlay.classList.remove('visible');
      overlay.setAttribute('aria-hidden', 'true');
    }
    var resolver = resolveDialog;
    var focusTarget = returnFocus;
    resolveDialog = null;
    returnFocus = null;
    var result;
    if (currentMode === 'prompt') {
      result = accepted ? String(input && input.value != null ? input.value : '') : null;
    } else if (currentMode === 'alert') {
      result = true;
    } else {
      result = Boolean(accepted);
    }
    if (focusTarget && typeof focusTarget.focus === 'function') {
      try { focusTarget.focus(); } catch (_) {}
    }
    if (typeof resolver === 'function') resolver(result);
  }

  function open(mode, options) {
    options = options || {};
    ensureOverlay();
    var overlay = document.getElementById(OVERLAY_ID);
    var title = document.getElementById('pbgui-dialog-title');
    var message = document.getElementById('pbgui-dialog-message');
    var detail = document.getElementById('pbgui-dialog-detail');
    var field = document.getElementById('pbgui-dialog-field');
    var label = document.getElementById('pbgui-dialog-label');
    var input = document.getElementById('pbgui-dialog-input');
    var cancelBtn = document.getElementById('pbgui-dialog-cancel');
    var acceptBtn = document.getElementById('pbgui-dialog-accept');

    if (!overlay || !title || !message || !detail || !field || !label || !input || !cancelBtn || !acceptBtn) {
      if (mode === 'prompt') return Promise.resolve(window.prompt(String(options.message || ''), String(options.defaultValue || '')));
      if (mode === 'alert') {
        window.alert(String(options.message || 'Done.'));
        return Promise.resolve(true);
      }
      return Promise.resolve(window.confirm(String(options.message || 'Are you sure?')));
    }

    if (typeof resolveDialog === 'function') {
      var previous = resolveDialog;
      resolveDialog = null;
      previous(mode === 'prompt' ? null : false);
    }

    currentMode = mode;
    title.textContent = String(options.title || (mode === 'prompt' ? 'Enter value' : mode === 'alert' ? 'Notice' : 'Confirm action'));
    message.textContent = String(options.message || (mode === 'prompt' ? 'Enter a value.' : mode === 'alert' ? 'Done.' : 'Are you sure?'));
    var detailText = String(options.detail || '').trim();
    detail.textContent = detailText;
    detail.hidden = !detailText;
    field.hidden = mode !== 'prompt';
    label.textContent = String(options.label || 'Value');
    input.value = String(options.defaultValue == null ? '' : options.defaultValue);
    input.placeholder = String(options.placeholder || '');
    acceptBtn.textContent = String(options.confirmText || (mode === 'prompt' ? 'Save' : mode === 'alert' ? 'OK' : 'Confirm'));
    cancelBtn.textContent = String(options.cancelText || 'Cancel');
    cancelBtn.hidden = mode === 'alert';
    returnFocus = document.activeElement;

    return new Promise(function (resolve) {
      resolveDialog = resolve;
      overlay.classList.add('visible');
      overlay.setAttribute('aria-hidden', 'false');
      if (mode === 'prompt') {
        input.focus();
        try { input.select(); } catch (_) {}
      } else {
        acceptBtn.focus();
      }
    });
  }

  var api = {
    confirm: function (options) {
      return open('confirm', options);
    },
    alert: function (options) {
      return open('alert', options);
    },
    prompt: function (options) {
      return open('prompt', options);
    }
  };

  window.PBGuiDialogs = api;
  window.PBGuiAlert = api.alert;
  window.PBGuiPrompt = api.prompt;
  if (typeof window.PBGuiConfirm !== 'function') {
    window.PBGuiConfirm = api.confirm;
  }
}());
