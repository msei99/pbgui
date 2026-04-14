(function(global) {
  'use strict';

  function resolveElement(target) {
    if (!target) return null;
    if (typeof target === 'string') return document.getElementById(target);
    return target;
  }

  function captureTextareaAnchor(target) {
    var el = resolveElement(target);
    if (!el || document.activeElement !== el) return null;
    return {
      top: el.getBoundingClientRect().top,
      scrollTop: el.scrollTop,
      selectionStart: el.selectionStart,
      selectionEnd: el.selectionEnd,
    };
  }

  function restoreTextareaAnchor(target, anchor) {
    if (!anchor) return;
    var el = resolveElement(target);
    if (!el) return;
    var delta = el.getBoundingClientRect().top - anchor.top;
    if (delta) window.scrollBy(0, delta);
    el.scrollTop = anchor.scrollTop;
    try {
      el.setSelectionRange(anchor.selectionStart, anchor.selectionEnd);
    } catch (e) {}
  }

  function autoResizeTextarea(target) {
    var el = resolveElement(target);
    if (!el) return;
    var anchor = captureTextareaAnchor(el);
    el.style.height = 'auto';
    el.style.height = el.scrollHeight + 'px';
    restoreTextareaAnchor(el, anchor);
  }

  function openModal(target) {
    var el = resolveElement(target);
    if (el) el.classList.add('open');
    return el;
  }

  function closeModal(target) {
    var el = resolveElement(target);
    if (el) el.classList.remove('open');
    return el;
  }

  function createDebouncedRunner(fn, delay) {
    var timerId = null;
    function schedule() {
      var context = this;
      var args = arguments;
      clearTimeout(timerId);
      timerId = setTimeout(function() {
        timerId = null;
        fn.apply(context, args);
      }, delay);
    }
    schedule.cancel = function() {
      clearTimeout(timerId);
      timerId = null;
    };
    return schedule;
  }

  var fixedValidationEntries = {};
  var fixedValidationOrder = [];

  function ensureFixedValidationOverlay() {
    var style = document.getElementById('pb-json-fixed-validation-style');
    if (!style) {
      style = document.createElement('style');
      style.id = 'pb-json-fixed-validation-style';
      style.textContent = [
        '#pb-json-fixed-validation{display:none;position:fixed;top:64px;right:16px;z-index:1400;width:min(520px,calc(100vw - 32px));max-height:calc(100vh - 80px);padding:10px 12px;border:1px solid rgba(255,95,95,0.65);border-left-width:4px;border-radius:6px;background:rgba(43,12,12,0.985);box-shadow:0 14px 36px rgba(0,0,0,0.45);box-sizing:border-box;overflow:auto;font-size:13px;line-height:1.35;color:#ffd7d7}',
        '#pb-json-fixed-validation.pb-json-fixed-open{display:block}',
        '#pb-json-fixed-validation .pb-json-fixed-main{font-weight:700;color:#fff3f3}',
        '#pb-json-fixed-validation .pb-json-fixed-meta{margin-top:4px;color:#ffb3b3}',
        '#pb-json-fixed-validation .pb-json-fixed-actions{margin-top:8px}',
        '#pb-json-fixed-validation .pb-json-fixed-btn{height:26px;padding:0 10px;border:1px solid rgba(255,95,95,0.45);border-radius:4px;background:rgba(255,255,255,0.08);color:#fff1f1;cursor:pointer;font-size:13px}',
        '#pb-json-fixed-validation .pb-json-fixed-btn:hover{background:rgba(255,255,255,0.08)}'
      ].join('');
      document.head.appendChild(style);
    }
    var overlay = document.getElementById('pb-json-fixed-validation');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'pb-json-fixed-validation';
      overlay.setAttribute('aria-live', 'polite');
      document.body.appendChild(overlay);
    }
    return overlay;
  }

  function renderFixedValidationOverlay(preferredOwnerId) {
    var overlay = ensureFixedValidationOverlay();
    var ownerId = preferredOwnerId && fixedValidationEntries[preferredOwnerId]
      ? preferredOwnerId
      : (fixedValidationOrder.length ? fixedValidationOrder[fixedValidationOrder.length - 1] : null);
    overlay.textContent = '';
    if (!ownerId) {
      overlay.classList.remove('pb-json-fixed-open');
      return;
    }
    var entry = fixedValidationEntries[ownerId];
    if (!entry) {
      overlay.classList.remove('pb-json-fixed-open');
      return;
    }

    var main = document.createElement('div');
    main.className = 'pb-json-fixed-main';
    main.textContent = entry.summary || 'Invalid JSON';
    overlay.appendChild(main);

    if (entry.message) {
      var meta = document.createElement('div');
      meta.className = 'pb-json-fixed-meta';
      meta.textContent = entry.message;
      overlay.appendChild(meta);
    }

    if (entry.action) {
      var actions = document.createElement('div');
      actions.className = 'pb-json-fixed-actions';
      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'pb-json-fixed-btn';
      button.textContent = entry.actionLabel || 'Reveal line in editor';
      button.addEventListener('click', function() {
        entry.action();
      });
      actions.appendChild(button);
      overlay.appendChild(actions);
    }

    overlay.classList.add('pb-json-fixed-open');
  }

  function setFixedValidationStatus(ownerId, opts) {
    if (!ownerId) return;
    fixedValidationEntries[ownerId] = {
      summary: opts && opts.summary ? opts.summary : 'Invalid JSON',
      message: opts && opts.message ? opts.message : '',
      actionLabel: opts && opts.actionLabel ? opts.actionLabel : 'Reveal line in editor',
      action: opts && typeof opts.action === 'function' ? opts.action : null,
    };
    fixedValidationOrder = fixedValidationOrder.filter(function(id) { return id !== ownerId; });
    fixedValidationOrder.push(ownerId);
    renderFixedValidationOverlay(ownerId);
  }

  function clearFixedValidationStatus(ownerId) {
    if (!ownerId) {
      fixedValidationEntries = {};
      fixedValidationOrder = [];
      renderFixedValidationOverlay(null);
      return;
    }
    delete fixedValidationEntries[ownerId];
    fixedValidationOrder = fixedValidationOrder.filter(function(id) { return id !== ownerId; });
    renderFixedValidationOverlay(null);
  }

  global.PBGuiEditorShared = {
    autoResizeTextarea: autoResizeTextarea,
    captureTextareaAnchor: captureTextareaAnchor,
    restoreTextareaAnchor: restoreTextareaAnchor,
    openModal: openModal,
    closeModal: closeModal,
    createDebouncedRunner: createDebouncedRunner,
    setFixedValidationStatus: setFixedValidationStatus,
    clearFixedValidationStatus: clearFixedValidationStatus,
  };
})(window);