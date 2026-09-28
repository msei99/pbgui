(function (global) {
  'use strict';

  var STORAGE_KEY = 'pbgui.sidebar.width';
  var DEFAULT_WIDTH = 240;
  var MIN_WIDTH = 160;
  var MAX_WIDTH = 420;
  var MOBILE_WIDTH = 760;

  function isMobile() {
    return global.innerWidth <= MOBILE_WIDTH;
  }

  function clampWidth(value) {
    var width = Number(value);
    if (!Number.isFinite(width) || value === null || value === '') width = DEFAULT_WIDTH;
    return Math.round(Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, width)));
  }

  function savedWidth() {
    try {
      return clampWidth(global.localStorage.getItem(STORAGE_KEY));
    } catch (error) {
      return DEFAULT_WIDTH;
    }
  }

  function init(options) {
    var opts = options || {};
    var sidebar = document.getElementById(opts.sidebarId || 'sidebar');
    var handle = document.getElementById(opts.handleId || 'sidebar-resize');
    if (!sidebar || !handle || handle.dataset.sidebarResizeBound === 'true') return;

    var currentWidth = savedWidth();
    var activePointer = null;
    var startX = 0;
    var startWidth = 0;
    handle.dataset.sidebarResizeBound = 'true';
    handle.setAttribute('role', 'separator');
    handle.setAttribute('aria-label', 'Resize sidebar');
    handle.setAttribute('aria-controls', sidebar.id);
    handle.setAttribute('aria-orientation', 'vertical');
    handle.setAttribute('tabindex', '0');
    handle.setAttribute('aria-valuemin', String(MIN_WIDTH));
    handle.setAttribute('aria-valuemax', String(MAX_WIDTH));

    function applyWidth() {
      var width = clampWidth(currentWidth);
      document.documentElement.style.setProperty('--pbgui-sidebar-width', width + 'px');
      sidebar.style.width = isMobile() ? '' : width + 'px';
      handle.setAttribute('aria-valuenow', String(width));
    }

    function saveWidth() {
      currentWidth = clampWidth(currentWidth);
      try {
        global.localStorage.setItem(STORAGE_KEY, String(currentWidth));
      } catch (error) {
        // Resizing still works when browser storage is unavailable.
      }
    }

    function finishDrag(event) {
      if (activePointer === null) return;
      if (event && event.pointerId !== undefined && event.pointerId !== activePointer) return;
      var pointerId = activePointer;
      activePointer = null;
      handle.classList.remove('active');
      document.documentElement.classList.remove('pbgui-sidebar-resizing');
      if (handle.hasPointerCapture(pointerId)) handle.releasePointerCapture(pointerId);
      saveWidth();
    }

    handle.addEventListener('pointerdown', function (event) {
      if (isMobile() || activePointer !== null || event.isPrimary === false || event.button !== 0) return;
      event.preventDefault();
      activePointer = event.pointerId;
      startX = event.clientX;
      startWidth = sidebar.getBoundingClientRect().width;
      // Keep receiving moves and releases over embedded frames and outside the handle.
      handle.setPointerCapture(activePointer);
      handle.classList.add('active');
      document.documentElement.classList.add('pbgui-sidebar-resizing');
    });
    document.addEventListener('pointermove', function (event) {
      if (event.pointerId !== activePointer) return;
      currentWidth = startWidth + event.clientX - startX;
      applyWidth();
    });
    document.addEventListener('pointerup', finishDrag);
    document.addEventListener('pointercancel', finishDrag);
    handle.addEventListener('lostpointercapture', finishDrag);
    global.addEventListener('blur', finishDrag);
    handle.addEventListener('keydown', function (event) {
      if (isMobile()) return;
      if (event.key === 'ArrowLeft') currentWidth = clampWidth(currentWidth) - 10;
      else if (event.key === 'ArrowRight') currentWidth = clampWidth(currentWidth) + 10;
      else if (event.key === 'Home') currentWidth = MIN_WIDTH;
      else if (event.key === 'End') currentWidth = MAX_WIDTH;
      else return;
      event.preventDefault();
      applyWidth();
      saveWidth();
    });
    global.addEventListener('resize', function () {
      if (isMobile()) finishDrag();
      applyWidth();
    });
    applyWidth();
  }

  global.PBGuiSidebarResize = { init: init };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { init(); });
  } else {
    init();
  }
}(window));
