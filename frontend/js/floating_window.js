/* Shared title-bar dragging and edge/corner resizing for detail windows. */
(function () {
  'use strict';
  function attach(panel, header, options) {
    options = options || {};
    var gesture = null, geometry = null, disposed = false, handles = [];
    var margin = 16, storageKey = options.storage_key || '';
    function limits() {
      var width = Math.max(1, innerWidth - margin * 2), height = Math.max(1, innerHeight - margin * 2);
      return {width:width,height:height,minWidth:Math.min(420,width),minHeight:Math.min(250,height)};
    }
    function apply(rect) {
      var bounds = limits();
      var width = Math.min(bounds.width, Math.max(bounds.minWidth, rect.width));
      var height = Math.min(bounds.height, Math.max(bounds.minHeight, rect.height));
      var left = Math.max(margin, Math.min(rect.left, innerWidth - width - margin));
      var top = Math.max(margin, Math.min(rect.top, innerHeight - height - margin));
      geometry = {left:left,top:top,width:width,height:height};
      panel.classList.add('is-positioned');
      Object.keys(geometry).forEach(function (key) { panel.style[key] = geometry[key] + 'px'; });
    }
    function save() {
      if (!storageKey || !geometry) return;
      try { localStorage.setItem(storageKey, JSON.stringify(geometry)); } catch (_) {}
    }
    function fit() { if (geometry) { apply(geometry); save(); } }
    if (storageKey) {
      try {
        var saved = JSON.parse(localStorage.getItem(storageKey) || 'null');
        if (saved && ['left','top','width','height'].every(function (key) { return typeof saved[key] === 'number' && Number.isFinite(saved[key]); })) apply(saved);
      } catch (_) {}
    }
    header.classList.add('pbg-window-titlebar');
    ['n','s','e','w','ne','nw','se','sw'].forEach(function (direction) {
      var handle = document.createElement('div');
      handle.className = 'pbg-window-resize pbg-window-resize-' + direction;
      handle.dataset.windowResize = direction;
      handle.setAttribute('aria-hidden','true');
      panel.appendChild(handle); handles.push(handle);
    });
    function start(event) {
      if (disposed || event.button !== 0) return;
      var handle = event.target.closest('[data-window-resize]');
      if (!handle && (!header.contains(event.target) || event.target.closest('button,input,select,textarea,a'))) return;
      var rect = panel.getBoundingClientRect();
      apply({left:rect.left,top:rect.top,width:rect.width,height:rect.height});
      gesture = {id:event.pointerId,kind:handle ? handle.dataset.windowResize : 'move',x:event.clientX,y:event.clientY,rect:Object.assign({},geometry)};
      panel.setPointerCapture(event.pointerId); panel.classList.add('is-interacting'); event.preventDefault();
    }
    function move(event) {
      if (!gesture || gesture.id !== event.pointerId) return;
      var rect = gesture.rect, dx = event.clientX - gesture.x, dy = event.clientY - gesture.y;
      var next = Object.assign({},rect), kind = gesture.kind, bounds = limits();
      if (kind === 'move') { next.left += dx; next.top += dy; }
      else {
        var right = rect.left + rect.width, bottom = rect.top + rect.height;
        if (kind.includes('w')) next.left = Math.max(margin,Math.min(right - bounds.minWidth,rect.left + dx));
        if (kind.includes('n')) next.top = Math.max(margin,Math.min(bottom - bounds.minHeight,rect.top + dy));
        if (kind.includes('e')) right = Math.min(innerWidth-margin,Math.max(rect.left+bounds.minWidth,right+dx));
        if (kind.includes('s')) bottom = Math.min(innerHeight-margin,Math.max(rect.top+bounds.minHeight,bottom+dy));
        next.width = right - next.left; next.height = bottom - next.top;
      }
      apply(next); event.preventDefault();
    }
    function end(event) {
      if (!gesture || event && event.pointerId !== gesture.id) return;
      var id = gesture.id; gesture = null;
      panel.classList.remove('is-interacting');
      if (panel.hasPointerCapture(id)) panel.releasePointerCapture(id);
      save();
    }
    panel.addEventListener('pointerdown',start);
    panel.addEventListener('pointermove',move);
    panel.addEventListener('pointerup',end);
    panel.addEventListener('pointercancel',end);
    panel.addEventListener('lostpointercapture',end);
    window.addEventListener('resize',fit);
    return {fit:fit, cancel:function () { end(); }, dispose:function () {
      if (disposed) return;
      end(); disposed = true;
      panel.removeEventListener('pointerdown',start); panel.removeEventListener('pointermove',move);
      panel.removeEventListener('pointerup',end); panel.removeEventListener('pointercancel',end);
      panel.removeEventListener('lostpointercapture',end); window.removeEventListener('resize',fit);
      handles.forEach(function (handle) { handle.remove(); }); header.classList.remove('pbg-window-titlebar');
    }};
  }
  window.PBGuiFloatingWindow = {attach:attach};
}());
