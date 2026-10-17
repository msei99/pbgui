/* Shared renderer for PBGui's existing data-tip label/field tooltip convention. */
(function () {
  'use strict';
  var installed = false;
  function install() {
    if (installed) return;
    installed = true;
    var tooltip = document.createElement('div');
    tooltip.id = 'data-tip-tooltip'; tooltip.hidden = true; tooltip.setAttribute('role', 'tooltip');
    document.body.appendChild(tooltip);
    var active = null, previousDescription = null, hideTimer = null;
    function hide() {
      clearTimeout(hideTimer); tooltip.hidden = true; tooltip.textContent = '';
      if (active && active.getAttribute('aria-describedby') === tooltip.id) {
        if (previousDescription === null) active.removeAttribute('aria-describedby');
        else active.setAttribute('aria-describedby', previousDescription);
      }
      active = null; previousDescription = null;
    }
    function position(event) {
      if (tooltip.hidden || !active) return;
      if (event && tooltip.contains(event.target)) return;
      if (!active.isConnected) { hide(); return; }
      var rect = active.getBoundingClientRect();
      var x = event && event.clientX != null ? event.clientX : rect.left;
      var y = event && event.clientY != null ? event.clientY : rect.bottom;
      tooltip.style.left = Math.max(8, Math.min(x + 14, innerWidth - tooltip.offsetWidth - 8)) + 'px';
      tooltip.style.top = Math.max(8, Math.min(y + 14, innerHeight - tooltip.offsetHeight - 8)) + 'px';
    }
    function show(event) {
      if (!event.target.closest || tooltip.contains(event.target)) return;
      var target = event.target.closest('[data-tip]');
      if (!target || !target.getAttribute('data-tip')) return;
      if (active !== target) { hide(); active = target; previousDescription = target.getAttribute('aria-describedby'); }
      clearTimeout(hideTimer);
      tooltip.textContent = target.getAttribute('data-tip');
      tooltip.style.fontFamily = target.getAttribute('data-tip-mono') === 'true' ? 'var(--mono, monospace)' : '';
      target.setAttribute('aria-describedby', tooltip.id); tooltip.hidden = false; position(event);
    }
    function leave(event) {
      var next = event.relatedTarget;
      if (next && (tooltip.contains(next) || active && active.contains(next))) return;
      clearTimeout(hideTimer); hideTimer = setTimeout(hide, 150);
    }
    function escape(event) { if (event.key === 'Escape') hide(); }
    function hold() { clearTimeout(hideTimer); }
    function visibility() { if (document.hidden) hide(); }
    function scroll(event) { if (!tooltip.contains(event.target)) hide(); }
    tooltip.addEventListener('mouseenter', hold); tooltip.addEventListener('mouseleave', leave);
    document.addEventListener('mouseover', show); document.addEventListener('focusin', show);
    document.addEventListener('mouseout', leave); document.addEventListener('focusout', leave);
    document.addEventListener('mousemove', position); document.addEventListener('keydown', escape);
    document.addEventListener('click', show); document.addEventListener('visibilitychange', visibility);
    window.addEventListener('resize', hide); window.addEventListener('scroll', scroll, true);
    window.addEventListener('pagehide', function () {
      hide(); tooltip.remove(); installed = false;
      document.removeEventListener('mouseover', show); document.removeEventListener('focusin', show);
      document.removeEventListener('mouseout', leave); document.removeEventListener('focusout', leave);
      document.removeEventListener('mousemove', position); document.removeEventListener('keydown', escape);
      document.removeEventListener('click', show); document.removeEventListener('visibilitychange', visibility);
      window.removeEventListener('resize', hide); window.removeEventListener('scroll', scroll, true);
    }, {once: true});
  }
  install();
  window.addEventListener('pageshow', function (event) { if (event.persisted) install(); });
})();
