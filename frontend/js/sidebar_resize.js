(function(global) {
  function init(options) {
    var opts = options || {};
    var sidebarId = opts.sidebarId || 'sidebar';
    var handleId = opts.handleId || 'sidebar-resize';
    var minWidth = Number(opts.minWidth);
    var maxWidth = Number(opts.maxWidth);
    var sidebar = document.getElementById(sidebarId);
    var handle = document.getElementById(handleId);
    var active = false;
    var nextWidth;

    if (!sidebar || !handle) return;
    if (handle.dataset.sidebarResizeBound === 'true') return;

    if (!isFinite(minWidth)) minWidth = 140;
    if (!isFinite(maxWidth)) maxWidth = 300;

    handle.dataset.sidebarResizeBound = 'true';
    handle.addEventListener('mousedown', function(event) {
      event.preventDefault();
      active = true;
      handle.classList.add('active');
    });
    document.addEventListener('mousemove', function(event) {
      if (!active) return;
      nextWidth = event.clientX;
      if (nextWidth < minWidth) nextWidth = minWidth;
      if (nextWidth > maxWidth) nextWidth = maxWidth;
      sidebar.style.width = nextWidth + 'px';
    });
    document.addEventListener('mouseup', function() {
      if (!active) return;
      active = false;
      handle.classList.remove('active');
    });
  }

  global.PBGuiSidebarResize = {
    init: init
  };
}(window));