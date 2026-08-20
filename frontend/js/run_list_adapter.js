;(function () {
  'use strict';

  function create(version) {
    var isV8 = String(version || '').toLowerCase() === 'v8';
    return {
      version: isV8 ? 'v8' : 'v7',
      isV8: isV8,
      label: isV8 ? 'PB8' : 'PB7',
      navSubtitle: isV8 ? 'PBv8 RUN' : 'PBv7 RUN',
      navCurrent: isV8 ? 'v8_run' : 'v7_run',
      websocketPath: isV8 ? '/api/v8/ws/v8' : '/api/v7/ws/v7',
      supportsBackups: true,
      supportsForcedModes: true,
      supportsConversion: !isV8,
      configureUi: function () {
        document.title = (isV8 ? 'PBv8' : 'PBv7') + ' Run';
        document.querySelectorAll(isV8 ? '[data-v7-only]' : '[data-v8-only]').forEach(function(element) {
          element.hidden = true;
        });
        var addButton = document.getElementById('add-instance-btn');
        if (addButton) addButton.textContent = '\u2795 Add ' + (isV8 ? 'PB8 Instance' : 'Instance');
      }
    };
  }

  window.PBGuiRunListAdapter = { create: create };
}());
