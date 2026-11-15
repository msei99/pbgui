;(function () {
  'use strict';

  function createV9() {
    return {
      version: 'v9',
      isV8: false,
      isV9: true,
      label: 'PB9',
      runtimeLabel: 'PB9',
      navSubtitle: 'PBv9 RUN',
      navCurrent: 'v9_run',
      websocketPath: '/api/v9/ws/v9',
      supportsBackups: true,
      supportsForcedModes: true,
      supportsConversion: false,
      supportsRollback: true,
      supportsBalanceCalculator: false,
      showsStrategy: true,
      configureUi: function () {
        document.title = 'PBv9 Run';
        document.querySelectorAll('[data-v7-only], [data-v8-only]').forEach(function(element) {
          element.hidden = true;
        });
        var addButton = document.getElementById('add-instance-btn');
        if (addButton) addButton.textContent = '➕ Add PB9 Instance';
      }
    };
  }

  function create(version) {
    if (String(version || '').toLowerCase() === 'v9') return createV9();
    var isV8 = String(version || '').toLowerCase() === 'v8';
    return {
      version: isV8 ? 'v8' : 'v7',
      isV8: isV8,
      isV9: false,
      label: isV8 ? 'PB8' : 'PB7',
      navSubtitle: isV8 ? 'PBv8 RUN' : 'PBv7 RUN',
      navCurrent: isV8 ? 'v8_run' : 'v7_run',
      websocketPath: isV8 ? '/api/v8/ws/v8' : '/api/v7/ws/v7',
      supportsBackups: true,
      supportsForcedModes: true,
      supportsConversion: !isV8,
      supportsRollback: isV8,
      supportsBalanceCalculator: true,
      showsStrategy: isV8,
      configureUi: function () {
        document.title = (isV8 ? 'PBv8' : 'PBv7') + ' Run';
        document.querySelectorAll(isV8 ? '[data-v7-only]' : '[data-v8-only]').forEach(function(element) {
          element.hidden = true;
        });
        var addButton = document.getElementById('add-instance-btn');
        if (addButton) addButton.textContent = '➕ Add ' + (isV8 ? 'PB8 Instance' : 'Instance');
      }
    };
  }

  window.PBGuiRunListAdapter = { create: create };
}());
