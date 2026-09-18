/* Overwrite digits in complete ISO dates without disturbing selection or paste. */
(function () {
  'use strict';
  var overwritten = new WeakSet();
  function attribute(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, function (c) {
      return {'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c];
    });
  }
  window.PBGuiDateInput = {
    render: function (id, value, options) {
      options = options || {};
      var date = attribute(value || '');
      return '<div style="position:relative;min-width:0;flex:1">' +
        '<input type="text" data-date-overwrite="true" id="' + attribute(id) +
        '" value="' + date + '" data-prev="' + date + '" class="' + attribute(options.className || '') +
        '"' + (options.clearSemantic ? ' data-date-clear-semantic="true"' : '') +
        ' style="width:100%;box-sizing:border-box;padding-right:26px">' +
        '<button type="button" data-date-calendar="true" data-dp="' + attribute(id) +
        '" style="position:absolute;right:2px;top:50%;transform:translateY(-50%);background:transparent;border:none;padding:0 3px;font-size:var(--fs-sm);line-height:1;cursor:pointer" title="Open calendar">📅</button></div>';
    }
  };
  document.addEventListener('change', function (event) {
    var input = event.target;
    if (!input.matches || !input.matches('input[data-date-overwrite]')) return;
    overwritten.delete(input);
    input.dataset.prev = input.value;
    if (input.hasAttribute('data-date-clear-semantic')) input.dataset.semanticValue = '';
  });
  document.addEventListener('focusout', function (event) {
    var input = event.target;
    if (!overwritten.has(input)) return;
    overwritten.delete(input);
    input.dispatchEvent(new Event('change', {bubbles:true}));
  });
  document.addEventListener('click', function (event) {
    var button = event.target.closest && event.target.closest('button[data-date-calendar]');
    if (button && window.__dp) window.__dp.show(button.dataset.dp, button);
  });
  document.addEventListener('beforeinput', function (event) {
    var input = event.target;
    if (!input.matches || !input.matches('input[data-date-overwrite]') ||
        event.inputType !== 'insertText' || event.isComposing ||
        !/^\d$/.test(event.data || '') ||
        !/^\d{4}-\d{2}-\d{2}$/.test(input.value) ||
        input.selectionStart !== input.selectionEnd) return;
    var position = input.selectionStart;
    if (position === 4 || position === 7) position++;
    event.preventDefault();
    if (position >= 10) return;
    input.setRangeText(event.data, position, position + 1, 'end');
    overwritten.add(input);
    var next = position + 1;
    if (next === 4 || next === 7) next++;
    input.setSelectionRange(next, next);
    input.dispatchEvent(new Event('input', {bubbles: true}));
  });
}());
