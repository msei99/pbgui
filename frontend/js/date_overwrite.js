/* Overwrite digits in complete ISO dates without disturbing selection or paste. */
(function () {
  'use strict';
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
    var next = position + 1;
    if (next === 4 || next === 7) next++;
    input.setSelectionRange(next, next);
    input.dispatchEvent(new Event('input', {bubbles: true}));
  });
}());
