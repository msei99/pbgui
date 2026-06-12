/* Shared PBGui date picker for YYYY-MM-DD text inputs. */
(function () {
  'use strict';

  if (window.PBGuiDatePicker) {
    if (!window.__dp) window.__dp = window.PBGuiDatePicker;
    return;
  }

  var MN = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var inputId = null;
  var panel = null;
  var year = 0;
  var month = 0;
  var menu = '';

  function ensureCss() {
    if (document.getElementById('pbgui-date-picker-css')) return;
    var style = document.createElement('style');
    style.id = 'pbgui-date-picker-css';
    style.textContent = [
      '.date-input-wrap{position:relative;}',
      '.date-input-wrap input{padding-right:28px;}',
      '.calendar-trigger{position:absolute;right:2px;top:50%;transform:translateY(-50%);background:transparent;border:0;color:var(--text,#e2e8f0);padding:0 3px;font-size:var(--fs-sm,13px);line-height:1;cursor:pointer;}',
      '.pbgui-dp{position:fixed;z-index:12000;background:var(--bg2,#131b2b);border:1px solid var(--line2,#2d3748);border-radius:8px;padding:8px;box-shadow:0 18px 50px rgba(0,0,0,.65);width:232px;color:var(--text,#e2e8f0);font:var(--fs-sm,13px)/1.35 inherit;}',
      '.pbgui-dp-row{display:grid;grid-template-columns:28px 1fr 74px 28px;gap:4px;align-items:center;margin-bottom:6px;}',
      '.pbgui-dp button{border:1px solid var(--line2,#2d3748);background:#111827;color:var(--text,#e2e8f0);border-radius:5px;cursor:pointer;font:inherit;min-height:24px;}',
      '.pbgui-dp button:hover{border-color:var(--accent,#63b3ed);}',
      '.pbgui-dp-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:3px;text-align:center;}',
      '.pbgui-dp-dow{color:var(--muted,#94a3b8);font-size:var(--fs-xs,11px);padding:3px 0;}',
      '.pbgui-dp-day{border-radius:4px;padding:4px 0;cursor:pointer;}',
      '.pbgui-dp-day:hover{background:rgba(99,179,237,.16);}',
      '.pbgui-dp-day.today{color:var(--accent,#63b3ed);font-weight:700;}',
      '.pbgui-dp-day.selected{background:rgba(99,179,237,.24);font-weight:700;}',
      '.pbgui-dp-menu{position:absolute;background:var(--bg2,#131b2b);border:1px solid var(--line2,#2d3748);border-radius:6px;box-shadow:0 10px 28px rgba(0,0,0,.55);max-height:190px;overflow:auto;display:none;padding:4px;}',
      '.pbgui-dp-menu.open{display:grid;gap:3px;}',
      '.pbgui-dp-menu button.selected{border-color:var(--accent,#63b3ed);}',
      '.pbgui-dp-foot{display:flex;justify-content:space-between;margin-top:7px;}',
      '.pbgui-dp-foot button{background:transparent;border:0;color:var(--muted,#94a3b8);padding:2px 6px;min-height:20px;}'
    ].join('');
    document.head.appendChild(style);
  }

  ensureCss();

  function pad(n) { return String(n).padStart(2, '0'); }
  function fmt(y, m, d) { return y + '-' + pad(m + 1) + '-' + pad(d); }
  function parse(value) {
    var m = String(value || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;
    var d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
    if (d.getFullYear() !== Number(m[1]) || d.getMonth() !== Number(m[2]) - 1 || d.getDate() !== Number(m[3])) return null;
    return d;
  }
  function currentInput() { return inputId ? document.getElementById(inputId) : null; }

  function position(anchor) {
    if (!panel || !anchor) return;
    var rect = anchor.getBoundingClientRect();
    var left = Math.min(window.innerWidth - 244, Math.max(8, rect.left));
    var top = rect.bottom + 6;
    if (top + 280 > window.innerHeight) top = Math.max(8, rect.top - 280);
    panel.style.left = left + 'px';
    panel.style.top = top + 'px';
  }

  function monthMenu() {
    var html = '';
    for (var i = 0; i < 12; i++) html += '<button type="button" class="' + (i === month ? 'selected' : '') + '" onclick="window.__dp.sm(' + i + ')">' + MN[i] + '</button>';
    return html;
  }

  function yearMenu() {
    var now = new Date().getFullYear();
    var start = Math.min(year - 6, now - 12);
    var end = Math.max(year + 6, now + 2);
    var html = '';
    for (var y = start; y <= end; y++) html += '<button type="button" class="' + (y === year ? 'selected' : '') + '" onclick="window.__dp.sy(' + y + ')">' + y + '</button>';
    return html;
  }

  function render() {
    if (!panel) return;
    var selected = parse((currentInput() || {}).value || '');
    var today = new Date();
    var first = new Date(year, month, 1);
    var days = new Date(year, month + 1, 0).getDate();
    var blanks = (first.getDay() + 6) % 7;
    var html = '<div class="pbgui-dp-row">'
      + '<button type="button" onclick="window.__dp.pm()">&#8249;</button>'
      + '<div style="position:relative"><button type="button" onclick="window.__dp.tm()" style="width:100%">' + MN[month] + ' <span style="font-size:10px;opacity:.8">▾</span></button><div class="pbgui-dp-menu ' + (menu === 'month' ? 'open' : '') + '" style="left:0;min-width:112px">' + monthMenu() + '</div></div>'
      + '<div style="position:relative"><button type="button" onclick="window.__dp.ty()" style="width:100%">' + year + ' <span style="font-size:10px;opacity:.8">▾</span></button><div class="pbgui-dp-menu ' + (menu === 'year' ? 'open' : '') + '" style="right:0;min-width:74px">' + yearMenu() + '</div></div>'
      + '<button type="button" onclick="window.__dp.nm()">&#8250;</button></div>';
    html += '<div class="pbgui-dp-grid"><div class="pbgui-dp-dow">Mo</div><div class="pbgui-dp-dow">Tu</div><div class="pbgui-dp-dow">We</div><div class="pbgui-dp-dow">Th</div><div class="pbgui-dp-dow">Fr</div><div class="pbgui-dp-dow">Sa</div><div class="pbgui-dp-dow">Su</div>';
    for (var b = 0; b < blanks; b++) html += '<div></div>';
    for (var day = 1; day <= days; day++) {
      var isToday = today.getFullYear() === year && today.getMonth() === month && today.getDate() === day;
      var isSelected = selected && selected.getFullYear() === year && selected.getMonth() === month && selected.getDate() === day;
      html += '<div class="pbgui-dp-day' + (isToday ? ' today' : '') + (isSelected ? ' selected' : '') + '" onclick="window.__dp.pick(' + day + ')">' + day + '</div>';
    }
    html += '</div><div class="pbgui-dp-foot"><button type="button" onclick="window.__dp.hide()">Close</button><button type="button" onclick="window.__dp.today()">Today</button></div>';
    panel.innerHTML = html;
  }

  function show(id, anchor) {
    ensureCss();
    inputId = id;
    var input = currentInput();
    var d = parse(input ? input.value : '') || new Date();
    year = d.getFullYear();
    month = d.getMonth();
    menu = '';
    if (!panel) {
      panel = document.createElement('div');
      panel.className = 'pbgui-dp';
      document.body.appendChild(panel);
      document.addEventListener('click', function (event) {
        if (!panel || !panel.style.display || panel.style.display === 'none') return;
        if (panel.contains(event.target)) return;
        if (event.target && event.target.getAttribute && event.target.getAttribute('data-dp')) return;
        hide();
      });
    }
    panel.style.display = 'block';
    render();
    position(anchor || input);
  }

  function hide() { if (panel) panel.style.display = 'none'; }
  function pick(day) {
    var input = currentInput();
    if (input) {
      input.value = fmt(year, month, day);
      input.dataset.prev = input.value;
      input.dispatchEvent(new Event('change', { bubbles: true }));
    }
    hide();
  }

  window.PBGuiDatePicker = window.__dp = {
    show: show,
    hide: hide,
    pm: function () { month--; if (month < 0) { month = 11; year--; } menu = ''; render(); },
    nm: function () { month++; if (month > 11) { month = 0; year++; } menu = ''; render(); },
    sm: function (m) { month = m; menu = ''; render(); },
    sy: function (y) { year = y; menu = ''; render(); },
    tm: function () { menu = menu === 'month' ? '' : 'month'; render(); },
    ty: function () { menu = menu === 'year' ? '' : 'year'; render(); },
    today: function () { var d = new Date(); year = d.getFullYear(); month = d.getMonth(); pick(d.getDate()); },
    pick: pick
  };
}());
