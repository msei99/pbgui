/* Retained Vast benchmark browsing; shared sidebar controls and local SVG charts. */
(function (global) {
  'use strict';
  function create({request}) {
    const el = id => document.getElementById(id);
    const events = new AbortController();
    let rows = [], selected = new Map(), offset = 0, total = 0, fingerprint = null;
    let active = false, disposed = false, generation = 0, drag = null;
    const colors = ['#4da6ff', '#5eead4', '#fbbf24', '#c084fc'];
    const number = value => Number.isFinite(value) ? value.toLocaleString(undefined, {maximumFractionDigits: 1}) : '—';
    const money = value => Number.isFinite(value) ? value.toFixed(4) : '—';
    const text = (tag, value, parent, className) => {
      const node = document.createElement(tag); node.textContent = value;
      if (className) node.className = className;
      if (parent) parent.appendChild(node);
      return node;
    };
    const on = (id, event, callback) => el(id).addEventListener(event, callback, {signal:events.signal});
    const status = value => { el('performance-status').textContent = value; };
    const comparable = () => selected.size >= 2 && [...selected.values()].every(row =>
      row.fingerprint && row.fingerprint === selected.values().next().value.fingerprint);
    function updateSelection() {
      el('performance-rows').querySelectorAll('tr[data-run]').forEach(row => {
        row.classList.toggle('selected', selected.has(row.dataset.run));
        row.setAttribute('aria-selected', String(selected.has(row.dataset.run)));
      });
      el('performance-compare').disabled = !comparable();
      el('performance-view').disabled = selected.size !== 1;
      el('performance-same').disabled = !selected.size || !selected.values().next().value.fingerprint;
      el('performance-selection').textContent = selected.size + ' selected' +
        (selected.size > 1 && !comparable() ? ' · comparison requires identical verified workload fingerprints' : '');
    }
    function visibleRows() {
      const query = el('performance-filter').value.toLowerCase().trim();
      return rows.filter(row => !query || [row.config_name, row.hardware?.gpu_name, row.hardware?.machine_id,
        ...(row.workload?.exchanges || []), ...(row.workload?.coins || [])].join(' ').toLowerCase().includes(query));
    }
    function selectRange(end) {
      if (!drag) return;
      selected = new Map(drag.base);
      const visible = visibleRows();
      visible.slice(Math.min(drag.anchor, end), Math.max(drag.anchor, end) + 1).forEach(row => {
        if (!drag.add) selected.delete(row.id);
        else if (selected.size < 4) selected.set(row.id, row);
      });
      generation++; updateSelection();
    }
    function renderRows() {
      const body = el('performance-rows'); body.replaceChildren();
      visibleRows().forEach((row, index) => {
        const tr = document.createElement('tr'); tr.dataset.run = row.id; tr.tabIndex = 0;
        const w = row.workload || {}, h = row.hardware || {}, s = row.summary || {};
        const date = row.created_at ? new Date(row.created_at * 1000).toLocaleString() : 'Unknown date';
        const values = [String(row.config_name || row.id) + ' · ' + (row.status || 'unknown') + ' · ' + date,
          (h.gpu_name || 'Unknown GPU') + ' / ' + (h.machine_id ?? '—'),
          row.cpu_allocation_resolved ? number(row.workers) : 'Unverified', number(w.coin_count),
          (w.exchanges || []).join(', ') || '—', number(w.scenario_count), number(w.exported_candles),
          number(s.proxy_per_minute), number(s.exact_per_minute), number(s.proxy_rate_min) + ' – ' + number(s.proxy_rate_max),
          number(row.startup_to_first_counter_seconds), number(s.exact_per_usd), money(row.cost_estimate?.total_usd), number(row.comparable_host_runs),
          row.fingerprint ? row.fingerprint.slice(0, 12) : 'Unverified'];
        values.forEach((value, column) => {
          const cell = text('td', column ? value : '', tr);
          if (!column) { text('strong', row.config_name || row.id, cell); text('div', (row.status || 'unknown') + ' · ' + date, cell, 'muted'); }
        });
        tr.title = w.reason || 'Select runs to compare identical frozen workloads';
        tr.addEventListener('pointerdown', event => {
          if (event.button !== 0) return;
          event.preventDefault(); tr.focus();
          drag = {anchor:index, base:new Map(selected), add:!selected.has(row.id)}; selectRange(index);
        });
        tr.addEventListener('pointerenter', event => { if (event.buttons === 1) selectRange(index); });
        tr.addEventListener('keydown', event => {
          if (![' ', 'Enter'].includes(event.key)) return;
          event.preventDefault();
          if (selected.has(row.id)) selected.delete(row.id);
          else if (selected.size < 4) selected.set(row.id, row);
          generation++; updateSelection();
        });
        body.appendChild(tr);
      });
      if (!body.children.length) {
        const row = document.createElement('tr'); const cell = text('td', 'No recorded runs match this view.', row);
        cell.colSpan = 15; body.appendChild(row);
      }
      el('performance-count').textContent = total + ' retained runs' + (fingerprint ? ' · identical workload' : '');
      el('performance-prev').disabled = offset === 0;
      el('performance-next').disabled = offset + 100 >= total;
      updateSelection();
    }
    async function refresh() {
      const current = ++generation;
      status('Loading performance history…');
      try {
        const data = await request('/performance?limit=100&offset=' + offset + (fingerprint ? '&fingerprint=' + encodeURIComponent(fingerprint) : ''));
        if (disposed || !active || current !== generation) return;
        rows = data.runs || []; total = data.total || 0;
        for (const row of rows) if (selected.has(row.id)) selected.set(row.id, row);
        renderRows(); status('History is sampled approximately every 60 seconds. Select runs or refresh for new measurements.');
      } catch (error) { if (!disposed && active && current === generation) status(error.message); }
    }
    function drawChart(hostId, runs, key, title) {
      const host = el(hostId); host.replaceChildren(); text('h3', title, host);
      const legend = text('div', '', host, 'performance-legend');
      const series = runs.map((run, index) => {
        const samples = run.series?.counter || [];
        const base = samples[0]?.sampled_at; const points = [];
        for (let i = 1; i < samples.length; i++) {
          const a = samples[i-1], b = samples[i], dt = b.sampled_at - a.sampled_at;
          const dp = b.proxy_total - a.proxy_total, de = b.exact_total - a.exact_total;
          points.push(dt >= 10 && dp >= 0 && de >= 0 ? {
            x:(b.sampled_at-base)/60, y:60 * (key === 'proxy_total' ? dp : de)/dt, seconds:dt
          } : null);
        }
        const label = (run.hardware?.gpu_name || 'Unknown GPU') + ' · ' + (run.hardware?.machine_id ?? '—') + ' · ' + run.id.slice(0, 8);
        const item = text('button', label, legend); item.type = 'button'; item.style.color = colors[index];
        item.setAttribute('aria-pressed', 'true'); item.title = 'Show or hide this run in the chart';
        return {points, color:colors[index], label, toggle:item};
      });
      const all = series.flatMap(item => item.points.filter(Boolean));
      if (!all.length) { text('p', 'At least two counter samples ten seconds apart are required.', host, 'muted'); return; }
      const maxX = Math.max(1, ...all.map(p => p.x)), maxY = Math.max(1, ...all.map(p => p.y));
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('viewBox', '0 0 960 280'); svg.setAttribute('role', 'img'); svg.setAttribute('aria-label', title);
      host.appendChild(svg);
      const shape = (tag, attributes, label) => {
        const node = document.createElementNS(svg.namespaceURI, tag);
        Object.entries(attributes).forEach(([name, value]) => node.setAttribute(name, String(value)));
        if (label != null) node.textContent = label;
        svg.appendChild(node); return node;
      };
      const x = value => 90 + 840 * value / maxX, y = value => 235 - 210 * value / maxY;
      for (let i=0; i<=4; i++) {
        shape('line', {x1:90, x2:930, y1:y(maxY*i/4), y2:y(maxY*i/4), stroke:'var(--border)'});
        shape('text', {x:82, y:y(maxY*i/4)+4, fill:'var(--text-dim)', 'text-anchor':'end', 'font-size':12}, number(maxY*i/4));
        shape('text', {x:x(maxX*i/4), y:255, fill:'var(--text-dim)', 'text-anchor':'middle', 'font-size':12}, number(maxX*i/4));
      }
      shape('text', {x:510, y:276, fill:'var(--text-dim)', 'text-anchor':'middle', 'font-size':12}, 'Minutes since first recorded counter');
      series.forEach(item => {
        const before = new Set(svg.children);
        let segment = [];
        const flush = () => {
          if (segment.length) shape('polyline', {points:segment.join(' '), fill:'none', stroke:item.color, 'stroke-width':2});
          segment = [];
        };
        item.points.forEach(point => {
          if (!point) { flush(); return; }
          segment.push(x(point.x) + ',' + y(point.y));
          const dot = shape('circle', {cx:x(point.x), cy:y(point.y), r:3, fill:item.color});
          const hint = document.createElementNS(svg.namespaceURI, 'title');
          hint.textContent = item.label + ': ' + number(point.y) + '/min at ' + number(point.x) + ' min; interval ' + number(point.seconds) + 's';
          dot.appendChild(hint);
        }); flush();
        const shapes = [...svg.children].filter(node => !before.has(node));
        item.toggle.addEventListener('click', () => {
          const shown = item.toggle.getAttribute('aria-pressed') !== 'true';
          item.toggle.setAttribute('aria-pressed', String(shown));
          item.toggle.style.opacity = shown ? '1' : '.45';
          shapes.forEach(node => { node.style.display = shown ? '' : 'none'; });
        });
      });
    }
    async function compare(inspect = false) {
      if (inspect ? selected.size !== 1 : !comparable()) return;
      const current = ++generation;
      status('Loading comparison…');
      try {
        const data = await request('/performance/compare', {method:'POST', body:JSON.stringify({ids:[...selected.keys()]})});
        if (disposed || !active || current !== generation) return;
        const details = el('performance-details'); details.replaceChildren();
        for (const run of data.runs) {
          const card = text('div', '', details, 'optlog-card'), w = run.workload || {}, h = run.hardware || {}, s = run.summary || {};
          text('strong', (h.gpu_name || 'Unknown GPU') + ' · machine ' + (h.machine_id ?? '—'), card);
          [number(run.workers) + ' CPU workers · ' + number(h.ram_gb) + ' GB RAM · ' + number(h.vram_gb) + ' GB VRAM',
            number(s.proxy_per_minute) + ' proxy/min · ' + number(s.exact_per_minute) + ' exact/min',
            'Covered: ' + number(s.covered_seconds) + 's · ' + number(s.samples) + ' counter samples',
            'Exact/min range: ' + number(s.exact_rate_min) + ' – ' + number(s.exact_rate_max),
            'Cost estimate: $' + money(run.cost_estimate?.compute_usd) + ' compute + $' + money(run.cost_estimate?.transfer_usd) + ' transfer',
            'Coins: ' + (w.coins || []).join(', ') + ' · exchanges: ' + (w.exchanges || []).join(', '),
            'Scenarios: ' + number(w.scenario_count) + ' · timeframe: ' + (w.start_date || '—') + ' → ' + (w.end_date || '—'),
            'Dataset: ' + number(w.exported_candles) + ' exported candles · ' + number(w.exported_symbols) + ' symbol/exchange datasets · ' + (w.resolutions || []).join(', '),
            'Parameters: ' + number(w.parameter_count) + ' · population: ' + number(w.population_size) + ' · seed: ' + (w.seed ?? '—'),
            'PB8: ' + (w.revision || '—') + ' · outcome: ' + (run.status || '—') + ' / ' + (run.stop_reason || '—')
          ].forEach(value => text('p', value, card, 'muted'));
        }
        drawChart('performance-proxy-chart', data.runs, 'proxy_total', 'Proxy evaluations / minute');
        drawChart('performance-exact-chart', data.runs, 'exact_total', 'Exact evaluations / minute');
        el('performance-browser').hidden = true; el('performance-comparison').hidden = false;
        el('performance-back').hidden = false; status((data.runs.length > 1 ? 'Identical frozen workload · ' : 'Workload · ') + (data.runs[0].fingerprint || 'Unverified — comparison unavailable'));
      } catch (error) { if (!disposed && active && current === generation) status(error.message); }
    }
    function browse() {
      generation++; el('performance-browser').hidden = false; el('performance-comparison').hidden = true;
      el('performance-back').hidden = true;
    }
    on('performance-refresh', 'click', () => { browse(); refresh(); });
    on('performance-filter', 'input', renderRows);
    on('performance-prev', 'click', () => { offset = Math.max(0, offset-100); refresh(); });
    on('performance-next', 'click', () => { offset += 100; refresh(); });
    on('performance-compare', 'click', () => compare());
    on('performance-view', 'click', () => compare(true));
    on('performance-back', 'click', () => { browse(); refresh(); });
    on('performance-same', 'click', () => {
      fingerprint = selected.values().next().value?.fingerprint || null; offset = 0; browse(); refresh();
    });
    on('performance-clear', 'click', () => {
      selected.clear(); fingerprint = null; offset = 0; el('performance-filter').value = ''; browse(); refresh();
    });
    document.addEventListener('pointerup', () => { drag = null; }, {signal:events.signal});
    return {
      show() { active = true; browse(); refresh(); },
      hide() { active = false; generation++; drag = null; },
      dispose() { disposed = true; active = false; generation++; events.abort(); selected.clear(); }
    };
  }
  global.PBGuiVastPerformance = {create};
}(window));
