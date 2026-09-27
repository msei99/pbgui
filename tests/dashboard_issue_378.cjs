/* Offline chart regressions for dashboard issue #378. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'frontend', 'dashboard_render.js'), 'utf8');
const window = {};
vm.runInNewContext(source, {window, document: {}, setTimeout, clearTimeout});
const render = window.DashRender;
let chart;
const plotly = {
    react(_div, traces, layout) { chart = {traces, layout}; },
    Plots: {resize() {}},
};
const chartDiv = {
    closest() { return null; },
    addEventListener() {},
    removeEventListener() {},
};

render.renderPnl(chartDiv, {
    mode: 'Cumulative',
    bars: [
        {date: '2026-09-01', income: 10, cum: 10},
        {date: '2026-09-02', income: -3, cum: 7},
    ],
}, {Plotly: plotly, noResize: true});
assert.equal(chart.traces[0].type, 'scatter');
assert.deepEqual(Array.from(chart.traces[0].y), [10, 7]);
assert.equal(chart.layout.xaxis.automargin, true);
assert.equal(chart.layout.yaxis.automargin, true);

render.renderPnl(chartDiv, {
    mode: 'Daily',
    bars: [{date: '2026-09-01', income: 10, cum: 10}],
}, {Plotly: plotly, noResize: true});
assert.equal(chart.traces[0].type, 'bar');
assert.deepEqual(Array.from(chart.traces[0].y), [10]);

render.renderPpl(chartDiv, {bars: []}, {Plotly: plotly, noResize: true});
assert.ok(chart.layout.yaxis.range.every(Number.isFinite));
assert.equal(chart.layout.xaxis.automargin, true);
assert.equal(chart.layout.yaxis.automargin, true);
assert.ok(chart.layout.margin.b >= 65);
assert.equal(render.tweBarPct(-40), '0.0');
