/* Plot only the native PB8 HSL observations and controller transitions. */
(function () {
  'use strict';
  function traces(report) {
    if (!report || report.schema_version !== 1 || report.engine !== 'hsl'
        || ['coin', 'pside', 'unified'].indexOf(report.mode) < 0) throw new Error('Unsupported native HSL report schema.');
    if (!report.detailed || !Array.isArray(report.samples) || !report.samples.length) return [];
    var result = [];
    (report.scopes || []).forEach(function (scope) {
      var rows = report.samples.filter(function (row) { return row.side === scope.side && row.coin === scope.coin; });
      if (!rows.length) return;
      var name = report.mode === 'unified' ? 'Portfolio' : (scope.side === 0 ? 'Long' : 'Short');
      if (report.mode === 'coin') name = String((report.coins || [])[scope.coin]) + ' ' + name;
      var x = rows.map(function (row) { return new Date(row.timestamp).toISOString(); });
      ['raw', 'ema'].forEach(function (key) {
        result.push({x: x, y: rows.map(function (row) { return row[key]; }), name: name + ' ' + key,
          mode: 'lines', connectgaps: false, type: 'scattergl'});
      });
      result.push({x: [x[0], x[x.length - 1]], y: [scope.policy.red_threshold, scope.policy.red_threshold],
        name: name + ' RED threshold', mode: 'lines', line: {dash: 'dash'}});
      var transitions = rows.map(function (row) {
        if ([null, 'normal', 'panic', 'halted'].indexOf(row.action) < 0) throw new Error('Unsupported native HSL action.');
        return {sequence: row.sequence, timestamp: row.timestamp,
          state: row.action === null ? null : row.action === 'normal' ? 0 : 1};
      });
      (report.events || []).filter(function (event) {
        return event.side === scope.side && event.coin === scope.coin;
      }).forEach(function (event) {
        if (['red', 'flat', 'restart'].indexOf(event.kind) < 0) throw new Error('Unsupported native HSL event.');
        transitions.push({sequence: event.sequence, timestamp: event.observed_at, state: event.kind === 'restart' ? 0 : 1});
      });
      transitions.sort(function (a, b) { return a.sequence - b.sequence; });
      result.push({x: transitions.map(function (row) { return new Date(row.timestamp).toISOString(); }),
        y: transitions.map(function (row) { return row.state; }), name: name + ' controller',
        mode: 'lines', yaxis: 'y2', line: {shape: 'hv'}, connectgaps: false});
    });
    return result;
  }
  function render(container, report) {
    var data = traces(report);
    if (!data.length) {
      container.textContent = 'Native HSL samples are unavailable. Enable backtest.hsl_detailed_report and re-backtest (metrics-only runs have no samples).';
      return;
    }
    window.Plotly.newPlot(container, data, {paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
      font: {color: '#cbd5e1'}, margin: {l: 65, r: 65, t: 40, b: 45},
      title: 'Native HSL observations (' + report.mode + ')', xaxis: {title: 'Time'},
      yaxis: {title: 'Drawdown', domain: [0.3, 1]},
      yaxis2: {title: 'Controller', domain: [0, 0.2], tickvals: [0, 1], ticktext: ['GREEN', 'RED']},
      legend: {orientation: 'h'}}, {responsive: true});
  }
  window.PB8HslReport = {traces: traces, render: render};
}());
