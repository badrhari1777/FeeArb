(function () {
  'use strict';

  var rootPath = window.__ROOT_PATH__ || '';
  var initial = window.__FUNDING_HISTORY_INITIAL__ || {};
  var defaultExchanges = initial.default_exchanges || ['binance', 'kucoin'];
  var supportedExchanges = initial.supported_exchanges || [];
  var windows = initial.windows || [
    { hours: 4, label: '4h' },
    { hours: 12, label: '12h' },
    { hours: 24, label: '1d' },
    { hours: 72, label: '3d' }
  ];
  var state = { lastPayload: null };

  var el = {
    form: document.getElementById('fh-form'),
    symbol: document.getElementById('fh-symbol'),
    points: document.getElementById('fh-points'),
    submit: document.getElementById('fh-submit'),
    status: document.getElementById('fh-status'),
    exchanges: document.getElementById('fh-exchanges'),
    headerSymbol: document.getElementById('fh-header-symbol'),
    headerSelected: document.getElementById('fh-header-selected'),
    headerRun: document.getElementById('fh-header-run'),
    bestBody: document.getElementById('fh-best-body'),
    exchangeBody: document.getElementById('fh-exchange-body'),
    pairBody: document.getElementById('fh-pair-body'),
    timelineHead: document.getElementById('fh-timeline-head'),
    timelineBody: document.getElementById('fh-timeline-body'),
    chart: document.getElementById('fh-rate-chart'),
    chartNote: document.getElementById('fh-chart-note'),
    method: document.getElementById('fh-method')
  };

  function escapeHtml(value) {
    return String(value === undefined || value === null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function fmtNumber(value, digits) {
    if (value === undefined || value === null || isNaN(value)) {
      return '-';
    }
    return Number(value).toFixed(digits);
  }

  function fmtSigned(value, digits, suffix) {
    if (value === undefined || value === null || isNaN(value)) {
      return '-';
    }
    var num = Number(value);
    return (num > 0 ? '+' : '') + num.toFixed(digits) + (suffix || '');
  }

  function fmtPercentFromBps(value) {
    if (value === undefined || value === null || isNaN(value)) {
      return '-';
    }
    return fmtSigned(Number(value) / 100, 4, '%');
  }

  function fmtPct(value, digits) {
    if (value === undefined || value === null || isNaN(value)) {
      return '-';
    }
    return fmtSigned(value, digits || 3, '%');
  }

  function fmtUsd(value) {
    if (value === undefined || value === null || isNaN(value)) {
      return '-';
    }
    return fmtSigned(value, 4, ' USDT');
  }

  function fmtTime(value) {
    if (!value) {
      return '-';
    }
    var date = new Date(value);
    if (isNaN(date.getTime())) {
      date = new Date(Number(value));
    }
    if (isNaN(date.getTime())) {
      return String(value);
    }
    return date.toISOString().replace('T', ' ').replace('.000Z', 'Z');
  }

  function fmtSlotTime(value) {
    if (!value) {
      return '-';
    }
    var date = new Date(value);
    if (isNaN(date.getTime())) {
      date = new Date(Number(value));
    }
    if (isNaN(date.getTime())) {
      return String(value);
    }
    var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    var msk = new Date(date.getTime() + 3 * 60 * 60 * 1000);
    var day = String(msk.getUTCDate()).padStart(2, '0');
    var month = months[msk.getUTCMonth()];
    var year = msk.getUTCFullYear();
    var hour = String(msk.getUTCHours()).padStart(2, '0');
    return day + ' ' + month + ' ' + year + ', ' + hour + ':00 MSK';
  }

  function toneClass(value) {
    var num = Number(value);
    if (isNaN(num)) {
      return '';
    }
    if (num > 0) {
      return 'value-positive';
    }
    if (num < 0) {
      return 'value-negative';
    }
    return '';
  }

  function setStatus(text, kind) {
    if (!el.status) {
      return;
    }
    el.status.textContent = text || '';
    el.status.className = 'settings-status' + (kind ? ' settings-status--' + kind : '');
  }

  function renderExchangeChecks() {
    if (!el.exchanges) {
      return;
    }
    var exchanges = supportedExchanges.length ? supportedExchanges : defaultExchanges;
    el.exchanges.innerHTML = exchanges.map(function (exchange) {
      var checked = defaultExchanges.indexOf(exchange) !== -1 ? ' checked' : '';
      return (
        '<label class="exchange-check">' +
        '<input type="checkbox" name="fh-exchange" value="' + escapeHtml(exchange) + '"' + checked + ' />' +
        '<span>' + escapeHtml(exchange) + '</span>' +
        '</label>'
      );
    }).join('');
  }

  function selectedExchanges() {
    var inputs = document.querySelectorAll('input[name="fh-exchange"]:checked');
    var out = [];
    for (var i = 0; i < inputs.length; i += 1) {
      out.push(inputs[i].value);
    }
    return out;
  }

  function renderBest(payload) {
    var best = payload.best_by_window || {};
    var rows = [];
    var bestWindows = [{ hours: 'next', label: 'next' }].concat(windows);
    for (var i = 0; i < bestWindows.length; i += 1) {
      var label = bestWindows[i].label;
      var row = best[label];
      if (!row) {
        rows.push('<tr><td>' + escapeHtml(label) + '</td><td colspan="6" class="muted">No complete pair data</td></tr>');
        continue;
      }
      rows.push(
        '<tr>' +
        '<td>' + escapeHtml(label) + '</td>' +
        '<td>' + escapeHtml(row.direction_label || '-') + '</td>' +
        '<td class="' + toneClass(row.net_pct) + '">' + fmtPct(row.net_pct, 4) + '</td>' +
        '<td class="' + toneClass(row.usd_per_1000_notional) + '">' + fmtUsd(row.usd_per_1000_notional) + '</td>' +
        '<td class="' + toneClass(row.annualized_pct) + '">' + fmtPct(row.annualized_pct, 2) + '</td>' +
        '<td>' + fmtNumber(row.coverage_pct, 1) + '%' + (row.net_hourly_bps !== undefined ? '<span class="cell-note">' + fmtNumber(row.net_hourly_bps, 3) + ' bps/h</span>' : '') + '</td>' +
        '<td>' + escapeHtml(row.verdict || row.status || '-') + '</td>' +
        '</tr>'
      );
    }
    el.bestBody.innerHTML = rows.join('');
  }

  function renderExchanges(payload) {
    var rows = payload.exchanges || [];
    if (!rows.length) {
      el.exchangeBody.innerHTML = '<tr><td colspan="10" class="muted">No exchange data.</td></tr>';
      return;
    }
    el.exchangeBody.innerHTML = rows.map(function (row) {
      var latest = row.funding_history && row.funding_history.length ? row.funding_history[0] : {};
      var notes = [];
      if (row.error) {
        notes.push(row.error);
      }
      if (row.warnings && row.warnings.length) {
        notes = notes.concat(row.warnings);
      }
      return (
        '<tr>' +
        '<td>' + escapeHtml(row.exchange || '-') + '</td>' +
        '<td>' + escapeHtml(row.exchange_symbol || row.symbol || '-') + '</td>' +
        '<td>' + escapeHtml(row.status || '-') + '</td>' +
        '<td class="' + toneClass(row.latest_funding_bps) + '">' + fmtPercentFromBps(row.latest_funding_bps) + '</td>' +
        '<td class="' + toneClass(row.next_funding_bps) + '">' + fmtPercentFromBps(row.next_funding_bps) + '<span class="cell-note">' + escapeHtml(row.next_funding_source || '-') + '</span></td>' +
        '<td>' + fmtNumber(row.funding_interval_hours_resolved, 2) + 'h<span class="cell-note">' + fmtNumber(row.latest_funding_hourly_bps, 3) + ' bps/h</span></td>' +
        '<td>' + escapeHtml((row.funding_history || []).length) + '</td>' +
        '<td>' + fmtSlotTime(latest.slot_time_utc || latest.time_utc || latest.ts_ms) + '</td>' +
        '<td>' + fmtSlotTime(row.next_funding_time) + '</td>' +
        '<td>' + escapeHtml(notes.join('; ') || '-') + '</td>' +
        '</tr>'
      );
    }).join('');
  }

  function renderPairs(payload) {
    var rows = (payload.next_funding_windows || []).concat(payload.pair_windows || []);
    if (!rows.length) {
      el.pairBody.innerHTML = '<tr><td colspan="9" class="muted">No pair calculations.</td></tr>';
      return;
    }
    el.pairBody.innerHTML = rows.map(function (row) {
      return (
        '<tr>' +
        '<td>' + escapeHtml(row.window_label || '-') + '</td>' +
        '<td>' + escapeHtml(row.direction_label || '-') + '</td>' +
        '<td class="' + toneClass(row.left_leg_bps) + '">' + fmtPercentFromBps(row.left_leg_bps) + '</td>' +
        '<td class="' + toneClass(row.right_leg_bps) + '">' + fmtPercentFromBps(row.right_leg_bps) + '</td>' +
        '<td class="' + toneClass(row.net_pct) + '">' + fmtPct(row.net_pct, 4) + '</td>' +
        '<td class="' + toneClass(row.usd_per_1000_notional) + '">' + fmtUsd(row.usd_per_1000_notional) + '</td>' +
        '<td class="' + toneClass(row.annualized_pct) + '">' + fmtPct(row.annualized_pct, 2) + '</td>' +
        '<td>' + fmtNumber(row.coverage_pct, 1) + '%</td>' +
        '<td>' + escapeHtml(row.status || '-') + (row.net_hourly_bps !== undefined ? '<span class="cell-note">' + fmtNumber(row.net_hourly_bps, 3) + ' bps/h</span>' : '') + '</td>' +
        '</tr>'
      );
    }).join('');
  }

  function renderTimeline(payload) {
    var rows = payload.timeline || [];
    var exchanges = payload.selected_exchanges || selectedExchanges();
    el.timelineHead.innerHTML = '<tr><th>Funding hour</th>' + exchanges.map(function (exchange) {
      return '<th>' + escapeHtml(exchange) + '</th>';
    }).join('') + '</tr>';
    if (!rows.length) {
      el.timelineBody.innerHTML = '<tr><td colspan="' + (exchanges.length + 1) + '" class="muted">No funding events in the selected window.</td></tr>';
      return;
    }
    el.timelineBody.innerHTML = rows.map(function (row) {
      var cells = exchanges.map(function (exchange) {
        var item = (row.exchanges || {})[exchange];
        if (!item) {
          return '<td class="muted">-</td>';
        }
        return '<td class="' + toneClass(item.rate_bps) + '">' + fmtPercentFromBps(item.rate_bps) + '<span class="cell-note">' + fmtNumber(item.interval_hours, 2) + 'h</span></td>';
      }).join('');
      return '<tr><td>' + fmtSlotTime(row.time_utc || row.ts_ms) + '</td>' + cells + '</tr>';
    }).join('');
  }

  function svgLine(points, xMin, xMax, yMin, yMax, width, height) {
    var usableW = width - 70;
    var usableH = height - 46;
    return points.map(function (point, idx) {
      var x = 48 + ((point.ts_ms - xMin) / Math.max(1, xMax - xMin)) * usableW;
      var y = 18 + (1 - ((point.rate_pct - yMin) / Math.max(0.000001, yMax - yMin))) * usableH;
      return (idx === 0 ? 'M' : 'L') + x.toFixed(2) + ' ' + y.toFixed(2);
    }).join(' ');
  }

  function renderChart(payload) {
    if (!el.chart) {
      return;
    }
    var series = ((payload.charts || {}).exchange_rates) || {};
    var names = Object.keys(series);
    var all = [];
    names.forEach(function (name) {
      (series[name] || []).forEach(function (point) {
        if (point && point.ts_ms && point.rate_bps !== null && !isNaN(point.rate_bps)) {
          all.push({ ts_ms: Number(point.ts_ms), rate_pct: Number(point.rate_bps) / 100 });
        }
      });
    });
    if (!all.length) {
      el.chart.innerHTML = '<text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="#8fa2c4">No chart data</text>';
      return;
    }
    var xMin = Math.min.apply(null, all.map(function (p) { return p.ts_ms; }));
    var xMax = Math.max.apply(null, all.map(function (p) { return p.ts_ms; }));
    var yMin = Math.min.apply(null, all.map(function (p) { return p.rate_pct; }));
    var yMax = Math.max.apply(null, all.map(function (p) { return p.rate_pct; }));
    if (yMin === yMax) {
      yMin -= 1;
      yMax += 1;
    }
    var colors = ['#58a6ff', '#2dd4bf', '#f59e0b', '#f87171', '#a78bfa', '#84cc16', '#f472b6', '#e2e8f0'];
    var width = 900;
    var height = 260;
    var grid = [
      '<line x1="48" y1="18" x2="48" y2="214" class="chart-axis" />',
      '<line x1="48" y1="214" x2="870" y2="214" class="chart-axis" />',
      '<text x="10" y="24" class="chart-label">' + escapeHtml(fmtPct(yMax, 4)) + '</text>',
      '<text x="10" y="214" class="chart-label">' + escapeHtml(fmtPct(yMin, 4)) + '</text>',
      '<text x="48" y="242" class="chart-label">' + escapeHtml(fmtTime(xMin).slice(0, 16)) + '</text>',
      '<text x="720" y="242" class="chart-label">' + escapeHtml(fmtTime(xMax).slice(0, 16)) + '</text>'
    ];
    var paths = [];
    var legend = [];
    names.forEach(function (name, idx) {
      var pts = (series[name] || []).filter(function (point) {
        return point && point.ts_ms && point.rate_bps !== null && !isNaN(point.rate_bps);
      }).map(function (point) {
        return { ts_ms: Number(point.ts_ms), rate_pct: Number(point.rate_bps) / 100 };
      });
      if (!pts.length) {
        return;
      }
      var color = colors[idx % colors.length];
      paths.push('<path d="' + svgLine(pts, xMin, xMax, yMin, yMax, width, height) + '" fill="none" stroke="' + color + '" stroke-width="2.2" />');
      legend.push('<span><i style="background:' + color + '"></i>' + escapeHtml(name) + '</span>');
    });
    el.chart.innerHTML = grid.join('') + paths.join('');
    if (el.chartNote) {
      el.chartNote.innerHTML = 'Y-axis: funding percent per settlement event. ' + legend.join(' ');
    }
  }

  function renderPayload(payload) {
    state.lastPayload = payload;
    if (el.headerSymbol) {
      el.headerSymbol.textContent = payload.symbol || '-';
    }
    if (el.headerSelected) {
      el.headerSelected.textContent = (payload.selected_exchanges || []).join(', ') || '-';
    }
    if (el.headerRun) {
      el.headerRun.textContent = fmtTime(payload.requested_at);
    }
    renderBest(payload);
    renderChart(payload);
    renderExchanges(payload);
    renderPairs(payload);
    renderTimeline(payload);
    if (el.method && payload.method) {
      el.method.textContent = JSON.stringify(payload.method, null, 2);
    }
  }

  function analyze() {
    var symbol = (el.symbol.value || '').trim().toUpperCase();
    var exchanges = selectedExchanges();
    var points = parseInt(el.points.value || '200', 10);
    if (!symbol) {
      setStatus('Symbol is required.', 'error');
      return;
    }
    if (!exchanges.length) {
      setStatus('Select at least one exchange.', 'error');
      return;
    }
    setStatus('Fetching funding history...', 'pending');
    el.submit.disabled = true;
    fetch(rootPath + '/api/funding-history/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        symbol: symbol,
        exchanges: exchanges,
        windows_hours: windows.map(function (item) { return item.hours; }),
        funding_points: isNaN(points) ? 200 : points
      })
    }).then(function (response) {
      return response.json().then(function (data) {
        if (!response.ok) {
          throw new Error(data.detail || 'Request failed');
        }
        return data;
      });
    }).then(function (payload) {
      if (payload.windows && payload.windows.length) {
        windows = payload.windows;
      }
      renderPayload(payload);
      setStatus('Done.', 'ok');
    }).catch(function (err) {
      setStatus(err.message || String(err), 'error');
    }).finally(function () {
      el.submit.disabled = false;
    });
  }

  renderExchangeChecks();
  if (el.form) {
    el.form.addEventListener('submit', function (event) {
      event.preventDefault();
      analyze();
    });
  }
}());
