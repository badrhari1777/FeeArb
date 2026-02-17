(function () {
  'use strict';

  var defaultSymbol = (window.__COIN_SYMBOL__ || '').toUpperCase();
  var defaultWindow = parseInt(window.__DEFAULT_WINDOW_MINUTES__ || 4320, 10);
  var defaultFunding = parseInt(window.__DEFAULT_FUNDING_POINTS__ || 120, 10);

  var elements = {
    form: document.getElementById('analysis-form'),
    symbolInput: document.getElementById('symbol-input'),
    windowInput: document.getElementById('window-input'),
    fundingInput: document.getElementById('funding-input'),
    submit: document.getElementById('analysis-submit'),
    status: document.getElementById('analysis-status'),
    headerSymbol: document.getElementById('header-symbol'),
    headerWindow: document.getElementById('header-window'),
    headerFunding: document.getElementById('header-funding'),
    headerRun: document.getElementById('header-run'),
    exchangeTable: document.getElementById('exchange-analysis-body'),
    pairTable: document.getElementById('pair-analysis-body'),
    botLogic: document.getElementById('bot-logic-block'),
    fundingHead: document.getElementById('funding-history-head'),
    fundingBody: document.getElementById('funding-history-body'),
    candleSummary: document.getElementById('candle-summary'),
    candleMeta: document.getElementById('candle-meta')
  };

  function escapeHtml(value) {
    return String(value === undefined || value === null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function formatNumber(val, digits) {
    if (val === undefined || val === null || isNaN(val)) {
      return '-';
    }
    return Number(val).toFixed(digits);
  }

  function formatPercent(val, digits) {
    if (val === undefined || val === null || isNaN(val)) {
      return '-';
    }
    return (Number(val) * 100).toFixed(digits) + '%';
  }

  function _pad(num) {
    var n = parseInt(num, 10);
    return n < 10 ? '0' + n : String(n);
  }

  function formatTs(ts) {
    if (!ts && ts !== 0) {
      return '-';
    }
    var date = new Date(ts);
    if (isNaN(date.getTime())) {
      return '-';
    }
    var shifted = new Date(date.getTime() + 3 * 60 * 60 * 1000);
    return _pad(shifted.getUTCMonth() + 1) + '-' + _pad(shifted.getUTCDate()) + ' ' +
      _pad(shifted.getUTCHours()) + ':' + _pad(shifted.getUTCMinutes());
  }

  function setStatus(message, isError) {
    if (!elements.status) {
      return;
    }
    elements.status.textContent = message || '';
    elements.status.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function buildUrl(symbol, windowMinutes, fundingPoints) {
    var params = [];
    params.push('window_minutes=' + encodeURIComponent(windowMinutes));
    params.push('funding_points=' + encodeURIComponent(fundingPoints));
    return '/api/coin/' + encodeURIComponent(symbol) + '?' + params.join('&');
  }

  function requestAnalysis(symbol, windowMinutes, fundingPoints, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', buildUrl(symbol, windowMinutes, fundingPoints), true);
    xhr.setRequestHeader('Accept', 'application/json');
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) {
        return;
      }
      var payload = null;
      if (xhr.responseText) {
        try {
          payload = JSON.parse(xhr.responseText);
        } catch (_ignore) {
          payload = null;
        }
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        callback(null, payload);
        return;
      }
      var detail = payload && payload.detail ? String(payload.detail) : ('status ' + xhr.status);
      callback(new Error('Request failed: ' + detail), null);
    };
    xhr.onerror = function () {
      callback(new Error('Network error'), null);
    };
    xhr.send();
  }

  function renderExchangeTable(exchanges, fundingPoints) {
    if (!elements.exchangeTable) {
      return;
    }
    if (!exchanges || !exchanges.length) {
      elements.exchangeTable.innerHTML = '<tr><td colspan="12" class="muted">No exchanges returned data.</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < exchanges.length; i += 1) {
      var ex = exchanges[i] || {};
      var snap = ex.snapshot || {};
      var funding = ex.funding_history && ex.funding_history.length ? ex.funding_history[0] : null;
      var oi = ex.open_interest || {};
      var quality = ex.data_quality || {};
      var notes = []
        .concat(ex.errors || [])
        .concat(ex.warnings || []);
      if (quality.candles_coverage_pct !== undefined && quality.candles_coverage_pct !== null) {
        notes.push('coverage=' + formatNumber(quality.candles_coverage_pct, 1) + '%');
      }
      html += '<tr>' +
        '<td>' + escapeHtml(ex.exchange || '-') + '</td>' +
        '<td>' + formatPercent(funding ? funding.rate : snap.funding_rate, 4) + '</td>' +
        '<td>' + escapeHtml(snap.next_funding_time || '-') + '</td>' +
        '<td>' + formatNumber(snap.spread, 6) + '</td>' +
        '<td>' + formatNumber(snap.mid, 6) + '</td>' +
        '<td>' + formatNumber(snap.mark_price, 6) + '</td>' +
        '<td>' + escapeHtml(ex.candles_1m ? ex.candles_1m.length : 0) + '</td>' +
        '<td>' + escapeHtml(ex.funding_history ? ex.funding_history.length : 0) + ' / ' + fundingPoints + '</td>' +
        '<td>' + formatNumber(ex.funding_interval_hours_resolved, 2) + 'h</td>' +
        '<td>' + escapeHtml(oi.history ? oi.history.length : 0) + '</td>' +
        '<td>' + escapeHtml(ex.status || '-') + '</td>' +
        '<td>' + escapeHtml(notes.join('; ')) + '</td>' +
      '</tr>';
    }
    elements.exchangeTable.innerHTML = html;
  }

  function renderPairTable(pairs) {
    if (!elements.pairTable) {
      return;
    }
    if (!pairs || !pairs.length) {
      elements.pairTable.innerHTML = '<tr><td colspan="12" class="muted">Pair analysis is empty.</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < pairs.length; i += 1) {
      var row = pairs[i] || {};
      var spread = row.spread || {};
      var interval = row.funding_interval_hours || {};
      var funding = row.funding_hourly || {};
      var oi = row.open_interest || {};
      var reasons = (row.reasons || []).join(', ');
      html += '<tr>' +
        '<td>' + escapeHtml((row.left_exchange || '-') + ' vs ' + (row.right_exchange || '-')) + '</td>' +
        '<td>' + escapeHtml(row.recommendation || '-') + '</td>' +
        '<td>' + formatNumber(row.score, 2) + '</td>' +
        '<td>' + formatNumber(spread.current_pct, 4) + '</td>' +
        '<td>' + formatNumber(spread.weighted_mean_pct, 4) + '</td>' +
        '<td>' + formatNumber(spread.p95_abs_pct, 4) + '</td>' +
        '<td>' + formatNumber(spread.z_score, 3) + '</td>' +
        '<td>' + escapeHtml(
          (interval.left !== null && interval.left !== undefined ? formatNumber(interval.left, 2) : '-') +
          ' / ' +
          (interval.right !== null && interval.right !== undefined ? formatNumber(interval.right, 2) : '-') +
          ' (match=' + (interval.match ? 'yes' : 'no') + ')'
        ) + '</td>' +
        '<td>' + formatPercent(funding.delta, 4) + '</td>' +
        '<td>' + formatNumber(oi.divergence_6h_pct, 2) + '</td>' +
        '<td>' + formatNumber(spread.coverage_pct, 2) + '</td>' +
        '<td>' + escapeHtml(reasons || '-') + '</td>' +
      '</tr>';
    }
    elements.pairTable.innerHTML = html;
  }

  function renderBotLogic(logic) {
    if (!elements.botLogic) {
      return;
    }
    if (!logic) {
      elements.botLogic.textContent = 'No decision yet.';
      return;
    }
    var lines = [];
    lines.push('Decision: ' + (logic.decision || '-'));
    lines.push('Score: ' + formatNumber(logic.score, 2));
    var pair = logic.recommended_pair || {};
    lines.push('Recommended pair: ' + (pair.left_exchange || '-') + ' vs ' + (pair.right_exchange || '-'));
    lines.push('Reason: ' + (logic.reason || '-'));
    var reasons = logic.pair_reasons || [];
    lines.push('Pair reasons: ' + (reasons.length ? reasons.join(', ') : '-'));
    lines.push('Note: ' + (logic.note || '-'));
    elements.botLogic.textContent = lines.join('\n');
  }

  function renderFundingHistory(exchanges, windowMinutes) {
    if (!elements.fundingBody || !elements.fundingHead) {
      return;
    }
    var list = Array.isArray(exchanges) ? exchanges.filter(Boolean) : [];
    if (!list.length) {
      elements.fundingBody.innerHTML = '<tr><td colspan="2" class="muted">No data</td></tr>';
      elements.fundingHead.innerHTML = '<tr><th class="slot-col">#</th><th class="time-col">Time (UTC+3)</th></tr>';
      return;
    }

    var enriched = [];
    var snapshotRates = {};
    var latest = 0;
    var i;
    for (i = 0; i < list.length; i += 1) {
      var ex = list[i] || {};
      var history = ex.funding_history ? ex.funding_history.slice() : [];
      history.sort(function (a, b) {
        var ta = (a && (a.ts_ms || a.timestamp || 0)) || 0;
        var tb = (b && (b.ts_ms || b.timestamp || 0)) || 0;
        return tb - ta;
      });
      if (history.length) {
        latest = Math.max(latest, history[0].ts_ms || history[0].timestamp || 0);
      }
      if (ex.snapshot && typeof ex.snapshot.funding_rate === 'number') {
        snapshotRates[ex.exchange || '-'] = ex.snapshot.funding_rate;
      }
      enriched.push({
        exchange: ex.exchange || '-',
        history: history
      });
    }

    var nowTs = Date.now();
    latest = Math.max(latest || 0, nowTs);
    var anchor = new Date(latest);
    anchor.setUTCMinutes(0, 0, 0);
    var bucketSize = 60 * 60 * 1000;
    var rowCount = Math.max(24, Math.min(120, Math.ceil((windowMinutes || 4320) / 60)));
    var buckets = [];
    for (i = 0; i < rowCount; i += 1) {
      buckets.push(anchor.getTime() - i * bucketSize);
    }

    var maps = {};
    for (i = 0; i < enriched.length; i += 1) {
      var entry = enriched[i];
      var map = {};
      var hist = entry.history || [];
      for (var j = 0; j < hist.length; j += 1) {
        var itm = hist[j] || {};
        var t = itm.ts_ms || itm.timestamp;
        if (!t && t !== 0) {
          continue;
        }
        var bucket = Math.floor(t / bucketSize) * bucketSize;
        if (!map.hasOwnProperty(bucket)) {
          map[bucket] = itm;
        }
      }
      maps[entry.exchange] = map;
    }

    var headHtml = '<tr><th class="slot-col">#</th><th class="time-col">Time (UTC+3)</th>';
    for (i = 0; i < enriched.length; i += 1) {
      headHtml += '<th>' + escapeHtml(enriched[i].exchange) + '</th>';
    }
    headHtml += '</tr>';
    elements.fundingHead.innerHTML = headHtml;

    var bodyHtml = '<tr class="row-now"><td class="slot-col history-time">now</td><td class="history-time time-col">' + escapeHtml(formatTs(nowTs)) + '</td>';
    for (i = 0; i < enriched.length; i += 1) {
      var exchNameNow = enriched[i].exchange;
      var nowRate = snapshotRates.hasOwnProperty(exchNameNow) ? snapshotRates[exchNameNow] : null;
      bodyHtml += '<td class="history-rate">' + (nowRate === null || nowRate === undefined ? '-' : formatPercent(nowRate, 5)) + '</td>';
    }
    bodyHtml += '</tr>';

    for (i = 0; i < buckets.length; i += 1) {
      var b = buckets[i];
      var slotLabel = String(i + 1);
      var timeLabel = formatTs(b);
      bodyHtml += '<tr><td class="slot-col history-time">' + escapeHtml(slotLabel) + '</td><td class="history-time time-col">' + escapeHtml(timeLabel) + '</td>';
      for (var k = 0; k < enriched.length; k += 1) {
        var exchName = enriched[k].exchange;
        var cell = maps[exchName] && maps[exchName][b];
        var rateText = cell ? formatPercent(cell.rate, 5) : '-';
        bodyHtml += '<td class="history-rate">' + rateText + '</td>';
      }
      bodyHtml += '</tr>';
    }
    elements.fundingBody.innerHTML = bodyHtml;
  }

  function renderCandles(exchanges) {
    if (!elements.candleMeta || !elements.candleSummary) {
      return;
    }
    if (!exchanges || !exchanges.length) {
      elements.candleSummary.textContent = 'No candles loaded yet.';
      elements.candleMeta.innerHTML = '';
      return;
    }
    var summaryParts = [];
    var metaHtml = '';
    for (var i = 0; i < exchanges.length; i += 1) {
      var ex = exchanges[i] || {};
      var candles = ex.candles_1m || [];
      if (!candles.length) {
        continue;
      }
      var first = candles[candles.length - 1] || {};
      var last = candles[0] || {};
      summaryParts.push((ex.exchange || '-') + ': ' + candles.length + ' bars');
      metaHtml += '<div class="grid-card">' +
        '<h3>' + escapeHtml(ex.exchange || '-') + '</h3>' +
        '<p class="muted">Latest close: ' + formatNumber(last.close, 6) + ' @ ' + formatTs(last.ts_ms) + '</p>' +
        '<p class="muted">Oldest: ' + formatTs(first.ts_ms) + '</p>' +
      '</div>';
    }
    elements.candleSummary.textContent = summaryParts.length ? summaryParts.join(' | ') : 'Candles unavailable.';
    elements.candleMeta.innerHTML = metaHtml || '<p class="muted">No candle data.</p>';
  }

  function updateHeader(symbol, windowMinutes, fundingPoints, requestedAt) {
    if (elements.headerSymbol) {
      elements.headerSymbol.textContent = symbol;
    }
    if (elements.headerWindow) {
      elements.headerWindow.textContent = windowMinutes;
    }
    if (elements.headerFunding) {
      elements.headerFunding.textContent = fundingPoints;
    }
    if (elements.headerRun) {
      elements.headerRun.textContent = requestedAt || '-';
    }
  }

  function handleSubmit(event) {
    if (event) {
      event.preventDefault();
    }
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    var windowMinutes = parseInt(elements.windowInput.value || defaultWindow, 10);
    var fundingPoints = parseInt(elements.fundingInput.value || defaultFunding, 10);
    if (!symbol) {
      setStatus('Enter symbol.', true);
      return;
    }
    setStatus('Fetching historical data...', false);
    elements.submit.disabled = true;

    requestAnalysis(symbol, windowMinutes, fundingPoints, function (err, payload) {
      elements.submit.disabled = false;
      if (err) {
        setStatus(err.message || 'Error fetching analysis', true);
        return;
      }
      setStatus('Updated', false);
      updateHeader(symbol, windowMinutes, fundingPoints, payload && payload.requested_at);
      renderExchangeTable(payload && payload.exchanges, fundingPoints);
      renderPairTable(payload && payload.pair_analysis);
      renderBotLogic(payload && payload.bot_logic);
      renderFundingHistory(payload && payload.exchanges, windowMinutes);
      renderCandles(payload && payload.exchanges);
    });
  }

  function init() {
    if (elements.symbolInput && !elements.symbolInput.value) {
      elements.symbolInput.value = defaultSymbol;
    }
    if (elements.windowInput && !elements.windowInput.value) {
      elements.windowInput.value = defaultWindow;
    }
    if (elements.fundingInput && !elements.fundingInput.value) {
      elements.fundingInput.value = defaultFunding;
    }
    if (elements.form) {
      elements.form.addEventListener('submit', handleSubmit);
    }
    handleSubmit(null);
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    init();
  } else {
    document.addEventListener('DOMContentLoaded', init);
  }
})();
