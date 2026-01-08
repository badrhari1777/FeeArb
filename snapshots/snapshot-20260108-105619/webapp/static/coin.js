(function () {
  'use strict';

  var defaultSymbol = (window.__COIN_SYMBOL__ || '').toUpperCase();
  var defaultWindow = parseInt(window.__DEFAULT_WINDOW_MINUTES__ || 720, 10);
  var defaultFunding = parseInt(window.__DEFAULT_FUNDING_POINTS__ || 24, 10);

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
    // Shift to UTC+3 and drop the year for compactness.
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
      if (xhr.readyState === 4) {
        if (xhr.status >= 200 && xhr.status < 300) {
          try {
            var data = JSON.parse(xhr.responseText);
            callback(null, data);
          } catch (err) {
            callback(err);
          }
        } else {
          callback(new Error('Request failed with status ' + xhr.status));
        }
      }
    };
    xhr.send();
  }

  function renderExchangeTable(exchanges, fundingPoints) {
    if (!elements.exchangeTable) {
      return;
    }
    if (!exchanges || !exchanges.length) {
      elements.exchangeTable.innerHTML = '<tr><td colspan="10" class="muted">No exchanges returned data.</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < exchanges.length; i += 1) {
      var ex = exchanges[i] || {};
      var snap = ex.snapshot || {};
      var funding = ex.funding_history && ex.funding_history.length ? ex.funding_history[0] : null;
      html += '<tr>' +
        '<td>' + escapeHtml(ex.exchange || '-') + '</td>' +
        '<td>' + formatPercent(funding ? funding.rate : snap.funding_rate, 4) + '</td>' +
        '<td>' + escapeHtml(snap.next_funding_time || '-') + '</td>' +
        '<td>' + formatNumber(snap.spread, 6) + '</td>' +
        '<td>' + formatNumber(snap.mid, 6) + '</td>' +
        '<td>' + formatNumber(snap.mark_price, 6) + '</td>' +
        '<td>' + escapeHtml(ex.candles_1m ? ex.candles_1m.length : 0) + '</td>' +
        '<td>' + escapeHtml(ex.funding_history ? ex.funding_history.length : 0) + ' / ' + fundingPoints + '</td>' +
        '<td>' + escapeHtml(ex.status || '-') + '</td>' +
        '<td>' + escapeHtml((ex.errors && ex.errors.join('; ')) || (ex.warnings && ex.warnings.join('; ')) || '') + '</td>' +
      '</tr>';
    }
    elements.exchangeTable.innerHTML = html;
  }

  function renderFundingHistory(exchanges) {
    if (!elements.fundingBody || !elements.fundingHead) {
      return;
    }
    var list = Array.isArray(exchanges) ? exchanges.filter(Boolean) : [];
    if (!list.length) {
      elements.fundingBody.innerHTML = '<tr><td colspan="2" class="muted">No data</td></tr>';
      elements.fundingHead.innerHTML = '<tr><th class="slot-col">#</th><th class="time-col">Time (UTC+3)</th></tr>';
      return;
    }

    // Build histories (cap to 24) and snapshot map for "now" row.
    var enriched = [];
    var snapshotRates = {};
    for (var i = 0; i < list.length; i += 1) {
      var ex = list[i] || {};
      var history = ex.funding_history ? ex.funding_history.slice() : [];
      history.sort(function (a, b) {
        var ta = (a && (a.ts_ms || a.timestamp || 0)) || 0;
        var tb = (b && (b.ts_ms || b.timestamp || 0)) || 0;
        return tb - ta;
      });
      if (history.length > 24) {
        history = history.slice(0, 24);
      }
      if (ex.snapshot && typeof ex.snapshot.funding_rate === 'number') {
        snapshotRates[ex.exchange || '-'] = ex.snapshot.funding_rate;
      }
      enriched.push({
        exchange: ex.exchange || '-',
        history: history
      });
    }

    // Determine bucket grid (hourly, last 24 slots).
    var latest = 0;
    for (i = 0; i < enriched.length; i += 1) {
      var h = enriched[i].history;
      if (h && h.length) {
        var tsCandidate = h[0].ts_ms || h[0].timestamp || 0;
        if (tsCandidate > latest) {
          latest = tsCandidate;
        }
      }
    }
    var nowTs = Date.now();
    latest = Math.max(latest || 0, nowTs);
    if (!latest) {
      elements.fundingBody.innerHTML = '<tr><td colspan="2" class="muted">No data</td></tr>';
      elements.fundingHead.innerHTML = '<tr><th class="slot-col">#</th><th class="time-col">Time (UTC+3)</th></tr>';
      return;
    }
    var anchor = new Date(latest);
    anchor.setUTCMinutes(0, 0, 0); // align to top of hour
    var bucketSize = 60 * 60 * 1000;
    var buckets = [];
    for (i = 0; i < 24; i += 1) {
      buckets.push(anchor.getTime() - i * bucketSize);
    }
    // Build exchange maps by hour bucket.
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

    // Render head.
    var headHtml = '<tr><th class="slot-col">#</th><th class="time-col">Time (UTC+3)</th>';
    for (i = 0; i < enriched.length; i += 1) {
      headHtml += '<th>' + escapeHtml(enriched[i].exchange) + '</th>';
    }
    headHtml += '</tr>';
    elements.fundingHead.innerHTML = headHtml;

    // Render "now" row from snapshots.
    var bodyHtml = '<tr class="row-now"><td class="slot-col history-time">now</td><td class="history-time time-col">' + escapeHtml(formatTs(nowTs)) + '</td>';
    for (i = 0; i < enriched.length; i += 1) {
      var exchNameNow = enriched[i].exchange;
      var nowRate = snapshotRates.hasOwnProperty(exchNameNow) ? snapshotRates[exchNameNow] : null;
      bodyHtml += '<td class="history-rate">' + (nowRate === null || nowRate === undefined ? '-' : formatPercent(nowRate, 5)) + '</td>';
    }
    bodyHtml += '</tr>';

    // Render history rows (1..24).
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
      setStatus('Введите символ.', true);
      return;
    }
    setStatus('Fetching...', false);
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
      renderFundingHistory(payload && payload.exchanges);
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
    // Run initial load.
    handleSubmit(null);
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    init();
  } else {
    document.addEventListener('DOMContentLoaded', init);
  }
})();
