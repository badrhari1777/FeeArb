(function () {
  'use strict';

  var defaultSymbol = (window.__COIN_SYMBOL__ || '').toUpperCase();
  var defaultWindow = parseInt(window.__DEFAULT_WINDOW_MINUTES__ || 4320, 10);
  var defaultFunding = parseInt(window.__DEFAULT_FUNDING_POINTS__ || 120, 10);
  var state = {
    lastAnalysis: null,
    lastReview: null,
    selectedPaperPositionKey: '',
    outcomesAutoStatusTimer: null,
    retentionStatusTimer: null
  };

  var elements = {
    form: document.getElementById('analysis-form'),
    symbolInput: document.getElementById('symbol-input'),
    windowInput: document.getElementById('window-input'),
    fundingInput: document.getElementById('funding-input'),
    submit: document.getElementById('analysis-submit'),
    status: document.getElementById('analysis-status'),
    exportJsonBtn: document.getElementById('export-json-btn'),
    exportCsvBtn: document.getElementById('export-csv-btn'),
    exportParquetBtn: document.getElementById('export-parquet-btn'),
    exportStatus: document.getElementById('export-status'),
    reviewDaysInput: document.getElementById('review-days-input'),
    reviewTopInput: document.getElementById('review-top-input'),
    reviewLoadBtn: document.getElementById('review-load-btn'),
    reviewExportJsonBtn: document.getElementById('review-export-json-btn'),
    reviewExportCsvBtn: document.getElementById('review-export-csv-btn'),
    reviewStatus: document.getElementById('review-status'),
    reviewSummaryBlock: document.getElementById('review-summary-block'),
    reviewActivityBody: document.getElementById('review-activity-body'),
    reviewShortlistBody: document.getElementById('review-shortlist-body'),
    reviewTagsBody: document.getElementById('review-tags-body'),
    entryReviewBody: document.getElementById('entry-review-body'),
    exitReviewBody: document.getElementById('exit-review-body'),
    entryActionScorecardBody: document.getElementById('entry-action-scorecard-body'),
    exitActionScorecardBody: document.getElementById('exit-action-scorecard-body'),
    phaseScorecardBody: document.getElementById('phase-scorecard-body'),
    replayLimitInput: document.getElementById('replay-limit-input'),
    replayRunBtn: document.getElementById('replay-run-btn'),
    replayStatus: document.getElementById('replay-status'),
    replayBlock: document.getElementById('replay-block'),
    outcomesHorizonsInput: document.getElementById('outcomes-horizons-input'),
    outcomesLimitInput: document.getElementById('outcomes-limit-input'),
    outcomesFilterHorizonsInput: document.getElementById('outcomes-filter-horizons-input'),
    outcomesFilterPhaseSelect: document.getElementById('outcomes-filter-phase-select'),
    outcomesFilterActionsInput: document.getElementById('outcomes-filter-actions-input'),
    outcomesLoadBtn: document.getElementById('outcomes-load-btn'),
    outcomesRunBtn: document.getElementById('outcomes-run-btn'),
    outcomesAutoRunBtn: document.getElementById('outcomes-auto-run-btn'),
    outcomesAutoPauseBtn: document.getElementById('outcomes-auto-pause-btn'),
    outcomesAutoResumeBtn: document.getElementById('outcomes-auto-resume-btn'),
    outcomesStatus: document.getElementById('outcomes-status'),
    outcomesAutoHealth: document.getElementById('outcomes-auto-health'),
    outcomesAutoStatusBlock: document.getElementById('outcomes-auto-status-block'),
    outcomesReviewBody: document.getElementById('outcomes-review-body'),
    outcomesBlock: document.getElementById('outcomes-block'),
    operatorScorecardBlock: document.getElementById('operator-scorecard-block'),
    retentionMaxAgeInput: document.getElementById('retention-max-age-input'),
    retentionClosedPaperInput: document.getElementById('retention-closed-paper-input'),
    retentionRunBtn: document.getElementById('retention-run-btn'),
    retentionStatus: document.getElementById('retention-status'),
    retentionBlock: document.getElementById('retention-block'),
    headerSymbol: document.getElementById('header-symbol'),
    headerWindow: document.getElementById('header-window'),
    headerFunding: document.getElementById('header-funding'),
    headerRun: document.getElementById('header-run'),
    exchangeTable: document.getElementById('exchange-analysis-body'),
    pairTable: document.getElementById('pair-analysis-body'),
    botLogic: document.getElementById('bot-logic-block'),
    decisionJournal: document.getElementById('decision-journal-block'),
    positionLogicPaperBody: document.getElementById('position-logic-paper-body'),
    positionLogicRealBody: document.getElementById('position-logic-real-body'),
    positionLogic: document.getElementById('position-logic-block'),
    fundingHead: document.getElementById('funding-history-head'),
    fundingBody: document.getElementById('funding-history-body'),
    candleSummary: document.getElementById('candle-summary'),
    candleMeta: document.getElementById('candle-meta'),
    visualSummary: document.getElementById('visual-summary'),
    visualWindowBody: document.getElementById('visual-window-body'),
    visualSpreadChart: document.getElementById('visual-spread-chart'),
    visualFundingChart: document.getElementById('visual-funding-chart'),
    visualNotes: document.getElementById('visual-notes'),
    paperForm: document.getElementById('paper-enter-form'),
    paperQtyInput: document.getElementById('paper-qty-input'),
    paperPairInput: document.getElementById('paper-pair-input'),
    paperDirectionInput: document.getElementById('paper-direction-input'),
    paperActionInput: document.getElementById('paper-action-input'),
    paperEnterSubmit: document.getElementById('paper-enter-submit'),
    paperEnterStatus: document.getElementById('paper-enter-status'),
    paperPositionsBody: document.getElementById('paper-positions-body'),
    paperEventsTitle: document.getElementById('paper-events-title'),
    paperEventsBlock: document.getElementById('paper-events-block')
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

  function formatSignedNumber(val, digits, suffix) {
    if (val === undefined || val === null || isNaN(val)) {
      return '-';
    }
    var num = Number(val);
    var text = num.toFixed(digits);
    if (num > 0) {
      text = '+' + text;
    }
    return text + (suffix || '');
  }

  function valueToneClass(val) {
    if (val === undefined || val === null || isNaN(val)) {
      return '';
    }
    if (Number(val) > 0) {
      return 'value-pos';
    }
    if (Number(val) < 0) {
      return 'value-neg';
    }
    return '';
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

  function setPaperStatus(message, isError) {
    if (!elements.paperEnterStatus) {
      return;
    }
    elements.paperEnterStatus.textContent = message || '';
    elements.paperEnterStatus.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function setExportStatus(message, isError) {
    if (!elements.exportStatus) {
      return;
    }
    elements.exportStatus.textContent = message || '';
    elements.exportStatus.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function setReplayStatus(message, isError) {
    if (!elements.replayStatus) {
      return;
    }
    elements.replayStatus.textContent = message || '';
    elements.replayStatus.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function setReviewStatus(message, isError) {
    if (!elements.reviewStatus) {
      return;
    }
    elements.reviewStatus.textContent = message || '';
    elements.reviewStatus.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function setOutcomesStatus(message, isError) {
    if (!elements.outcomesStatus) {
      return;
    }
    elements.outcomesStatus.textContent = message || '';
    elements.outcomesStatus.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function setRetentionStatus(message, isError) {
    if (!elements.retentionStatus) {
      return;
    }
    elements.retentionStatus.textContent = message || '';
    elements.retentionStatus.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function requestJson(method, url, body, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open(method, url, true);
    xhr.setRequestHeader('Accept', 'application/json');
    if (body !== undefined && body !== null) {
      xhr.setRequestHeader('Content-Type', 'application/json');
    }
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
    if (body !== undefined && body !== null) {
      xhr.send(JSON.stringify(body));
    } else {
      xhr.send();
    }
  }

  function buildUrl(symbol, windowMinutes, fundingPoints) {
    var params = [];
    params.push('window_minutes=' + encodeURIComponent(windowMinutes));
    params.push('funding_points=' + encodeURIComponent(fundingPoints));
    return '/api/coin/' + encodeURIComponent(symbol) + '?' + params.join('&');
  }

  function requestAnalysis(symbol, windowMinutes, fundingPoints, callback) {
    requestJson('GET', buildUrl(symbol, windowMinutes, fundingPoints), null, callback);
  }

  function requestPaperPositions(symbol, callback) {
    var url = '/api/coin/paper/positions?symbol=' + encodeURIComponent(symbol);
    requestJson('GET', url, null, callback);
  }

  function requestPaperEvents(positionKey, callback) {
    var url = '/api/coin/paper/events/' + encodeURIComponent(positionKey) + '?limit=100';
    requestJson('GET', url, null, callback);
  }

  function requestPaperEnter(payload, callback) {
    requestJson('POST', '/api/coin/paper/enter', payload, callback);
  }

  function requestPaperAction(payload, callback) {
    requestJson('POST', '/api/coin/paper/action', payload, callback);
  }

  function requestReplay(symbol, limit, callback) {
    var safeLimit = parseInt(limit, 10);
    if (!safeLimit || safeLimit < 10) {
      safeLimit = 500;
    }
    if (safeLimit > 5000) {
      safeLimit = 5000;
    }
    var url = '/api/coin/replay/' + encodeURIComponent(symbol) +
      '?limit=' + encodeURIComponent(safeLimit) +
      '&include_stored_decisions=1';
    requestJson('GET', url, null, callback);
  }

  function requestWeeklyReview(symbol, days, top, callback) {
    var safeDays = parseInt(days, 10);
    var safeTop = parseInt(top, 10);
    if (!safeDays || safeDays < 1) {
      safeDays = 7;
    }
    if (!safeTop || safeTop < 1) {
      safeTop = 3;
    }
    var url;
    if (symbol) {
      url = '/api/coin/review/' + encodeURIComponent(symbol) +
        '?days=' + encodeURIComponent(safeDays) +
        '&top=' + encodeURIComponent(safeTop) +
        '&include_live_analysis=0';
    } else {
      url = '/api/coin/review/weekly?days=' + encodeURIComponent(safeDays) +
        '&top=' + encodeURIComponent(safeTop);
    }
    requestJson('GET', url, null, callback);
  }

  function requestOutcomes(symbol, limit, filters, callback) {
    var safeLimit = parseInt(limit, 10);
    if (!safeLimit || safeLimit < 10) {
      safeLimit = 500;
    }
    if (safeLimit > 5000) {
      safeLimit = 5000;
    }
    var query = ['limit=' + encodeURIComponent(safeLimit)];
    var safeFilters = filters || {};
    if (safeFilters.horizons) {
      query.push('horizons=' + encodeURIComponent(String(safeFilters.horizons)));
    }
    if (safeFilters.phaseBucket) {
      query.push('phase_buckets=' + encodeURIComponent(String(safeFilters.phaseBucket)));
    }
    if (safeFilters.actions) {
      query.push('actions=' + encodeURIComponent(String(safeFilters.actions)));
    }
    var url = '/api/coin/outcomes/' + encodeURIComponent(symbol) + '?' + query.join('&');
    requestJson('GET', url, null, callback);
  }

  function requestOutcomesAutoStatus(symbol, callback) {
    var url = '/api/coin/outcomes/auto-status';
    var safeSymbol = String(symbol || '').trim().toUpperCase();
    if (safeSymbol) {
      url += '?symbol=' + encodeURIComponent(safeSymbol);
    }
    requestJson('GET', url, null, callback);
  }

  function requestOutcomesAutoRun(symbol, callback) {
    var url = '/api/coin/outcomes/auto-run';
    var safeSymbol = String(symbol || '').trim().toUpperCase();
    if (safeSymbol) {
      url += '?symbol=' + encodeURIComponent(safeSymbol);
    }
    requestJson('POST', url, null, callback);
  }

  function requestOutcomesAutoScheduler(enabled, callback) {
    var url = '/api/coin/outcomes/auto-scheduler?enabled=' + encodeURIComponent(enabled ? '1' : '0');
    requestJson('POST', url, null, callback);
  }

  function requestRetentionStatus(callback) {
    requestJson('GET', '/api/coin/maintenance/retention-status', null, callback);
  }

  function requestRetentionRun(maxAgeDays, closedPaperDays, callback) {
    var url = '/api/coin/maintenance/retention-run';
    var params = [];
    if (maxAgeDays !== undefined && maxAgeDays !== null && maxAgeDays !== '') {
      params.push('max_age_days=' + encodeURIComponent(maxAgeDays));
    }
    if (closedPaperDays !== undefined && closedPaperDays !== null && closedPaperDays !== '') {
      params.push('closed_paper_days=' + encodeURIComponent(closedPaperDays));
    }
    if (params.length) {
      url += '?' + params.join('&');
    }
    requestJson('POST', url, null, callback);
  }

  function requestEvaluateOutcomes(symbol, horizons, decisionLimit, force, callback) {
    var safeLimit = parseInt(decisionLimit, 10);
    if (!safeLimit || safeLimit < 10) {
      safeLimit = 500;
    }
    if (safeLimit > 5000) {
      safeLimit = 5000;
    }
    var horizonText = String(horizons || '').trim();
    if (!horizonText) {
      horizonText = '15m,1h,4h';
    }
    var url = '/api/coin/outcomes/' + encodeURIComponent(symbol) +
      '/evaluate?horizons=' + encodeURIComponent(horizonText) +
      '&decision_limit=' + encodeURIComponent(safeLimit) +
      '&force=' + encodeURIComponent(force ? '1' : '0');
    requestJson('POST', url, null, callback);
  }

  function readOutcomesFilters() {
    return {
      horizons: elements.outcomesFilterHorizonsInput && elements.outcomesFilterHorizonsInput.value ?
        elements.outcomesFilterHorizonsInput.value.trim() : '',
      phaseBucket: elements.outcomesFilterPhaseSelect && elements.outcomesFilterPhaseSelect.value ?
        elements.outcomesFilterPhaseSelect.value.trim() : '',
      actions: elements.outcomesFilterActionsInput && elements.outcomesFilterActionsInput.value ?
        elements.outcomesFilterActionsInput.value.trim().toUpperCase() : ''
    };
  }

  function buildExportUrl(kind) {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    var windowMinutes = parseInt(elements.windowInput.value || defaultWindow, 10);
    var fundingPoints = parseInt(elements.fundingInput.value || defaultFunding, 10);
    if (!symbol) {
      return '';
    }
    var base = '/api/coin/export/' + encodeURIComponent(symbol);
    var query = '?include_live_analysis=1' +
      '&window_minutes=' + encodeURIComponent(windowMinutes) +
      '&funding_points=' + encodeURIComponent(fundingPoints);
    if (kind === 'json') {
      return base + query;
    }
    if (kind === 'csv') {
      return base + '/timeline.csv' + query;
    }
    if (kind === 'parquet') {
      return base + '/timeline.parquet' + query;
    }
    return '';
  }

  function buildReviewExportUrl(kind) {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    var days = parseInt(elements.reviewDaysInput && elements.reviewDaysInput.value ? elements.reviewDaysInput.value : '7', 10);
    var top = parseInt(elements.reviewTopInput && elements.reviewTopInput.value ? elements.reviewTopInput.value : '3', 10);
    if (!days || days < 1) {
      days = 7;
    }
    if (!top || top < 1) {
      top = 3;
    }
    if (kind === 'json') {
      if (symbol) {
        return '/api/coin/review/' + encodeURIComponent(symbol) +
          '?days=' + encodeURIComponent(days) +
          '&top=' + encodeURIComponent(top) +
          '&include_live_analysis=0';
      }
      return '/api/coin/review/weekly?days=' + encodeURIComponent(days) +
        '&top=' + encodeURIComponent(top);
    }
    if (kind === 'csv') {
      if (symbol) {
        return '/api/coin/review/' + encodeURIComponent(symbol) +
          '/timeline.csv?days=' + encodeURIComponent(days) +
          '&top=' + encodeURIComponent(top);
      }
      return '/api/coin/review/weekly.csv?days=' + encodeURIComponent(days) +
        '&top=' + encodeURIComponent(top);
    }
    return '';
  }

  function triggerExport(kind) {
    var url = buildExportUrl(kind);
    if (!url) {
      setExportStatus('Symbol is required for export.', true);
      return;
    }
    setExportStatus('Starting export: ' + kind.toUpperCase(), false);
    window.open(url, '_blank');
  }

  function triggerReviewExport(kind) {
    var url = buildReviewExportUrl(kind);
    if (!url) {
      setReviewStatus('Review export is unavailable.', true);
      return;
    }
    setReviewStatus('Starting review export: ' + kind.toUpperCase(), false);
    window.open(url, '_blank');
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
      elements.pairTable.innerHTML = '<tr><td colspan="14" class="muted">Pair analysis is empty.</td></tr>';
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
        '<td>' + escapeHtml(row.selected_direction || '-') + '</td>' +
        '<td>' + escapeHtml(row.selected_action || '-') + '</td>' +
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
    lines.push('Recommended action: ' + (logic.recommended_action || '-'));
    lines.push('Score: ' + formatNumber(logic.score, 2));
    var pair = logic.recommended_pair || {};
    lines.push('Recommended pair: ' + (pair.left_exchange || '-') + ' vs ' + (pair.right_exchange || '-'));
    lines.push('Pair key: ' + (pair.pair_key || '-'));
    lines.push('Direction: ' + (pair.direction || '-'));
    lines.push('Decision phase: ' + (logic.decision_phase || pair.decision_phase || '-'));
    lines.push('Feature snapshot id: ' + (pair.feature_snapshot_id !== undefined ? pair.feature_snapshot_id : '-'));
    lines.push('Reason: ' + (logic.reason || '-'));
    var reasons = logic.reason_codes || logic.pair_reasons || [];
    lines.push('Pair reasons: ' + (reasons.length ? reasons.join(', ') : '-'));
    var reasonText = logic.reason_text || [];
    if (reasonText.length) {
      lines.push('Reason text: ' + reasonText.join(' | '));
    }
    lines.push('Note: ' + (logic.note || '-'));
    elements.botLogic.textContent = lines.join('\n');
  }

  function renderDecisionJournal(journal) {
    if (!elements.decisionJournal) {
      return;
    }
    if (!journal) {
      elements.decisionJournal.textContent = 'No decision journal entries yet.';
      return;
    }
    elements.decisionJournal.textContent = JSON.stringify(journal, null, 2);
  }

  function renderPositionLogic(positionLogic) {
    if (!elements.positionLogic) {
      return;
    }
    if (!positionLogic) {
      if (elements.positionLogicPaperBody) {
        elements.positionLogicPaperBody.innerHTML = '<tr><td colspan="8" class="muted">No paper position logic yet.</td></tr>';
      }
      if (elements.positionLogicRealBody) {
        elements.positionLogicRealBody.innerHTML = '<tr><td colspan="8" class="muted">No real/manual position logic yet.</td></tr>';
      }
      elements.positionLogic.textContent = 'No position logic yet.';
      return;
    }

    function _reviewText(row) {
      var correctness = row && row.latest_correctness ? String(row.latest_correctness).toUpperCase() : '';
      var horizon = row && row.latest_review_horizon ? String(row.latest_review_horizon) : '';
      var timing = row && row.latest_timing_quality ? String(row.latest_timing_quality) : '';
      if (!correctness) {
        return '-';
      }
      var chunks = [correctness];
      if (horizon) {
        chunks.push('@' + horizon);
      }
      if (timing) {
        chunks.push('timing=' + timing);
      }
      return chunks.join(' | ');
    }

    function _renderRows(target, rows, emptyText) {
      if (!target) {
        return;
      }
      if (!rows || !rows.length) {
        target.innerHTML = '<tr><td colspan="8" class="muted">' + escapeHtml(emptyText) + '</td></tr>';
        return;
      }
      var html = '';
      for (var i = 0; i < rows.length; i += 1) {
        var row = rows[i] || {};
        html += '<tr>' +
          '<td>' + escapeHtml(row.position_key || '-') + '</td>' +
          '<td>' + escapeHtml(row.pair_key || '-') + '</td>' +
          '<td>' + escapeHtml(row.direction || '-') + '</td>' +
          '<td>' + escapeHtml(row.action || '-') + '</td>' +
          '<td>' + escapeHtml(row.decision_phase || '-') + '</td>' +
          '<td>' + formatNumber(row.minutes_to_next_funding, 1) + '</td>' +
          '<td>' + escapeHtml(formatTs(row.decision_ts_ms)) + '</td>' +
          '<td>' + escapeHtml(_reviewText(row)) + '</td>' +
        '</tr>';
      }
      target.innerHTML = html;
    }

    _renderRows(
      elements.positionLogicPaperBody,
      positionLogic.paper || [],
      'No paper position logic yet.'
    );
    _renderRows(
      elements.positionLogicRealBody,
      positionLogic.real_manual || [],
      'No real/manual position logic yet.'
    );

    var rawPreview = {
      summary: positionLogic.summary || {},
      paper_count: (positionLogic.paper || []).length,
      real_manual_count: (positionLogic.real_manual || []).length
    };
    elements.positionLogic.textContent = JSON.stringify(rawPreview, null, 2);
  }

  function renderReplay(payload) {
    if (!elements.replayBlock) {
      return;
    }
    if (!payload) {
      elements.replayBlock.textContent = 'No replay yet.';
      return;
    }
    var summary = payload.summary || {};
    var out = {
      symbol: payload.symbol,
      replay_points: payload.replay_points,
      summary: summary,
      timeline_preview: (payload.timeline || []).slice(0, 20)
    };
    elements.replayBlock.textContent = JSON.stringify(out, null, 2);
  }

  function renderWeeklyReview(payload) {
    var review = payload && payload.review ? payload.review : payload;
    state.lastReview = review || null;
    if (!elements.reviewSummaryBlock) {
      return;
    }
    if (!review) {
      if (elements.reviewActivityBody) {
        elements.reviewActivityBody.innerHTML = '<tr><td colspan="7" class="muted">No recent activity yet.</td></tr>';
      }
      if (elements.reviewShortlistBody) {
        elements.reviewShortlistBody.innerHTML = '<tr><td colspan="8" class="muted">No shortlist history yet.</td></tr>';
      }
      if (elements.reviewTagsBody) {
        elements.reviewTagsBody.innerHTML = '<tr><td colspan="7" class="muted">No review tags yet.</td></tr>';
      }
      if (elements.entryReviewBody) {
        elements.entryReviewBody.innerHTML = '<tr><td colspan="6" class="muted">No entry review tags yet.</td></tr>';
      }
      if (elements.exitReviewBody) {
        elements.exitReviewBody.innerHTML = '<tr><td colspan="6" class="muted">No exit review tags yet.</td></tr>';
      }
      if (elements.entryActionScorecardBody) {
        elements.entryActionScorecardBody.innerHTML = '<tr><td colspan="5" class="muted">No entry action scorecard yet.</td></tr>';
      }
      if (elements.exitActionScorecardBody) {
        elements.exitActionScorecardBody.innerHTML = '<tr><td colspan="5" class="muted">No exit action scorecard yet.</td></tr>';
      }
      if (elements.phaseScorecardBody) {
        elements.phaseScorecardBody.innerHTML = '<tr><td colspan="5" class="muted">No phase scorecard yet.</td></tr>';
      }
      elements.reviewSummaryBlock.textContent = 'No weekly review yet.';
      return;
    }

    var scope = review.scope || {};
    var summary = review.summary || {};
    var reviewTags = review.review_tags || [];
    var entryReview = review.entry_review || {};
    var exitReview = review.exit_review || {};
    var lines = [];
    lines.push('Scope: days=' + (scope.days !== undefined ? scope.days : '-') +
      ' | top=' + (scope.top !== undefined ? scope.top : '-') +
      ' | symbol=' + (scope.symbol || 'ALL'));
    lines.push('Trade activity: ' + (summary.trade_activity_total !== undefined ? summary.trade_activity_total : 0) +
      ' | traded symbols=' + (summary.symbols_traded_count !== undefined ? summary.symbols_traded_count : 0));
    lines.push('Shortlist symbols: ' + (summary.symbols_shortlisted_count !== undefined ? summary.symbols_shortlisted_count : 0) +
      ' | paper open=' + (summary.paper_open_positions !== undefined ? summary.paper_open_positions : 0) +
      ' | real open=' + (summary.real_manual_open_positions !== undefined ? summary.real_manual_open_positions : 0));
    lines.push('Decisions: ' + (summary.decisions_total !== undefined ? summary.decisions_total : 0) +
      ' | outcomes=' + (summary.outcomes_total !== undefined ? summary.outcomes_total : 0));
    lines.push('Review tags: ' + (reviewTags.length || 0) +
      ' | by_type=' + JSON.stringify(summary.review_tag_counts || {}) +
      ' | by_severity=' + JSON.stringify(summary.review_tag_severity_counts || {}));
    lines.push('Top candidate symbols: ' + ((review.top_candidate_symbols || []).join(', ') || '-'));
    lines.push('Recent traded symbols: ' + ((review.recent_traded_symbols || []).join(', ') || '-'));
    var topItems = summary.top_review_items || [];
    if (topItems.length) {
      lines.push('Top review items: ' + topItems.map(function (item) {
        return [
          item.tag || '-',
          item.symbol || '-',
          'score=' + formatNumber(item.impact_score, 1)
        ].join('/');
      }).join(' | '));
    }
    lines.push('Entry review total: ' + (((entryReview.summary || {}).total) || 0) +
      ' | top=' + ((((entryReview.summary || {}).top_items || []).map(function (item) {
        return (item.tag || '-') + '/' + (item.symbol || '-') + '/score=' + formatNumber(item.impact_score, 1);
      })).join(' | ') || '-'));
    lines.push('Exit review total: ' + (((exitReview.summary || {}).total) || 0) +
      ' | top=' + ((((exitReview.summary || {}).top_items || []).map(function (item) {
        return (item.tag || '-') + '/' + (item.symbol || '-') + '/score=' + formatNumber(item.impact_score, 1);
      })).join(' | ') || '-'));
    lines.push('');
    lines.push(JSON.stringify(summary, null, 2));
    elements.reviewSummaryBlock.textContent = lines.join('\n');

    if (elements.reviewActivityBody) {
      var activityRows = (review.recent_trade_activity || []).slice(0, 40);
      if (!activityRows.length) {
        elements.reviewActivityBody.innerHTML = '<tr><td colspan="7" class="muted">No recent activity yet.</td></tr>';
      } else {
        var activityHtml = '';
        for (var i = 0; i < activityRows.length; i += 1) {
          var item = activityRows[i] || {};
          activityHtml += '<tr>' +
            '<td>' + escapeHtml(formatTs(item.ts_ms)) + '</td>' +
            '<td>' + escapeHtml(item.canonical_symbol || '-') + '</td>' +
            '<td>' + escapeHtml(item.activity_type || '-') + '</td>' +
            '<td>' + escapeHtml(item.pair_key || '-') + '</td>' +
            '<td>' + escapeHtml(item.direction || '-') + '</td>' +
            '<td>' + escapeHtml(item.source || '-') + '</td>' +
            '<td>' + escapeHtml(item.state_ref || '-') + '</td>' +
          '</tr>';
        }
        elements.reviewActivityBody.innerHTML = activityHtml;
      }
    }

    if (elements.reviewShortlistBody) {
      var shortlistRows = (review.shortlist_history || []).slice(0, 40);
      if (!shortlistRows.length) {
        elements.reviewShortlistBody.innerHTML = '<tr><td colspan="8" class="muted">No shortlist history yet.</td></tr>';
      } else {
        var shortlistHtml = '';
        for (var j = 0; j < shortlistRows.length; j += 1) {
          var row = shortlistRows[j] || {};
          shortlistHtml += '<tr>' +
            '<td>' + escapeHtml(formatTs(row.ts_ms)) + '</td>' +
            '<td>' + escapeHtml(row.canonical_symbol || '-') + '</td>' +
            '<td>' + escapeHtml(row.rank || '-') + '</td>' +
            '<td>' + formatNumber(row.candidate_score, 2) + '</td>' +
            '<td>' + formatNumber(row.entry_spread_pct, 4) + '</td>' +
            '<td>' + formatNumber(row.funding_edge_pct, 4) + '</td>' +
            '<td>' + escapeHtml(row.pair_key || '-') + '</td>' +
            '<td>' + escapeHtml((row.reason_codes || []).join(', ') || '-') + '</td>' +
          '</tr>';
        }
        elements.reviewShortlistBody.innerHTML = shortlistHtml;
      }
    }

    if (elements.reviewTagsBody) {
      var tagRows = reviewTags.slice(0, 40);
      if (!tagRows.length) {
        elements.reviewTagsBody.innerHTML = '<tr><td colspan="7" class="muted">No review tags yet.</td></tr>';
      } else {
        var tagHtml = '';
        for (var k = 0; k < tagRows.length; k += 1) {
          var tag = tagRows[k] || {};
          var ref = tag.state_ref || tag.decision_id || tag.pair_key || '-';
          tagHtml += '<tr>' +
            '<td>' + escapeHtml(formatTs(tag.ts_ms)) + '</td>' +
            '<td>' + escapeHtml(tag.tag || '-') + '</td>' +
            '<td>' + escapeHtml(tag.symbol || '-') + '</td>' +
            '<td>' + escapeHtml(tag.severity || '-') + '</td>' +
            '<td>' + formatNumber(tag.impact_score, 1) + '</td>' +
            '<td>' + escapeHtml(ref) + '</td>' +
            '<td>' + escapeHtml(tag.reason || '-') + '</td>' +
          '</tr>';
        }
        elements.reviewTagsBody.innerHTML = tagHtml;
      }
    }

    function renderReviewSlice(target, rows, emptyText) {
      if (!target) {
        return;
      }
      var list = (rows || []).slice(0, 20);
      if (!list.length) {
        target.innerHTML = '<tr><td colspan="6" class="muted">' + escapeHtml(emptyText) + '</td></tr>';
        return;
      }
      var html = '';
      for (var m = 0; m < list.length; m += 1) {
        var item = list[m] || {};
        var ref = item.state_ref || item.decision_id || item.pair_key || '-';
        html += '<tr>' +
          '<td>' + escapeHtml(formatTs(item.ts_ms)) + '</td>' +
          '<td>' + escapeHtml(item.tag || '-') + '</td>' +
          '<td>' + escapeHtml(item.symbol || '-') + '</td>' +
          '<td>' + formatNumber(item.impact_score, 1) + '</td>' +
          '<td>' + escapeHtml(ref) + '</td>' +
          '<td>' + escapeHtml(item.reason || '-') + '</td>' +
        '</tr>';
      }
      target.innerHTML = html;
    }

    renderReviewSlice(
      elements.entryReviewBody,
      entryReview.tags || [],
      'No entry review tags yet.'
    );
    renderReviewSlice(
      elements.exitReviewBody,
      exitReview.tags || [],
      'No exit review tags yet.'
    );

    function renderScorecardTable(target, rowsMap, emptyText, labelKey) {
      if (!target) {
        return;
      }
      var items = [];
      var source = rowsMap || {};
      for (var key in source) {
        if (Object.prototype.hasOwnProperty.call(source, key)) {
          items.push({ label: key, data: source[key] || {} });
        }
      }
      items.sort(function (left, right) {
        var a = left && left.data ? (left.data.total || 0) : 0;
        var b = right && right.data ? (right.data.total || 0) : 0;
        if (a !== b) {
          return b - a;
        }
        return String(left.label || '').localeCompare(String(right.label || ''));
      });
      if (!items.length) {
        target.innerHTML = '<tr><td colspan="5" class="muted">' + escapeHtml(emptyText) + '</td></tr>';
        return;
      }
      var html = '';
      for (var n = 0; n < items.length; n += 1) {
        var item = items[n] || {};
        var data = item.data || {};
        html += '<tr>' +
          '<td>' + escapeHtml(item.label || labelKey || '-') + '</td>' +
          '<td>' + escapeHtml(data.total || 0) + '</td>' +
          '<td>' + formatNumber(data.correct_rate_pct, 1) + '</td>' +
          '<td>' + formatNumber(data.avg_net_pnl_delta_pct, 4) + '</td>' +
          '<td>' + formatNumber(data.avg_alt_delta_pct, 4) + '</td>' +
        '</tr>';
      }
      target.innerHTML = html;
    }

    renderScorecardTable(
      elements.entryActionScorecardBody,
      (entryReview.summary || {}).action_scorecards || {},
      'No entry action scorecard yet.',
      'action'
    );
    renderScorecardTable(
      elements.exitActionScorecardBody,
      (exitReview.summary || {}).action_scorecards || {},
      'No exit action scorecard yet.',
      'action'
    );
    renderScorecardTable(
      elements.phaseScorecardBody,
      summary.phase_scorecards || {},
      'No phase scorecard yet.',
      'phase'
    );
  }

  function renderOutcomes(payload) {
    if (!elements.outcomesBlock) {
      return;
    }
    if (!payload || !payload.rows || !payload.rows.length) {
      if (elements.outcomesReviewBody) {
        elements.outcomesReviewBody.innerHTML = '<tr><td colspan="8" class="muted">No outcome rows yet.</td></tr>';
      }
      elements.outcomesBlock.textContent = 'No outcomes yet.';
      if (elements.operatorScorecardBlock) {
        elements.operatorScorecardBlock.textContent = 'No scorecard yet.';
      }
      return;
    }
    var rows = payload.rows || [];
    var summary = payload.summary || {};
    var rowsPreview = rows.slice(0, 200);
    if (elements.outcomesReviewBody) {
      var reviewHtml = '';
      for (var i = 0; i < rowsPreview.length; i += 1) {
        var row = rowsPreview[i] || {};
        var outcome = row.outcome || {};
        reviewHtml += '<tr>' +
          '<td>' + escapeHtml(row.decision_id || '-') + '</td>' +
          '<td>' + escapeHtml(row.action || '-') + '</td>' +
          '<td>' + escapeHtml(outcome.decision_phase || '-') + '</td>' +
          '<td>' + escapeHtml(row.horizon || '-') + '</td>' +
          '<td>' + escapeHtml(outcome.decision_correctness || '-') + '</td>' +
          '<td>' + escapeHtml(outcome.timing_quality || '-') + '</td>' +
          '<td>' + formatNumber(outcome.net_pnl_delta_pct, 4) + '</td>' +
          '<td>' + escapeHtml(formatTs(row.evaluated_at_ms)) + '</td>' +
        '</tr>';
      }
      elements.outcomesReviewBody.innerHTML = reviewHtml || '<tr><td colspan="8" class="muted">No outcome rows yet.</td></tr>';
    }
    var out = {
      symbol: payload.symbol,
      count: payload.count !== undefined ? payload.count : rows.length,
      summary: summary,
      rows_preview: rows.slice(0, 60)
    };
    elements.outcomesBlock.textContent = JSON.stringify(out, null, 2);
    if (elements.operatorScorecardBlock) {
      var scorecard = summary.operator_scorecard_pre_boundary || null;
      if (!scorecard || !scorecard.total_rows) {
        elements.operatorScorecardBlock.textContent = 'No pre-boundary outcomes yet.';
      } else {
        var traffic = scorecard.traffic_light || {};
        var overall = scorecard.overall || {};
        var lines = [];
        lines.push('Traffic light: ' + String(traffic.status || 'unknown').toUpperCase() +
          ' (score=' + (traffic.score !== undefined ? traffic.score : '-') + ')');
        lines.push('Known sample: ' + (overall.known_total !== undefined ? overall.known_total : '-') +
          ' | hit=' + (overall.hit_rate_pct !== undefined && overall.hit_rate_pct !== null ? formatNumber(overall.hit_rate_pct, 1) + '%' : '-') +
          ' | wrong=' + (overall.wrong_rate_pct !== undefined && overall.wrong_rate_pct !== null ? formatNumber(overall.wrong_rate_pct, 1) + '%' : '-'));
        lines.push('Timing: wait_help=' +
          (overall.wait_help_rate_pct !== undefined && overall.wait_help_rate_pct !== null ? formatNumber(overall.wait_help_rate_pct, 1) + '%' : '-') +
          ' | early_exit_help=' +
          (overall.early_exit_help_rate_pct !== undefined && overall.early_exit_help_rate_pct !== null ? formatNumber(overall.early_exit_help_rate_pct, 1) + '%' : '-'));
        if (traffic.reasons && traffic.reasons.length) {
          lines.push('Reasons: ' + traffic.reasons.join(', '));
        }
        lines.push('');
        lines.push(JSON.stringify(scorecard, null, 2));
        elements.operatorScorecardBlock.textContent = lines.join('\n');
      }
    }
  }

  function renderOutcomesAutoStatus(payload) {
    if (elements.outcomesAutoHealth) {
      elements.outcomesAutoHealth.textContent = '';
      elements.outcomesAutoHealth.className = 'settings-status';
    }
    if (!elements.outcomesAutoStatusBlock) {
      return;
    }
    if (!payload) {
      elements.outcomesAutoStatusBlock.textContent = 'No auto evaluator status yet.';
      return;
    }
    var lastCycle = payload.last_cycle || {};
    var symbolPending = payload.symbol_pending || {};
    var recentCycles = payload.recent_cycles || [];
    var health = payload.health || {};
    var healthStatus = String(health.status || '').toLowerCase();
    var healthReasons = health.reasons || [];
    var schedulerEnabled = payload.scheduler_enabled !== false;
    if (elements.outcomesAutoPauseBtn) {
      elements.outcomesAutoPauseBtn.disabled = !schedulerEnabled;
    }
    if (elements.outcomesAutoResumeBtn) {
      elements.outcomesAutoResumeBtn.disabled = schedulerEnabled;
    }
    if (elements.outcomesAutoHealth) {
      var healthText = 'Health: ' + (healthStatus || 'unknown').toUpperCase();
      if (healthReasons.length) {
        healthText += ' (' + healthReasons.join(', ') + ')';
      }
      elements.outcomesAutoHealth.textContent = healthText;
      if (healthStatus === 'healthy') {
        elements.outcomesAutoHealth.className = 'settings-status settings-status--success';
      } else if (healthStatus === 'warn') {
        elements.outcomesAutoHealth.className = 'settings-status settings-status--info';
      } else if (healthStatus === 'stale') {
        elements.outcomesAutoHealth.className = 'settings-status settings-status--error';
      }
    }
    var lines = [];
    lines.push('Health: ' + (healthStatus || 'unknown').toUpperCase() +
      (healthReasons.length ? ' | reasons=' + healthReasons.join(', ') : ''));
    lines.push('Scheduler enabled: ' + (schedulerEnabled ? 'yes' : 'no'));
    lines.push('Scheduler: ' + (payload.scheduler_running ? 'running' : 'stopped') +
      ' | poll=' + (payload.poll_sec !== undefined ? payload.poll_sec : '-') + 's');
    lines.push('Auto horizons: ' + ((payload.auto_horizons || []).join(', ') || '-'));
    if (lastCycle.ts_ms) {
      lines.push(
        'Last cycle: ' + formatTs(lastCycle.ts_ms) +
        ' | age=' + (payload.last_cycle_age_sec !== undefined && payload.last_cycle_age_sec !== null ?
          formatNumber(payload.last_cycle_age_sec, 1) + 's' : '-') +
        ' | symbols=' + (lastCycle.symbols_processed !== undefined ? lastCycle.symbols_processed : '-') +
        '/' + (lastCycle.symbols_total !== undefined ? lastCycle.symbols_total : '-') +
        ' | evaluated=' + (lastCycle.evaluated !== undefined ? lastCycle.evaluated : 0) +
        ' | skipped=' + (lastCycle.skipped !== undefined ? lastCycle.skipped : 0) +
        ' | deferred=' + (lastCycle.deferred !== undefined ? lastCycle.deferred : 0) +
        ' | errors=' + (lastCycle.errors !== undefined ? lastCycle.errors : 0)
      );
    } else {
      lines.push('Last cycle: not yet');
    }
    if (payload.symbol) {
      lines.push('Symbol: ' + payload.symbol +
        ' | decisions=' + (symbolPending.decisions_total !== undefined ? symbolPending.decisions_total : 0) +
        ' | missing=' + (symbolPending.missing_total !== undefined ? symbolPending.missing_total : 0));
    }
    if (recentCycles.length) {
      var tail = recentCycles.slice(Math.max(0, recentCycles.length - 3));
      var compact = [];
      for (var i = 0; i < tail.length; i += 1) {
        var c = tail[i] || {};
        compact.push(
          formatTs(c.ts_ms) + ':e=' + (c.evaluated || 0) +
          '/s=' + (c.skipped || 0) +
          '/d=' + (c.deferred || 0) +
          '/err=' + (c.errors || 0) +
          (c.scope_symbol ? '/sym=' + c.scope_symbol : '')
        );
      }
      lines.push('Recent cycles: ' + compact.join(' | '));
    }
    lines.push('');
    lines.push(JSON.stringify(payload, null, 2));
    elements.outcomesAutoStatusBlock.textContent = lines.join('\n');
  }

  function refreshOutcomesAutoStatus(callback) {
    var symbol = (elements.symbolInput && elements.symbolInput.value ? elements.symbolInput.value : '').trim().toUpperCase();
    requestOutcomesAutoStatus(symbol, function (err, payload) {
      if (err) {
        renderOutcomesAutoStatus({
          scheduler_running: false,
          error: err.message || 'auto_status_request_failed'
        });
        if (callback) {
          callback(err);
        }
        return;
      }
      renderOutcomesAutoStatus(payload);
      if (callback) {
        callback(null, payload);
      }
    });
  }

  function startOutcomesAutoStatusPolling() {
    if (state.outcomesAutoStatusTimer) {
      clearInterval(state.outcomesAutoStatusTimer);
      state.outcomesAutoStatusTimer = null;
    }
    refreshOutcomesAutoStatus(null);
    state.outcomesAutoStatusTimer = setInterval(function () {
      refreshOutcomesAutoStatus(null);
    }, 15000);
  }

  function renderRetentionStatus(payload) {
    if (!elements.retentionBlock) {
      return;
    }
    if (!payload) {
      elements.retentionBlock.textContent = 'No retention status yet.';
      return;
    }
    var retention = payload.retention || {};
    var counts = payload.table_counts || {};
    var lines = [];
    lines.push(
      'Retention scheduler: ' + (retention.scheduler_running ? 'running' : 'stopped') +
      ' | poll=' + (retention.poll_sec !== undefined ? retention.poll_sec : '-') + 's'
    );
    lines.push(
      'Defaults: max_age_days=' + (retention.max_age_days !== undefined ? retention.max_age_days : '-') +
      ' | closed_paper_days=' + (retention.closed_paper_days !== undefined ? retention.closed_paper_days : '-')
    );
    var report = retention.last_report || {};
    if (report.ts_ms) {
      lines.push(
        'Last run: ' + formatTs(report.ts_ms) +
        ' | reason=' + (report.reason || '-') +
        ' | total_deleted=' + ((report.deleted || {}).total_deleted !== undefined ? (report.deleted || {}).total_deleted : 0)
      );
    } else {
      lines.push('Last run: not yet');
    }
    lines.push('');
    lines.push('Table counts: ' + JSON.stringify(counts));
    lines.push('');
    lines.push(JSON.stringify(payload, null, 2));
    elements.retentionBlock.textContent = lines.join('\n');
  }

  function refreshRetentionStatus(callback) {
    requestRetentionStatus(function (err, payload) {
      if (err) {
        renderRetentionStatus({ error: err.message || 'retention_status_failed' });
        if (callback) {
          callback(err);
        }
        return;
      }
      renderRetentionStatus(payload);
      if (callback) {
        callback(null, payload);
      }
    });
  }

  function startRetentionStatusPolling() {
    if (state.retentionStatusTimer) {
      clearInterval(state.retentionStatusTimer);
      state.retentionStatusTimer = null;
    }
    refreshRetentionStatus(null);
    state.retentionStatusTimer = setInterval(function () {
      refreshRetentionStatus(null);
    }, 30000);
  }

  function loadOutcomes(symbol, limit, filters, callback) {
    requestOutcomes(symbol, limit, filters, function (err, payload) {
      if (err) {
        setOutcomesStatus(err.message || 'outcomes_request_failed', true);
        renderOutcomes(null);
        refreshOutcomesAutoStatus(null);
        if (callback) {
          callback(err);
        }
        return;
      }
      var filterInfo = payload && payload.filters ? payload.filters : {};
      var suffix = '';
      if ((filterInfo.horizons && filterInfo.horizons.length) ||
          (filterInfo.phase_buckets && filterInfo.phase_buckets.length) ||
          (filterInfo.actions && filterInfo.actions.length)) {
        suffix = ' (filtered)';
      }
      setOutcomesStatus('Loaded outcomes: ' + (payload && payload.count ? payload.count : 0) + suffix, false);
      renderOutcomes(payload);
      refreshOutcomesAutoStatus(null);
      if (callback) {
        callback(null, payload);
      }
    });
  }

  function _buildPaperActionButtons(positionKey, status) {
    var view = '<button type="button" class="button" data-paper-open-events="1" data-position-key="' + escapeHtml(positionKey) + '">Events</button>';
    if (status !== 'open') {
      return view;
    }
    return [
      view,
      '<button type="button" class="button" data-paper-action="HOLD" data-position-key="' + escapeHtml(positionKey) + '">HOLD</button>',
      '<button type="button" class="button" data-paper-action="ADD_SMALL" data-position-key="' + escapeHtml(positionKey) + '">ADD</button>',
      '<button type="button" class="button" data-paper-action="PARTIAL_EXIT" data-position-key="' + escapeHtml(positionKey) + '">PARTIAL EXIT</button>',
      '<button type="button" class="button" data-paper-action="FULL_EXIT" data-position-key="' + escapeHtml(positionKey) + '">FULL EXIT</button>'
    ].join(' ');
  }

  function renderPaperPositions(payload) {
    if (!elements.paperPositionsBody) {
      return;
    }
    var rows = payload && payload.rows ? payload.rows : [];
    if (!rows.length) {
      elements.paperPositionsBody.innerHTML = '<tr><td colspan="7" class="muted">No paper positions yet.</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var positionKey = String(row.position_key || '');
      var status = String(row.status || '-');
      html += '<tr>' +
        '<td>' + escapeHtml(positionKey) + '</td>' +
        '<td>' + escapeHtml(status) + '</td>' +
        '<td>' + escapeHtml(row.pair_key || '-') + '</td>' +
        '<td>' + escapeHtml(row.direction || '-') + '</td>' +
        '<td>' + formatNumber(row.qty, 6) + '</td>' +
        '<td>' + formatTs(row.updated_at_ms) + '</td>' +
        '<td>' + _buildPaperActionButtons(positionKey, status) + '</td>' +
      '</tr>';
    }
    elements.paperPositionsBody.innerHTML = html;
  }

  function renderPaperEvents(payload) {
    if (!elements.paperEventsBlock || !elements.paperEventsTitle) {
      return;
    }
    var rows = payload && payload.rows ? payload.rows : [];
    var positionKey = payload && payload.position_key ? payload.position_key : '';
    elements.paperEventsTitle.textContent = positionKey ? ('Events for ' + positionKey) : 'Select/open a paper position to view events.';
    if (!rows.length) {
      elements.paperEventsBlock.textContent = 'No events.';
      return;
    }
    var lines = [];
    for (var i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      lines.push(
        '[' + formatTs(row.ts_ms) + '] ' +
        (row.event_type || '-') +
        ' | ' + JSON.stringify(row.payload || {})
      );
    }
    elements.paperEventsBlock.textContent = lines.join('\n');
  }

  function loadPaperEvents(positionKey) {
    if (!positionKey) {
      renderPaperEvents(null);
      return;
    }
    state.selectedPaperPositionKey = positionKey;
    requestPaperEvents(positionKey, function (err, payload) {
      if (err) {
        renderPaperEvents({
          position_key: positionKey,
          rows: [
            {
              ts_ms: Date.now(),
              event_type: 'error',
              payload: { message: err.message || 'events_request_failed' }
            }
          ]
        });
        return;
      }
      renderPaperEvents(payload);
    });
  }

  function refreshPaperData(symbol, preferredPositionKey) {
    requestPaperPositions(symbol, function (err, payload) {
      if (err) {
        setPaperStatus(err.message || 'paper positions request failed', true);
        renderPaperPositions(null);
        return;
      }
      renderPaperPositions(payload);
      var rows = payload && payload.rows ? payload.rows : [];
      var target = preferredPositionKey || state.selectedPaperPositionKey || '';
      if (!target && rows.length) {
        target = rows[0].position_key || '';
      }
      if (target) {
        loadPaperEvents(target);
      } else {
        renderPaperEvents(null);
      }
    });
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

  function renderChart(svg, points, config) {
    if (!svg) {
      return;
    }
    while (svg.firstChild) {
      svg.removeChild(svg.firstChild);
    }
    var rows = Array.isArray(points) ? points.filter(Boolean) : [];
    if (!rows.length) {
      var empty = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      empty.setAttribute('x', '16');
      empty.setAttribute('y', '28');
      empty.setAttribute('fill', '#7c8aa0');
      empty.setAttribute('font-size', '12');
      empty.textContent = 'No data';
      svg.appendChild(empty);
      return;
    }

    var width = 560;
    var height = 180;
    var padLeft = 36;
    var padRight = 10;
    var padTop = 12;
    var padBottom = 22;
    var valueKey = config && config.valueKey ? config.valueKey : 'value';
    var values = [];
    var tsValues = [];
    for (var i = 0; i < rows.length; i += 1) {
      var ts = Number(rows[i].ts_ms);
      var value = Number(rows[i][valueKey]);
      if (isNaN(ts) || isNaN(value)) {
        continue;
      }
      tsValues.push(ts);
      values.push(value);
    }
    if (!values.length) {
      var emptyVals = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      emptyVals.setAttribute('x', '16');
      emptyVals.setAttribute('y', '28');
      emptyVals.setAttribute('fill', '#7c8aa0');
      emptyVals.setAttribute('font-size', '12');
      emptyVals.textContent = 'No usable points';
      svg.appendChild(emptyVals);
      return;
    }

    var minTs = Math.min.apply(null, tsValues);
    var maxTs = Math.max.apply(null, tsValues);
    var minVal = Math.min.apply(null, values);
    var maxVal = Math.max.apply(null, values);
    if (minVal === maxVal) {
      minVal -= 1;
      maxVal += 1;
    }
    var rangeTs = Math.max(1, maxTs - minTs);
    var rangeVal = Math.max(1e-9, maxVal - minVal);
    var plotWidth = width - padLeft - padRight;
    var plotHeight = height - padTop - padBottom;
    var zeroY = padTop + (maxVal / rangeVal) * plotHeight;

    var grid = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    grid.setAttribute('class', 'chart-gridlines');
    for (var j = 0; j < 3; j += 1) {
      var line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      var y = padTop + (plotHeight / 2) * j;
      line.setAttribute('x1', String(padLeft));
      line.setAttribute('x2', String(width - padRight));
      line.setAttribute('y1', String(y));
      line.setAttribute('y2', String(y));
      line.setAttribute('stroke', 'rgba(148, 163, 184, 0.18)');
      line.setAttribute('stroke-width', '1');
      grid.appendChild(line);
    }
    svg.appendChild(grid);

    if (minVal < 0 && maxVal > 0) {
      var zero = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      zero.setAttribute('x1', String(padLeft));
      zero.setAttribute('x2', String(width - padRight));
      zero.setAttribute('y1', String(zeroY));
      zero.setAttribute('y2', String(zeroY));
      zero.setAttribute('stroke', 'rgba(255,255,255,0.22)');
      zero.setAttribute('stroke-width', '1');
      svg.appendChild(zero);
    }

    var pathPoints = [];
    for (var k = 0; k < rows.length; k += 1) {
      var row = rows[k] || {};
      var rowTs = Number(row.ts_ms);
      var rowVal = Number(row[valueKey]);
      if (isNaN(rowTs) || isNaN(rowVal)) {
        continue;
      }
      var x = padLeft + ((rowTs - minTs) / rangeTs) * plotWidth;
      var yPos = padTop + ((maxVal - rowVal) / rangeVal) * plotHeight;
      pathPoints.push(String(x.toFixed(1)) + ',' + String(yPos.toFixed(1)));
    }

    var polyline = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
    polyline.setAttribute('fill', 'none');
    polyline.setAttribute('stroke', config && config.stroke ? config.stroke : '#59c3c3');
    polyline.setAttribute('stroke-width', '2.2');
    polyline.setAttribute('points', pathPoints.join(' '));
    svg.appendChild(polyline);

    var last = rows[rows.length - 1] || {};
    var lastDot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    var lastX = padLeft + ((Number(last.ts_ms) - minTs) / rangeTs) * plotWidth;
    var lastY = padTop + ((maxVal - Number(last[valueKey])) / rangeVal) * plotHeight;
    lastDot.setAttribute('cx', String(lastX.toFixed(1)));
    lastDot.setAttribute('cy', String(lastY.toFixed(1)));
    lastDot.setAttribute('r', '3.5');
    lastDot.setAttribute('fill', config && config.stroke ? config.stroke : '#59c3c3');
    svg.appendChild(lastDot);

    var topLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    topLabel.setAttribute('x', '6');
    topLabel.setAttribute('y', '16');
    topLabel.setAttribute('fill', '#9aa5b1');
    topLabel.setAttribute('font-size', '11');
    topLabel.textContent = formatSignedNumber(maxVal, 3, config && config.suffix ? config.suffix : '');
    svg.appendChild(topLabel);

    var bottomLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    bottomLabel.setAttribute('x', '6');
    bottomLabel.setAttribute('y', String(height - 8));
    bottomLabel.setAttribute('fill', '#9aa5b1');
    bottomLabel.setAttribute('font-size', '11');
    bottomLabel.textContent = formatSignedNumber(minVal, 3, config && config.suffix ? config.suffix : '');
    svg.appendChild(bottomLabel);

    var startLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    startLabel.setAttribute('x', String(padLeft));
    startLabel.setAttribute('y', String(height - 6));
    startLabel.setAttribute('fill', '#7c8aa0');
    startLabel.setAttribute('font-size', '11');
    startLabel.textContent = formatTs(minTs);
    svg.appendChild(startLabel);

    var endLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    endLabel.setAttribute('x', String(width - padRight));
    endLabel.setAttribute('y', String(height - 6));
    endLabel.setAttribute('text-anchor', 'end');
    endLabel.setAttribute('fill', '#7c8aa0');
    endLabel.setAttribute('font-size', '11');
    endLabel.textContent = formatTs(maxTs);
    svg.appendChild(endLabel);
  }

  function renderVisualAnalysis(visual) {
    if (!elements.visualSummary || !elements.visualWindowBody) {
      return;
    }
    if (!visual || !visual.pair_key) {
      elements.visualSummary.textContent = 'No visual summary yet.';
      elements.visualWindowBody.innerHTML = '<tr><td colspan="10" class="muted">Run an analysis to populate data.</td></tr>';
      if (elements.visualNotes) {
        elements.visualNotes.innerHTML = '';
      }
      renderChart(elements.visualSpreadChart, [], { valueKey: 'spread_pct', suffix: '%' });
      renderChart(elements.visualFundingChart, [], { valueKey: 'net_bps', suffix: ' bps' });
      return;
    }

    var summary = visual.summary || {};
    var summaryParts = [
      (visual.pair_label || '-') + ' | ' + (visual.direction_label || '-'),
      'action=' + (visual.selected_action || '-'),
      'recommendation=' + (visual.recommendation || '-'),
      'score=' + formatNumber(visual.score, 1),
      'spread now=' + formatSignedNumber(summary.spread_current_pct, 4, '%'),
      'funding/hour=' + formatSignedNumber(summary.funding_net_hourly_bps, 3, ' bps')
    ];
    elements.visualSummary.textContent = summaryParts.join(' | ');

    var rows = visual.windows || [];
    if (!rows.length) {
      elements.visualWindowBody.innerHTML = '<tr><td colspan="10" class="muted">No normalized windows yet.</td></tr>';
    } else {
      var html = '';
      for (var i = 0; i < rows.length; i += 1) {
        var row = rows[i] || {};
        html += '<tr>' +
          '<td>' + escapeHtml(row.label || '-') + '</td>' +
          '<td class="' + valueToneClass(row.funding_net_bps) + '">' + formatSignedNumber(row.funding_net_bps, 2, '') + '</td>' +
          '<td class="' + valueToneClass(row.funding_avg_hourly_bps) + '">' + formatSignedNumber(row.funding_avg_hourly_bps, 3, '') + '</td>' +
          '<td>' + formatNumber(row.funding_positive_share_pct, 1) + '</td>' +
          '<td class="' + valueToneClass(row.spread_current_pct) + '">' + formatSignedNumber(row.spread_current_pct, 4, '%') + '</td>' +
          '<td class="' + valueToneClass(row.spread_mean_pct) + '">' + formatSignedNumber(row.spread_mean_pct, 4, '%') + '</td>' +
          '<td>' + formatNumber(row.spread_p95_abs_pct, 4) + '</td>' +
          '<td>' + escapeHtml(row.spread_points || 0) + '</td>' +
          '<td>' + escapeHtml(row.funding_points || 0) + '</td>' +
          '<td>' + escapeHtml(row.signal || '-') + '</td>' +
        '</tr>';
      }
      elements.visualWindowBody.innerHTML = html;
    }

    if (elements.visualNotes) {
      var notes = Array.isArray(visual.notes) ? visual.notes : [];
      var notesHtml = '';
      for (var j = 0; j < notes.length; j += 1) {
        notesHtml += '<div class="grid-card"><p class="muted" style="margin:0;">' + escapeHtml(notes[j]) + '</p></div>';
      }
      elements.visualNotes.innerHTML = notesHtml;
    }

    renderChart(
      elements.visualSpreadChart,
      (visual.charts && visual.charts.spread && visual.charts.spread.points) || [],
      { valueKey: 'spread_pct', stroke: '#57d3bc', suffix: '%' }
    );
    renderChart(
      elements.visualFundingChart,
      (visual.charts && visual.charts.funding && visual.charts.funding.points) || [],
      { valueKey: 'net_bps', stroke: '#f2b84b', suffix: ' bps' }
    );
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

  function syncPaperDefaultsFromAnalysis(payload) {
    if (!payload) {
      return;
    }
    var logic = payload.bot_logic || {};
    var pair = logic.recommended_pair || {};
    if (elements.paperPairInput && pair.pair_key) {
      elements.paperPairInput.value = pair.pair_key;
    }
    if (elements.paperDirectionInput && pair.direction) {
      elements.paperDirectionInput.value = pair.direction;
    }
    if (elements.paperActionInput) {
      var action = logic.recommended_action || pair.action;
      if (action === 'ENTRY_SMALL' || action === 'ENTRY_STRONG') {
        elements.paperActionInput.value = action;
      }
    }
  }

  function renderAnalysisPayload(symbol, windowMinutes, fundingPoints, payload) {
    state.lastAnalysis = payload || null;
    updateHeader(symbol, windowMinutes, fundingPoints, payload && payload.requested_at);
    renderExchangeTable(payload && payload.exchanges, fundingPoints);
    renderPairTable(payload && payload.pair_analysis);
    renderBotLogic(payload && payload.bot_logic);
    renderVisualAnalysis(payload && payload.visual_analysis);
    renderDecisionJournal(payload && payload.decision_journal);
    renderPositionLogic(payload && payload.position_logic);
    renderFundingHistory(payload && payload.exchanges, windowMinutes);
    renderCandles(payload && payload.exchanges);
    syncPaperDefaultsFromAnalysis(payload);
  }

  function runAnalysis(callback) {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    var windowMinutes = parseInt(elements.windowInput.value || defaultWindow, 10);
    var fundingPoints = parseInt(elements.fundingInput.value || defaultFunding, 10);
    if (!symbol) {
      setStatus('Enter symbol.', true);
      if (callback) {
        callback(new Error('symbol_required'));
      }
      return;
    }
    setStatus('Fetching historical data...', false);
    elements.submit.disabled = true;

    requestAnalysis(symbol, windowMinutes, fundingPoints, function (err, payload) {
      elements.submit.disabled = false;
      if (err) {
        setStatus(err.message || 'Error fetching analysis', true);
        if (callback) {
          callback(err);
        }
        return;
      }
      setStatus('Updated', false);
      renderAnalysisPayload(symbol, windowMinutes, fundingPoints, payload);
      refreshPaperData(symbol, null);
      handleReviewLoadClick();
      var outcomesLimit = elements.outcomesLimitInput && elements.outcomesLimitInput.value ? elements.outcomesLimitInput.value : '500';
      loadOutcomes(symbol, outcomesLimit, readOutcomesFilters(), null);
      if (callback) {
        callback(null, payload);
      }
    });
  }

  function handleSubmit(event) {
    if (event) {
      event.preventDefault();
    }
    runAnalysis(null);
  }

  function handlePaperEnter(event) {
    if (event) {
      event.preventDefault();
    }
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    var qty = parseFloat(elements.paperQtyInput && elements.paperQtyInput.value ? elements.paperQtyInput.value : '');
    if (!symbol) {
      setPaperStatus('Symbol is required.', true);
      return;
    }
    if (!qty || qty <= 0) {
      setPaperStatus('Qty must be > 0.', true);
      return;
    }
    var payload = {
      symbol: symbol,
      qty: qty
    };
    if (elements.paperPairInput && elements.paperPairInput.value) {
      payload.pair_key = elements.paperPairInput.value.trim();
    }
    if (elements.paperDirectionInput && elements.paperDirectionInput.value) {
      payload.direction = elements.paperDirectionInput.value;
    }
    if (elements.paperActionInput && elements.paperActionInput.value) {
      payload.action = elements.paperActionInput.value;
    }
    setPaperStatus('Creating paper position...', false);
    if (elements.paperEnterSubmit) {
      elements.paperEnterSubmit.disabled = true;
    }
    requestPaperEnter(payload, function (err, result) {
      if (elements.paperEnterSubmit) {
        elements.paperEnterSubmit.disabled = false;
      }
      if (err) {
        setPaperStatus(err.message || 'paper_enter_failed', true);
        return;
      }
      setPaperStatus('Paper position created: ' + (result && result.position_key ? result.position_key : '-'), false);
      refreshPaperData(symbol, result && result.position_key);
      runAnalysis(null);
    });
  }

  function handlePaperTableClick(event) {
    var target = event && event.target ? event.target : null;
    if (!target) {
      return;
    }
    var positionKey = target.getAttribute('data-position-key');
    if (!positionKey) {
      return;
    }
    var openEvents = target.getAttribute('data-paper-open-events');
    if (openEvents) {
      loadPaperEvents(positionKey);
      return;
    }
    var action = target.getAttribute('data-paper-action');
    if (!action) {
      return;
    }
    setPaperStatus('Applying action ' + action + '...', false);
    requestPaperAction(
      {
        position_key: positionKey,
        action: action
      },
      function (err, payload) {
        if (err) {
          setPaperStatus(err.message || 'paper_action_failed', true);
          return;
        }
        setPaperStatus('Applied ' + action + ' for ' + positionKey, false);
        var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
        refreshPaperData(symbol, positionKey);
        loadPaperEvents(positionKey);
        runAnalysis(null);
        if (payload && payload.status === 'closed') {
          setPaperStatus('Position closed: ' + positionKey, false);
        }
      }
    );
  }

  function handleReplayClick() {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    if (!symbol) {
      setReplayStatus('Symbol is required.', true);
      return;
    }
    var limit = elements.replayLimitInput && elements.replayLimitInput.value ? elements.replayLimitInput.value : '500';
    setReplayStatus('Running replay...', false);
    if (elements.replayRunBtn) {
      elements.replayRunBtn.disabled = true;
    }
    requestReplay(symbol, limit, function (err, payload) {
      if (elements.replayRunBtn) {
        elements.replayRunBtn.disabled = false;
      }
      if (err) {
        setReplayStatus(err.message || 'replay_failed', true);
        renderReplay(null);
        return;
      }
      setReplayStatus('Replay ready: ' + (payload && payload.replay_points ? payload.replay_points : 0) + ' points', false);
      renderReplay(payload);
    });
  }

  function handleReviewLoadClick() {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    var days = elements.reviewDaysInput && elements.reviewDaysInput.value ? elements.reviewDaysInput.value : '7';
    var top = elements.reviewTopInput && elements.reviewTopInput.value ? elements.reviewTopInput.value : '3';
    setReviewStatus('Loading weekly review...', false);
    if (elements.reviewLoadBtn) {
      elements.reviewLoadBtn.disabled = true;
    }
    requestWeeklyReview(symbol, days, top, function (err, payload) {
      if (elements.reviewLoadBtn) {
        elements.reviewLoadBtn.disabled = false;
      }
      if (err) {
        setReviewStatus(err.message || 'review_load_failed', true);
        renderWeeklyReview(null);
        return;
      }
      var review = payload && payload.review ? payload.review : payload;
      var total = review && review.summary ? review.summary.trade_activity_total : 0;
      setReviewStatus('Weekly review loaded: activity=' + (total || 0), false);
      renderWeeklyReview(payload);
    });
  }

  function handleOutcomesClick() {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    if (!symbol) {
      setOutcomesStatus('Symbol is required.', true);
      return;
    }
    var horizons = elements.outcomesHorizonsInput && elements.outcomesHorizonsInput.value ?
      elements.outcomesHorizonsInput.value : '15m,1h,4h';
    var limit = elements.outcomesLimitInput && elements.outcomesLimitInput.value ?
      elements.outcomesLimitInput.value : '500';
    var filters = readOutcomesFilters();
    setOutcomesStatus('Evaluating outcomes...', false);
    if (elements.outcomesRunBtn) {
      elements.outcomesRunBtn.disabled = true;
    }
    requestEvaluateOutcomes(symbol, horizons, limit, false, function (err, payload) {
      if (elements.outcomesRunBtn) {
        elements.outcomesRunBtn.disabled = false;
      }
      if (err) {
        setOutcomesStatus(err.message || 'outcomes_evaluation_failed', true);
        refreshOutcomesAutoStatus(null);
        return;
      }
      var evaluated = payload && payload.evaluated ? payload.evaluated : 0;
      var skipped = payload && payload.skipped ? payload.skipped : 0;
      var deferred = payload && payload.deferred ? payload.deferred : 0;
      setOutcomesStatus(
        'Outcome evaluation complete: evaluated=' + evaluated + ', skipped=' + skipped + ', deferred=' + deferred,
        false
      );
      refreshOutcomesAutoStatus(null);
      loadOutcomes(symbol, limit, filters, null);
    });
  }

  function handleOutcomesLoadClick() {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    if (!symbol) {
      setOutcomesStatus('Symbol is required.', true);
      return;
    }
    var limit = elements.outcomesLimitInput && elements.outcomesLimitInput.value ?
      elements.outcomesLimitInput.value : '500';
    setOutcomesStatus('Loading outcomes...', false);
    if (elements.outcomesLoadBtn) {
      elements.outcomesLoadBtn.disabled = true;
    }
    loadOutcomes(symbol, limit, readOutcomesFilters(), function () {
      if (elements.outcomesLoadBtn) {
        elements.outcomesLoadBtn.disabled = false;
      }
    });
  }

  function handleOutcomesAutoRunClick() {
    var symbol = (elements.symbolInput.value || '').trim().toUpperCase();
    if (!symbol) {
      setOutcomesStatus('Symbol is required.', true);
      return;
    }
    setOutcomesStatus('Running auto cycle once...', false);
    if (elements.outcomesAutoRunBtn) {
      elements.outcomesAutoRunBtn.disabled = true;
    }
    requestOutcomesAutoRun(symbol, function (err, payload) {
      if (elements.outcomesAutoRunBtn) {
        elements.outcomesAutoRunBtn.disabled = false;
      }
      if (err) {
        setOutcomesStatus(err.message || 'outcomes_auto_run_failed', true);
        refreshOutcomesAutoStatus(null);
        return;
      }
      var cycle = payload && payload.cycle ? payload.cycle : {};
      setOutcomesStatus(
        'Auto cycle done: evaluated=' + (cycle.evaluated || 0) +
        ', skipped=' + (cycle.skipped || 0) +
        ', deferred=' + (cycle.deferred || 0) +
        ', errors=' + (cycle.errors || 0),
        false
      );
      if (payload && payload.status) {
        renderOutcomesAutoStatus(payload.status);
      } else {
        refreshOutcomesAutoStatus(null);
      }
      var limit = elements.outcomesLimitInput && elements.outcomesLimitInput.value ?
        elements.outcomesLimitInput.value : '500';
      loadOutcomes(symbol, limit, readOutcomesFilters(), null);
    });
  }

  function handleOutcomesAutoSchedulerClick(enabled) {
    setOutcomesStatus((enabled ? 'Resuming' : 'Pausing') + ' auto scheduler...', false);
    if (elements.outcomesAutoPauseBtn) {
      elements.outcomesAutoPauseBtn.disabled = true;
    }
    if (elements.outcomesAutoResumeBtn) {
      elements.outcomesAutoResumeBtn.disabled = true;
    }
    requestOutcomesAutoScheduler(enabled, function (err, payload) {
      if (err) {
        setOutcomesStatus(err.message || 'outcomes_auto_scheduler_failed', true);
        refreshOutcomesAutoStatus(null);
        return;
      }
      setOutcomesStatus('Auto scheduler ' + (enabled ? 'resumed' : 'paused'), false);
      renderOutcomesAutoStatus(payload);
    });
  }

  function handleRetentionRunClick() {
    var maxAge = elements.retentionMaxAgeInput && elements.retentionMaxAgeInput.value ? elements.retentionMaxAgeInput.value : '';
    var closedPaper = elements.retentionClosedPaperInput && elements.retentionClosedPaperInput.value ? elements.retentionClosedPaperInput.value : '';
    setRetentionStatus('Running retention...', false);
    if (elements.retentionRunBtn) {
      elements.retentionRunBtn.disabled = true;
    }
    requestRetentionRun(maxAge, closedPaper, function (err, payload) {
      if (elements.retentionRunBtn) {
        elements.retentionRunBtn.disabled = false;
      }
      if (err) {
        setRetentionStatus(err.message || 'retention_run_failed', true);
        refreshRetentionStatus(null);
        return;
      }
      var deleted = payload && payload.deleted ? payload.deleted : {};
      setRetentionStatus('Retention done: deleted=' + (deleted.total_deleted || 0), false);
      refreshRetentionStatus(null);
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
    if (elements.paperForm) {
      elements.paperForm.addEventListener('submit', handlePaperEnter);
    }
    if (elements.paperPositionsBody) {
      elements.paperPositionsBody.addEventListener('click', handlePaperTableClick);
    }
    if (elements.exportJsonBtn) {
      elements.exportJsonBtn.addEventListener('click', function () {
        triggerExport('json');
      });
    }
    if (elements.exportCsvBtn) {
      elements.exportCsvBtn.addEventListener('click', function () {
        triggerExport('csv');
      });
    }
    if (elements.exportParquetBtn) {
      elements.exportParquetBtn.addEventListener('click', function () {
        triggerExport('parquet');
      });
    }
    if (elements.reviewLoadBtn) {
      elements.reviewLoadBtn.addEventListener('click', handleReviewLoadClick);
    }
    if (elements.reviewExportJsonBtn) {
      elements.reviewExportJsonBtn.addEventListener('click', function () {
        triggerReviewExport('json');
      });
    }
    if (elements.reviewExportCsvBtn) {
      elements.reviewExportCsvBtn.addEventListener('click', function () {
        triggerReviewExport('csv');
      });
    }
    if (elements.replayRunBtn) {
      elements.replayRunBtn.addEventListener('click', handleReplayClick);
    }
    if (elements.outcomesRunBtn) {
      elements.outcomesRunBtn.addEventListener('click', handleOutcomesClick);
    }
    if (elements.outcomesLoadBtn) {
      elements.outcomesLoadBtn.addEventListener('click', handleOutcomesLoadClick);
    }
    if (elements.outcomesAutoRunBtn) {
      elements.outcomesAutoRunBtn.addEventListener('click', handleOutcomesAutoRunClick);
    }
    if (elements.outcomesAutoPauseBtn) {
      elements.outcomesAutoPauseBtn.addEventListener('click', function () {
        handleOutcomesAutoSchedulerClick(false);
      });
    }
    if (elements.outcomesAutoResumeBtn) {
      elements.outcomesAutoResumeBtn.addEventListener('click', function () {
        handleOutcomesAutoSchedulerClick(true);
      });
    }
    if (elements.retentionRunBtn) {
      elements.retentionRunBtn.addEventListener('click', handleRetentionRunClick);
    }
    startOutcomesAutoStatusPolling();
    startRetentionStatusPolling();
    handleSubmit(null);
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    init();
  } else {
    document.addEventListener('DOMContentLoaded', init);
  }
})();
