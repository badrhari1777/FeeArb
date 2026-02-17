/**
 * Mobile-first command surface for FeeArb.
 * Keeps backend behavior unchanged: uses existing API endpoints.
 */
(function () {
  'use strict';

  var SNAPSHOT_POLL_MS = 5000;
  var EXEC_POLL_MS = 2000;
  var MAX_EVENT_ROWS = 40;
  var MAX_LOG_LINES = 180;

  var state = normalizeState(window.__INITIAL_STATE__ || {});
  var currentExecId = null;
  var snapshotInFlight = false;
  var snapshotTimer = null;
  var execTimer = null;
  var exchangesInitialized = false;

  var elements = {
    statusPill: document.getElementById('mobile-status-pill'),
    lastUpdated: document.getElementById('mobile-last-updated'),
    refresh: document.getElementById('mobile-refresh'),
    action: document.getElementById('mobile-action'),
    symbol: document.getElementById('mobile-symbol'),
    qty: document.getElementById('mobile-qty'),
    longExchange: document.getElementById('mobile-long-exchange'),
    shortExchange: document.getElementById('mobile-short-exchange'),
    fromExchange: document.getElementById('mobile-from-exchange'),
    toExchange: document.getElementById('mobile-to-exchange'),
    side: document.getElementById('mobile-side'),
    analyze: document.getElementById('mobile-analyze'),
    execute: document.getElementById('mobile-execute'),
    stop: document.getElementById('mobile-stop'),
    commandStatus: document.getElementById('mobile-command-status'),
    execId: document.getElementById('mobile-exec-id'),
    plan: document.getElementById('mobile-plan'),
    execLog: document.getElementById('mobile-exec-log'),
    cards: document.getElementById('mobile-position-cards'),
    autoEvents: document.getElementById('mobile-auto-events'),
    autoEventsEmpty: document.getElementById('mobile-auto-events-empty')
  };

  function clone(value) {
    if (value === null || value === undefined) {
      return null;
    }
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (_err) {
      return value;
    }
  }

  function normalizeState(source) {
    var base = {
      status: 'idle',
      last_updated: null,
      settings: {},
      accounts: {
        positions_by_symbol: []
      },
      auto_exit: {
        defaults: {},
        rules: {},
        live_spreads: {},
        events: []
      }
    };
    if (!source || typeof source !== 'object') {
      return base;
    }
    base.status = source.status || base.status;
    base.last_updated = source.last_updated || null;
    base.settings = source.settings || {};
    if (source.accounts && typeof source.accounts === 'object') {
      base.accounts = {
        positions_by_symbol: source.accounts.positions_by_symbol || []
      };
    }
    if (source.auto_exit && typeof source.auto_exit === 'object') {
      base.auto_exit = {
        defaults: source.auto_exit.defaults || {},
        rules: source.auto_exit.rules || {},
        live_spreads: source.auto_exit.live_spreads || {},
        events: source.auto_exit.events || []
      };
    }
    return base;
  }

  function request(method, url, payload, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open(method, url, true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) {
        return;
      }
      var parsed = null;
      var err = null;
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          parsed = xhr.responseText ? JSON.parse(xhr.responseText) : null;
        } catch (parseErr) {
          err = parseErr;
        }
      } else {
        var responseText = (xhr.responseText || '').trim();
        if (responseText.length > 180) {
          responseText = responseText.slice(0, 180) + '...';
        }
        err = new Error(method + ' ' + url + ' failed (' + xhr.status + ')' + (responseText ? ' ' + responseText : ''));
      }
      callback(err, parsed);
    };
    xhr.onerror = function () {
      callback(new Error('Network error'), null);
    };
    xhr.setRequestHeader('Accept', 'application/json');
    if (payload) {
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.send(JSON.stringify(payload));
    } else {
      xhr.send();
    }
  }

  function escapeHtml(value) {
    var text = value === null || value === undefined ? '' : String(value);
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatNumber(value, digits) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(parsed)) {
      return '-';
    }
    var places = typeof digits === 'number' ? digits : 4;
    return parsed.toFixed(places);
  }

  function parseOptionalFloat(value) {
    if (value === null || value === undefined) {
      return null;
    }
    var text = String(value).trim();
    if (!text) {
      return null;
    }
    var parsed = parseFloat(text);
    return isNaN(parsed) ? null : parsed;
  }

  function formatDate(value) {
    if (!value) {
      return '-';
    }
    var dt = new Date(value);
    if (isNaN(dt.getTime())) {
      return String(value);
    }
    return dt.toLocaleString();
  }

  function setStatus(message, tone) {
    var node = elements.commandStatus;
    if (!node) {
      return;
    }
    var cls = 'settings-status';
    if (tone === 'error') {
      cls += ' settings-status--error';
    } else if (tone === 'success') {
      cls += ' settings-status--success';
    } else if (tone === 'info') {
      cls += ' settings-status--info';
    }
    node.className = cls;
    node.textContent = message || '';
  }

  function setExecId(execId, status) {
    if (!elements.execId) {
      return;
    }
    if (!execId) {
      elements.execId.textContent = 'Execution id: -';
      return;
    }
    var text = 'Execution id: ' + execId;
    if (status) {
      text += ' (' + status + ')';
    }
    elements.execId.textContent = text;
  }

  function setStopEnabled(enabled) {
    if (elements.stop) {
      elements.stop.disabled = !enabled;
    }
  }

  function currentAction() {
    return elements.action ? String(elements.action.value || 'enter').toLowerCase() : 'enter';
  }

  function toggleActionScopes() {
    var action = currentAction();
    var enterExit = document.querySelectorAll('.mobile-scope-enter-exit');
    var roll = document.querySelectorAll('.mobile-scope-roll');
    var i;
    for (i = 0; i < enterExit.length; i += 1) {
      if (action === 'roll') {
        enterExit[i].classList.add('hidden');
      } else {
        enterExit[i].classList.remove('hidden');
      }
    }
    for (i = 0; i < roll.length; i += 1) {
      if (action === 'roll') {
        roll[i].classList.remove('hidden');
      } else {
        roll[i].classList.add('hidden');
      }
    }
    if (elements.execute) {
      elements.execute.textContent = action === 'roll' ? 'Execute Roll' : ('Execute ' + action.charAt(0).toUpperCase() + action.slice(1));
    }
  }

  function autoExitKey(symbol, longExchange, shortExchange) {
    if (!symbol || !longExchange || !shortExchange) {
      return '';
    }
    return String(symbol).toUpperCase() + '|' + String(longExchange).toLowerCase() + '|' + String(shortExchange).toLowerCase();
  }

  function resolveExchanges() {
    var fromAnalysis = state.settings && state.settings.analysis_exchanges ? state.settings.analysis_exchanges : null;
    var fromGeneral = state.settings && state.settings.exchanges ? state.settings.exchanges : null;
    var names = [];
    var key;
    if (fromAnalysis) {
      for (key in fromAnalysis) {
        if (Object.prototype.hasOwnProperty.call(fromAnalysis, key) && fromAnalysis[key]) {
          names.push(String(key).toLowerCase());
        }
      }
    }
    if (!names.length && fromGeneral) {
      for (key in fromGeneral) {
        if (Object.prototype.hasOwnProperty.call(fromGeneral, key) && fromGeneral[key]) {
          names.push(String(key).toLowerCase());
        }
      }
    }
    if (!names.length && fromAnalysis) {
      for (key in fromAnalysis) {
        if (Object.prototype.hasOwnProperty.call(fromAnalysis, key)) {
          names.push(String(key).toLowerCase());
        }
      }
    }
    if (!names.length) {
      names = ['binance', 'okx', 'bybit', 'gate', 'bitget', 'bingx', 'mexc', 'kucoin'];
    }
    return names;
  }

  function ensureExchangeSelects() {
    if (exchangesInitialized) {
      return;
    }
    var exchanges = resolveExchanges();
    var selects = [
      elements.longExchange,
      elements.shortExchange,
      elements.fromExchange,
      elements.toExchange
    ];
    var i;
    var j;
    for (i = 0; i < selects.length; i += 1) {
      var select = selects[i];
      if (!select) {
        continue;
      }
      select.innerHTML = '';
      for (j = 0; j < exchanges.length; j += 1) {
        var value = exchanges[j];
        var option = document.createElement('option');
        option.value = value;
        option.textContent = value.toUpperCase();
        select.appendChild(option);
      }
    }
    if (elements.longExchange && exchanges.length) {
      elements.longExchange.value = exchanges[0];
    }
    if (elements.shortExchange && exchanges.length > 1) {
      elements.shortExchange.value = exchanges[1];
    }
    if (elements.fromExchange && elements.shortExchange) {
      elements.fromExchange.value = elements.shortExchange.value;
    }
    if (elements.toExchange && elements.longExchange) {
      elements.toExchange.value = elements.longExchange.value;
    }
    exchangesInitialized = true;
  }

  function actionMode(action) {
    if (action === 'exit') {
      return 'smart-exit';
    }
    if (action === 'roll') {
      return 'smart-roll';
    }
    return 'smart-enter';
  }

  function buildManualPayload(action, dryRun) {
    var symbol = elements.symbol ? String(elements.symbol.value || '').trim().toUpperCase() : '';
    var qty = parseOptionalFloat(elements.qty ? elements.qty.value : null);
    if (!symbol) {
      return { error: 'Symbol is required.', payload: null };
    }
    if ((action === 'enter' || action === 'roll') && (qty === null || qty <= 0)) {
      return { error: 'Qty is required for enter/roll.', payload: null };
    }
    var payload = {
      symbol: symbol,
      qty: qty,
      notional: null,
      mode: actionMode(action),
      max_slippage_bps: 8,
      spread_min_pct: null,
      spread_max_pct: 10,
      timeout_sec: 0,
      max_runtime_sec: 600,
      reprice_sec: 3,
      chunk_qty: null,
      chunk_notional: null,
      force_chunk_qty: false,
      use_orderbook_check: true,
      fallback_to_market: false,
      margin_mode: 'isolated',
      async_run: !dryRun,
      dry_run: !!dryRun
    };

    if (action === 'roll') {
      payload.from_exchange = elements.fromExchange ? elements.fromExchange.value : '';
      payload.to_exchange = elements.toExchange ? elements.toExchange.value : '';
      payload.side = elements.side ? elements.side.value : 'long';
      if (!payload.from_exchange || !payload.to_exchange) {
        return { error: 'From/To exchange are required for roll.', payload: null };
      }
    } else {
      payload.long_exchange = elements.longExchange ? elements.longExchange.value : '';
      payload.short_exchange = elements.shortExchange ? elements.shortExchange.value : '';
      if (!payload.long_exchange || !payload.short_exchange) {
        return { error: 'Long/Short exchange are required.', payload: null };
      }
      if (action === 'exit') {
        payload.exit_allow_flip = false;
      }
    }
    return { error: null, payload: payload };
  }

  function actionEndpoint(action) {
    if (action === 'exit') {
      return '/api/manual/exit';
    }
    if (action === 'roll') {
      return '/api/manual/roll';
    }
    return '/api/manual/enter';
  }

  function formatPlan(plan) {
    if (!plan) {
      return 'No data.';
    }
    var lines = [];
    if (plan.errors && plan.errors.length) {
      lines.push('Errors:');
      for (var i = 0; i < plan.errors.length; i += 1) {
        lines.push('  - ' + plan.errors[i]);
      }
    }
    if (plan.warnings && plan.warnings.length) {
      lines.push('Warnings:');
      for (var j = 0; j < plan.warnings.length; j += 1) {
        lines.push('  - ' + plan.warnings[j]);
      }
    }
    if (plan.spread_pct !== undefined && plan.spread_pct !== null) {
      lines.push('Spread: ' + formatNumber(plan.spread_pct, 4) + '%');
    }
    if (plan.recommended_qty !== undefined && plan.recommended_qty !== null) {
      lines.push('Recommended qty: ' + formatNumber(plan.recommended_qty, 6));
    }
    if (plan.recommended_notional !== undefined && plan.recommended_notional !== null) {
      lines.push('Recommended notional: ' + formatNumber(plan.recommended_notional, 2));
    }
    if (!lines.length) {
      try {
        return JSON.stringify(plan, null, 2);
      } catch (_err) {
        return String(plan);
      }
    }
    return lines.join('\n');
  }

  function formatExecLogs(logs) {
    if (!logs || !logs.length) {
      return 'No execution logs yet.';
    }
    var recent = logs.length > MAX_LOG_LINES ? logs.slice(logs.length - MAX_LOG_LINES) : logs;
    var out = [];
    for (var i = 0; i < recent.length; i += 1) {
      var entry = recent[i] || {};
      var ts = entry.ts ? '[' + entry.ts + '] ' : '';
      var eventText = entry.event ? entry.event + ': ' : '';
      var message = entry.message || '';
      if (entry.event === 'story') {
        out.push(ts + message);
        continue;
      }
      if (entry.data && typeof entry.data === 'object' && Object.keys(entry.data).length) {
        out.push(ts + eventText + message + ' ' + JSON.stringify(entry.data));
      } else {
        out.push(ts + eventText + message);
      }
    }
    return out.join('\n');
  }

  function formatAutoEvent(entry) {
    if (!entry || !entry.event) {
      return 'event';
    }
    if (entry.event === 'trigger') {
      return 'Trigger ' + (entry.symbol || '-') +
        ' spread=' + formatNumber(entry.spread_pct, 2) + '% target=' + formatNumber(entry.target_pct, 2) + '%';
    }
    if (entry.event === 'start') {
      var execId = entry.result && entry.result.execution_id ? entry.result.execution_id : '-';
      return 'Start ' + (entry.symbol || '-') + ' exec_id=' + execId;
    }
    if (entry.event === 'wait') {
      return 'Wait ' + (entry.symbol || '-') +
        ' spread=' + formatNumber(entry.spread_pct, 2) + '% target=' + formatNumber(entry.target_pct, 2) + '%';
    }
    if (entry.event === 'skip_running') {
      return 'Skip: execution running ' + (entry.execution_id || '-');
    }
    if (entry.event === 'skip') {
      return 'Skip ' + (entry.symbol || '-') + ' reason=' + (entry.reason || '-');
    }
    return entry.event;
  }

  function renderHeader() {
    if (elements.statusPill) {
      var status = String(state.status || 'idle').toLowerCase();
      elements.statusPill.className = 'status-pill status-pill--' + status;
      elements.statusPill.textContent = status;
    }
    if (elements.lastUpdated) {
      elements.lastUpdated.textContent = formatDate(state.last_updated);
    }
  }

  function renderPositions() {
    if (!elements.cards) {
      return;
    }
    var rows = state.accounts && state.accounts.positions_by_symbol ? state.accounts.positions_by_symbol : [];
    var summaries = [];
    for (var i = 0; i < rows.length; i += 1) {
      if (rows[i] && rows[i].type === 'summary') {
        summaries.push(rows[i]);
      }
    }
    if (!summaries.length) {
      elements.cards.innerHTML = '<div class="muted">No symbol summaries yet.</div>';
      return;
    }
    var html = '';
    for (var j = 0; j < summaries.length; j += 1) {
      var row = summaries[j];
      var longEx = row.long_exchange || '';
      var shortEx = row.short_exchange || '';
      var key = autoExitKey(row.symbol, longEx, shortEx);
      var rule = state.auto_exit.rules && state.auto_exit.rules.hasOwnProperty(key) ? state.auto_exit.rules[key] : null;
      var enabled = !!(rule && rule.enabled);
      var target = rule && rule.target_spread_pct !== undefined && rule.target_spread_pct !== null ? rule.target_spread_pct : '';
      var liveSpread = state.auto_exit.live_spreads && state.auto_exit.live_spreads.hasOwnProperty(key) ? state.auto_exit.live_spreads[key] : null;
      html += '<article class="mobile-card">' +
        '<div class="mobile-card-head">' +
          '<button type="button" class="mobile-symbol-link" data-symbol="' + escapeHtml(row.symbol || '') + '">' + escapeHtml(row.symbol || '-') + '</button>' +
          '<span class="mobile-card-pair">' + escapeHtml((longEx || '-').toUpperCase() + ' / ' + (shortEx || '-').toUpperCase()) + '</span>' +
        '</div>' +
        '<div class="mobile-card-grid">' +
          '<span>Qty:</span><strong>' + formatNumber(row.quantity, 4) + '</strong>' +
          '<span>PnL:</span><strong>' + formatNumber(row.unrealized_pnl, 4) + '</strong>' +
          '<span>Live spread:</span><strong>' + (liveSpread === null || liveSpread === undefined ? '-' : (formatNumber(liveSpread, 4) + '%')) + '</strong>' +
        '</div>' +
        '<div class="mobile-auto-row">' +
          '<label class="settings-checkbox"><input type="checkbox" class="mobile-auto-enabled" ' + (enabled ? 'checked' : '') + '>Auto exit</label>' +
          '<input type="number" class="mobile-auto-target" step="0.01" placeholder="-0.70" value="' + escapeHtml(target) + '">' +
          '<button type="button" class="button button--ghost mobile-auto-save" data-symbol="' + escapeHtml(row.symbol || '') + '" data-long="' + escapeHtml(longEx) + '" data-short="' + escapeHtml(shortEx) + '">Save</button>' +
        '</div>' +
      '</article>';
    }
    elements.cards.innerHTML = html;
    bindPositionCardEvents();
  }

  function bindPositionCardEvents() {
    var saveButtons = document.querySelectorAll('.mobile-auto-save');
    for (var i = 0; i < saveButtons.length; i += 1) {
      saveButtons[i].addEventListener('click', handleAutoExitSave);
    }
    var symbolButtons = document.querySelectorAll('.mobile-symbol-link');
    for (var j = 0; j < symbolButtons.length; j += 1) {
      symbolButtons[j].addEventListener('click', function (event) {
        var button = event && event.target ? event.target : null;
        if (!button || !elements.symbol) {
          return;
        }
        elements.symbol.value = button.getAttribute('data-symbol') || '';
      });
    }
  }

  function handleAutoExitSave(event) {
    var button = event && event.target ? event.target : null;
    if (!button) {
      return;
    }
    var card = button.closest('.mobile-card');
    if (!card) {
      return;
    }
    var enabledEl = card.querySelector('.mobile-auto-enabled');
    var targetEl = card.querySelector('.mobile-auto-target');
    var enabled = !!(enabledEl && enabledEl.checked);
    var targetVal = parseOptionalFloat(targetEl ? targetEl.value : null);
    if (enabled && (targetVal === null || !isFinite(targetVal))) {
      setStatus('Auto-exit target spread is required.', 'error');
      return;
    }
    var payload = {
      symbol: button.getAttribute('data-symbol') || '',
      long_exchange: button.getAttribute('data-long') || '',
      short_exchange: button.getAttribute('data-short') || '',
      enabled: enabled,
      target_spread_pct: enabled ? targetVal : null
    };
    request('POST', '/api/auto-exit/rule', payload, function (err, data) {
      if (err) {
        setStatus('Auto-exit update failed: ' + err.message, 'error');
        return;
      }
      if (data && data.auto_exit) {
        state.auto_exit = data.auto_exit;
        renderPositions();
        renderAutoEvents();
      }
      setStatus('Auto-exit updated.', 'success');
    });
  }

  function renderAutoEvents() {
    if (!elements.autoEvents || !elements.autoEventsEmpty) {
      return;
    }
    var events = state.auto_exit && state.auto_exit.events ? state.auto_exit.events : [];
    if (!events.length) {
      elements.autoEvents.innerHTML = '';
      elements.autoEventsEmpty.classList.remove('hidden');
      return;
    }
    elements.autoEventsEmpty.classList.add('hidden');
    var recent = events.length > MAX_EVENT_ROWS ? events.slice(events.length - MAX_EVENT_ROWS) : events.slice();
    recent.reverse();
    var html = '';
    for (var i = 0; i < recent.length; i += 1) {
      var entry = recent[i] || {};
      html += '<li class="event-log__item">' +
        '<span class="event-log__time">' + escapeHtml(entry.ts || '-') + '</span>' +
        '<span class="event-log__message">' + escapeHtml(formatAutoEvent(entry)) + '</span>' +
      '</li>';
    }
    elements.autoEvents.innerHTML = html;
  }

  function renderAll() {
    renderHeader();
    renderPositions();
    renderAutoEvents();
  }

  function applyExecutionData(data) {
    if (!data) {
      return;
    }
    setExecId(currentExecId, data.status || null);
    if (elements.execLog) {
      elements.execLog.textContent = formatExecLogs(data.logs || []);
    }
    if (elements.plan && data.result) {
      elements.plan.textContent = formatPlan(data.result);
    }
    if (data.status === 'running') {
      setStatus(data.stop_requested ? 'Stop requested; waiting...' : 'Execution running...', 'info');
      setStopEnabled(true);
      return;
    }
    if (data.status === 'completed') {
      setStatus('Execution completed.', 'success');
    } else if (data.status === 'completed_with_errors') {
      setStatus('Execution completed with errors.', 'error');
    } else if (data.status === 'failed') {
      setStatus('Execution failed: ' + (data.error || 'unknown error'), 'error');
    } else if (data.status) {
      setStatus('Execution status: ' + data.status, 'info');
    }
    setStopEnabled(false);
    stopExecPolling();
    currentExecId = null;
    setExecId(null, null);
  }

  function fetchExecution(execId) {
    request('GET', '/api/manual/exec/' + encodeURIComponent(execId), null, function (err, data) {
      if (err) {
        setStatus(err.message, 'error');
        return;
      }
      applyExecutionData(data);
    });
  }

  function startExecPolling(execId) {
    if (!execId) {
      return;
    }
    currentExecId = execId;
    setExecId(execId, 'running');
    setStopEnabled(true);
    if (execTimer) {
      window.clearInterval(execTimer);
      execTimer = null;
    }
    fetchExecution(execId);
    execTimer = window.setInterval(function () {
      if (!currentExecId) {
        stopExecPolling();
        return;
      }
      fetchExecution(currentExecId);
    }, EXEC_POLL_MS);
  }

  function stopExecPolling() {
    if (execTimer) {
      window.clearInterval(execTimer);
      execTimer = null;
    }
  }

  function fetchRunningExecution() {
    request('GET', '/api/manual/exec', null, function (err, data) {
      if (err || !data || !data.runs) {
        return;
      }
      var runs = data.runs || [];
      var running = null;
      for (var i = 0; i < runs.length; i += 1) {
        if (runs[i] && runs[i].status === 'running') {
          running = runs[i];
          break;
        }
      }
      if (!running) {
        return;
      }
      if (!currentExecId || currentExecId !== running.execution_id) {
        startExecPolling(running.execution_id);
      }
    });
  }

  function doAnalyze() {
    var action = currentAction();
    var built = buildManualPayload(action, true);
    if (built.error) {
      setStatus(built.error, 'error');
      return;
    }
    var payload = clone(built.payload) || {};
    payload.action = action;
    setStatus('Analyzing...', 'info');
    request('POST', '/api/manual/analyze', payload, function (err, data) {
      if (err) {
        setStatus(err.message, 'error');
        return;
      }
      if (elements.plan) {
        elements.plan.textContent = formatPlan(data);
      }
      if (data && data.errors && data.errors.length) {
        setStatus('Analyze completed with errors.', 'error');
      } else {
        setStatus('Analyze completed.', 'success');
      }
    });
  }

  function doExecute() {
    var action = currentAction();
    var built = buildManualPayload(action, true);
    if (built.error) {
      setStatus(built.error, 'error');
      return;
    }
    var endpoint = actionEndpoint(action);
    var preflightPayload = built.payload;
    setStatus('Running forced dry-run preflight...', 'info');
    request('POST', endpoint, preflightPayload, function (preErr, preflight) {
      if (preErr) {
        setStatus(preErr.message, 'error');
        return;
      }
      if (elements.plan) {
        elements.plan.textContent = formatPlan(preflight);
      }
      if (preflight && preflight.errors && preflight.errors.length) {
        setStatus('Preflight failed. Fix errors first.', 'error');
        return;
      }
      var spread = preflight && preflight.spread_pct !== undefined && preflight.spread_pct !== null
        ? formatNumber(preflight.spread_pct, 4) + '%'
        : '-';
      var confirmText = 'Submit ' + action.toUpperCase() +
        '\nSymbol: ' + preflightPayload.symbol +
        '\nQty: ' + (preflightPayload.qty === null || preflightPayload.qty === undefined ? 'auto' : preflightPayload.qty) +
        '\nSpread: ' + spread +
        '\n\nContinue?';
      if (!window.confirm(confirmText)) {
        setStatus('Execution canceled by user.', 'info');
        return;
      }
      var executePayload = clone(preflightPayload) || {};
      executePayload.dry_run = false;
      executePayload.async_run = true;
      setStatus('Submitting execution...', 'info');
      request('POST', endpoint, executePayload, function (execErr, execData) {
        if (execErr) {
          setStatus(execErr.message, 'error');
          return;
        }
        if (execData && execData.execution_id) {
          setStatus('Execution started.', 'success');
          startExecPolling(execData.execution_id);
          return;
        }
        if (elements.plan) {
          elements.plan.textContent = formatPlan(execData);
        }
        setStatus('Execution completed.', 'success');
      });
    });
  }

  function doStop() {
    if (!currentExecId) {
      return;
    }
    setStatus('Stop requested...', 'info');
    request('POST', '/api/manual/exec/' + encodeURIComponent(currentExecId) + '/stop', {}, function (err) {
      if (err) {
        setStatus(err.message, 'error');
        return;
      }
      setStatus('Stop requested; waiting for runner...', 'info');
    });
  }

  function refreshSnapshot(forceStatusMessage) {
    if (snapshotInFlight) {
      return;
    }
    snapshotInFlight = true;
    if (forceStatusMessage) {
      setStatus('Refreshing...', 'info');
    }
    request('GET', '/api/snapshot', null, function (err, payload) {
      snapshotInFlight = false;
      if (err) {
        if (forceStatusMessage) {
          setStatus(err.message, 'error');
        }
        return;
      }
      state = normalizeState(payload || {});
      if (!exchangesInitialized) {
        ensureExchangeSelects();
      }
      renderAll();
      fetchRunningExecution();
      if (forceStatusMessage) {
        setStatus('Snapshot updated.', 'success');
      }
    });
  }

  function bindEvents() {
    if (elements.action) {
      elements.action.addEventListener('change', toggleActionScopes);
    }
    if (elements.refresh) {
      elements.refresh.addEventListener('click', function () {
        refreshSnapshot(true);
      });
    }
    if (elements.analyze) {
      elements.analyze.addEventListener('click', doAnalyze);
    }
    if (elements.execute) {
      elements.execute.addEventListener('click', doExecute);
    }
    if (elements.stop) {
      elements.stop.addEventListener('click', doStop);
    }
  }

  function init() {
    ensureExchangeSelects();
    bindEvents();
    toggleActionScopes();
    renderAll();
    fetchRunningExecution();
    refreshSnapshot(false);
    if (!snapshotTimer) {
      snapshotTimer = window.setInterval(function () {
        refreshSnapshot(false);
      }, SNAPSHOT_POLL_MS);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
