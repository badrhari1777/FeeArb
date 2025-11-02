/**
 * Lightweight dashboard runtime with broad browser compatibility.
 * Avoids modern language features (optional chaining, fetch, etc.)
 * so the page keeps working on older Chromium / Edge builds.
 */
(function () {
  'use strict';

  var MIN_REFRESH_SECONDS = 30;
  var MAX_REFRESH_SECONDS = 86400;
  var MAX_RENDERED_EVENTS = 50;
  var MAX_TELEMETRY = 200;

  var defaultSettings = {
    sources: { arbitragescanner: true, coinglass: true },
    exchanges: { bybit: true, mexc: true },
    parser_refresh_seconds: 300,
    table_refresh_seconds: 300
  };

  var defaultExecution = {
    wallets: [],
    reservations: [],
    positions: [],
    telemetry: []
  };

  var defaultState = {
    status: 'idle',
    refresh_interval: defaultSettings.table_refresh_seconds,
    parser_refresh_interval: defaultSettings.parser_refresh_seconds,
    last_error: null,
    last_updated: null,
    snapshot: null,
    refresh_in_progress: false,
    events: [],
    exchange_status: [],
    settings: clone(defaultSettings),
    execution: clone(defaultExecution)
  };

  var globalState = normalizeState(window.__INITIAL_STATE__);
  var pollingTimer = null;
  var currentPollInterval = 0;
  var pollingInFlight = false;
  var telemetrySocket = null;

  var elements = {
    generatedAt: document.getElementById('generated-at'),
    lastUpdated: document.getElementById('last-updated'),
    screenerSource: document.getElementById('screener-source'),
    coinglassSource: document.getElementById('coinglass-source'),
    opportunityCount: document.getElementById('opportunity-count'),
    statusPill: document.getElementById('status-pill'),
    lastError: document.getElementById('last-error'),
    lastProgress: document.getElementById('last-progress'),
    exchangeSummary: document.getElementById('exchange-summary'),
    screenerTable: document.getElementById('screener-table'),
    coinglassTable: document.getElementById('coinglass-table'),
    universeTable: document.getElementById('universe-table-body'),
    opportunityTable: document.getElementById('opportunity-table-body'),
    messagesPanel: document.getElementById('messages'),
    messagesList: document.getElementById('messages-list'),
    settingsForm: document.getElementById('settings-form'),
    parserInput: document.getElementById('parser-interval'),
    tableInput: document.getElementById('table-interval'),
    settingsStatus: document.getElementById('settings-status'),
    settingsSubmit: document.getElementById('settings-submit'),
    refreshButton: document.getElementById('refresh-button'),
    hint: document.querySelector('.hint'),
    emptyState: document.getElementById('empty-state'),
    exchangeTable: document.getElementById('exchange-status-body'),
    eventLog: document.getElementById('event-log'),
    eventEmpty: document.getElementById('event-empty'),
    walletTable: document.getElementById('wallet-table-body'),
    reservationTable: document.getElementById('reservation-table-body'),
    positionTable: document.getElementById('positions-table-body'),
    executionLog: document.getElementById('execution-activity')
  };

  function clone(value) {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value !== 'object') {
      return value;
    }
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (_err) {
      var copy = {};
      var key;
      for (key in value) {
        if (Object.prototype.hasOwnProperty.call(value, key)) {
          copy[key] = clone(value[key]);
        }
      }
      return copy;
    }
  }

  function normalizeSettings(settings) {
    var normalized = clone(defaultSettings) || {};
    var key;
    var parsed;
    if (settings && typeof settings === 'object') {
      if (settings.sources) {
        for (key in normalized.sources) {
          if (Object.prototype.hasOwnProperty.call(normalized.sources, key)) {
            normalized.sources[key] = !!settings.sources[key];
          }
        }
        for (key in settings.sources) {
          if (Object.prototype.hasOwnProperty.call(settings.sources, key)) {
            normalized.sources[key] = !!settings.sources[key];
          }
        }
      }
      if (settings.exchanges) {
        for (key in normalized.exchanges) {
          if (Object.prototype.hasOwnProperty.call(normalized.exchanges, key)) {
            normalized.exchanges[key] = !!settings.exchanges[key];
          }
        }
        for (key in settings.exchanges) {
          if (Object.prototype.hasOwnProperty.call(settings.exchanges, key)) {
            normalized.exchanges[key] = !!settings.exchanges[key];
          }
        }
      }
      parsed = parseInt(settings.parser_refresh_seconds, 10);
      if (!isNaN(parsed)) {
        normalized.parser_refresh_seconds = clamp(parsed, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
      parsed = parseInt(settings.table_refresh_seconds, 10);
      if (!isNaN(parsed)) {
        normalized.table_refresh_seconds = clamp(parsed, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    return normalized;
  }

  function normalizeExecution(execution) {
    var normalized = clone(defaultExecution) || {
      wallets: [],
      reservations: [],
      positions: [],
      telemetry: []
    };
    if (!execution || typeof execution !== 'object') {
      return normalized;
    }
    if (Array.isArray(execution.wallets)) {
      normalized.wallets = clone(execution.wallets) || [];
    }
    if (Array.isArray(execution.reservations)) {
      normalized.reservations = clone(execution.reservations) || [];
    }
    if (Array.isArray(execution.positions)) {
      normalized.positions = clone(execution.positions) || [];
    }
    if (Array.isArray(execution.telemetry)) {
      normalized.telemetry = clone(execution.telemetry) || [];
    }
    return normalized;
  }

  function normalizeState(source) {
    var state = clone(defaultState) || defaultState;
    if (source && typeof source === 'object') {
      if (typeof source.status === 'string') {
        state.status = source.status;
      }
      if (typeof source.refresh_interval === 'number') {
        state.refresh_interval = source.refresh_interval;
      }
      if (typeof source.parser_refresh_interval === 'number') {
        state.parser_refresh_interval = source.parser_refresh_interval;
      }
      state.last_error = source.last_error || null;
      state.last_updated = source.last_updated || null;
      state.refresh_in_progress = !!source.refresh_in_progress;
      state.snapshot = source.snapshot ? clone(source.snapshot) : null;
      if (Array.isArray(source.events)) {
        state.events = source.events.slice(-MAX_RENDERED_EVENTS);
      }
      if (Array.isArray(source.exchange_status)) {
        state.exchange_status = source.exchange_status.slice();
      }
    }
    state.settings = normalizeSettings(source ? source.settings : null);
    state.execution = normalizeExecution(source ? source.execution : null);
    return state;
  }

  function clamp(value, minimum, maximum) {
    var result = value;
    if (result < minimum) {
      result = minimum;
    }
    if (result > maximum) {
      result = maximum;
    }
    return result;
  }

  function escapeHtml(value) {
    if (value === null || value === undefined) {
      return '';
    }
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatPercent(value, digits) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(number)) {
      return '-';
    }
    var multiplier = 100;
    var places = typeof digits === 'number' ? digits : 2;
    return number * multiplier < 0 ? '-' + Math.abs(number * multiplier).toFixed(places) + '%' : (number * multiplier).toFixed(places) + '%';
  }

  function formatNumber(value, digits) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(number)) {
      return '-';
    }
    var places = typeof digits === 'number' ? digits : 2;
    return number.toFixed(places);
  }

  function formatDate(value) {
    if (!value) {
      return '-';
    }
    try {
      var date = new Date(value);
      if (isNaN(date.getTime())) {
        return '-';
      }
      return date.toLocaleString();
    } catch (_err) {
      return '-';
    }
  }

  function setText(element, text) {
    if (!element) {
      return;
    }
    element.textContent = text || '';
  }

  function updateStatusPill(status) {
    if (!elements.statusPill) {
      return;
    }
    var className = 'status-pill';
    var label = status || 'unknown';
    if (status && typeof status === 'string') {
      className += ' status-pill--' + status.toLowerCase().replace(/[^a-z0-9]+/g, '-');
    } else {
      className += ' status-pill--unknown';
    }
    elements.statusPill.className = className;
    elements.statusPill.textContent = label;
  }

  function updateMetadata(state) {
    var snapshot = state.snapshot || null;
    setText(elements.generatedAt, formatDate(snapshot && snapshot.generated_at));
    var lastUpdated = state.last_updated ? formatDate(state.last_updated) : '-';
    setText(elements.lastUpdated, lastUpdated);
    setText(elements.opportunityCount, snapshot && snapshot.opportunities ? String(snapshot.opportunities.length) : '0');
    setText(elements.lastError, state.last_error || 'None');

    if (elements.screenerSource) {
      if (!snapshot) {
        setText(elements.screenerSource, '-');
      } else if (snapshot.screener_from_cache) {
        setText(elements.screenerSource, 'cache');
      } else {
        setText(elements.screenerSource, 'fresh');
      }
    }

    if (elements.coinglassSource) {
      if (!snapshot) {
        setText(elements.coinglassSource, '-');
      } else if (snapshot.coinglass_from_cache) {
        setText(elements.coinglassSource, 'cache');
      } else {
        setText(elements.coinglassSource, 'fresh');
      }
    }

    var events = state.events || [];
    if (elements.lastProgress) {
      if (events.length === 0) {
        elements.lastProgress.textContent = '-';
      } else {
        var last = events[events.length - 1];
        var message = last.payload && last.payload.message ? last.payload.message : last.event;
        elements.lastProgress.textContent = message || '-';
      }
    }
  }

  function renderScreener(rows) {
    if (!elements.screenerTable) {
      return;
    }
    var body = elements.screenerTable.querySelector('tbody');
    if (!body) {
      return;
    }
    var html = '';
    var limit = Math.min(rows.length || 0, 10);
    var i;
    for (i = 0; i < limit; i += 1) {
      var row = rows[i] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol) + '</td>' +
        '<td>' + formatPercent(row.spread, 4) + '</td>' +
        '<td>' + escapeHtml(row.long_exchange) + '</td>' +
        '<td>' + formatPercent(row.long_fee, 4) + '</td>' +
        '<td>' + escapeHtml(row.short_exchange) + '</td>' +
        '<td>' + formatPercent(row.short_fee, 4) + '</td>' +
      '</tr>';
    }
    body.innerHTML = html;
  }

  function renderCoinglass(rows) {
    if (!elements.coinglassTable) {
      return;
    }
    var body = elements.coinglassTable.querySelector('tbody');
    if (!body) {
      return;
    }
    var html = '';
    var limit = Math.min(rows.length || 0, 10);
    var i;
    for (i = 0; i < limit; i += 1) {
      var row = rows[i] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(row.ranking) + '</td>' +
        '<td>' + escapeHtml(row.symbol) + '</td>' +
        '<td>' + escapeHtml(row.pair) + '</td>' +
        '<td>' + escapeHtml(row.long_exchange) + '</td>' +
        '<td>' + escapeHtml(row.short_exchange) + '</td>' +
        '<td>' + formatPercent(row.net_funding_rate, 3) + '</td>' +
        '<td>' + formatPercent(row.apr, 2) + '</td>' +
        '<td>' + formatPercent(row.spread_rate, 3) + '</td>' +
      '</tr>';
    }
    body.innerHTML = html;
  }

  function renderUniverse(rows) {
    if (!elements.universeTable) {
      return;
    }
    var html = '';
    var i;
    for (i = 0; i < (rows && rows.length ? rows.length : 0); i += 1) {
      var row = rows[i] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol) + '</td>' +
        '<td>' + escapeHtml(row.sources) + '</td>' +
      '</tr>';
    }
    elements.universeTable.innerHTML = html;
  }

  function renderOpportunities(rows) {
    if (!elements.opportunityTable) {
      return;
    }
    var dataset = Array.isArray(rows) ? rows.slice() : [];
    dataset.sort(function (a, b) {
      var av = typeof a === 'object' && a !== null && typeof a.effective_spread === 'number'
        ? a.effective_spread
        : Number.NEGATIVE_INFINITY;
      var bv = typeof b === 'object' && b !== null && typeof b.effective_spread === 'number'
        ? b.effective_spread
        : Number.NEGATIVE_INFINITY;
      if (bv < av) {
        return -1;
      }
      if (bv > av) {
        return 1;
      }
      return 0;
    });

    var html = '';
    var i;
    for (i = 0; i < dataset.length; i += 1) {
      var row = dataset[i] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol) + '</td>' +
        '<td>' + escapeHtml(row.long_exchange) + '</td>' +
        '<td>' + formatPercent(row.long_rate, 3) + '</td>' +
        '<td>' + formatNumber(row.long_ask, 4) + '</td>' +
        '<td>' + formatNumber(row.long_liquidity_usd, 2) + '</td>' +
        '<td>' + escapeHtml(row.long_next_funding || '-') + '</td>' +
        '<td>' + formatNumber(row.long_funding_interval_hours, 2) + '</td>' +
        '<td>' + escapeHtml(row.short_exchange) + '</td>' +
        '<td>' + formatPercent(row.short_rate, 3) + '</td>' +
        '<td>' + formatNumber(row.short_bid, 4) + '</td>' +
        '<td>' + formatNumber(row.short_liquidity_usd, 2) + '</td>' +
        '<td>' + escapeHtml(row.short_next_funding || '-') + '</td>' +
        '<td>' + formatNumber(row.short_funding_interval_hours, 2) + '</td>' +
        '<td>' + formatPercent(row.spread, 3) + '</td>' +
        '<td>' + formatPercent(row.price_diff_pct, 3) + '</td>' +
        '<td>' + formatPercent(row.effective_spread, 3) + '</td>' +
        '<td>' + escapeHtml(row.participants) + '</td>' +
      '</tr>';
    }
    elements.opportunityTable.innerHTML = html;
  }

  function renderExchangeStatus(entries) {
    if (!elements.exchangeTable) {
      return;
    }
    var rows = entries || [];
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var status = row.status || 'unknown';
      html += '<tr>' +
        '<td>' + escapeHtml(row.exchange || row.name || '-') + '</td>' +
        '<td>' + escapeHtml(status) + '</td>' +
        '<td>' + escapeHtml(row.count === undefined || row.count === null ? '-' : row.count) + '</td>' +
        '<td>' + escapeHtml(row.message || row.error || '-') + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="4" class="muted">No exchange updates yet.</td></tr>';
    }
    elements.exchangeTable.innerHTML = html;
    if (elements.exchangeSummary) {
      elements.exchangeSummary.textContent = rows.length ? String(rows.length) + ' tracked' : '-';
    }
  }

  function renderEvents(events) {
    if (!elements.eventLog) {
      return;
    }
    if (!events || !events.length) {
      if (elements.eventLog.innerHTML !== '' && elements.eventEmpty) {
        elements.eventLog.innerHTML = '';
      }
      if (elements.eventEmpty) {
        elements.eventEmpty.style.display = '';
      }
      return;
    }
    if (elements.eventEmpty) {
      elements.eventEmpty.style.display = 'none';
    }
    var html = '';
    var total = events.length;
    var start = total > MAX_RENDERED_EVENTS ? total - MAX_RENDERED_EVENTS : 0;
    var i;
    for (i = start; i < total; i += 1) {
      var entry = events[i] || {};
      var timestamp = formatDate(entry.timestamp);
      var message = entry.payload && entry.payload.message ? entry.payload.message : entry.event;
      html = '<li class="event-log__item"><span class="event-log__time">' + escapeHtml(timestamp) + '</span><span class="event-log__message">' + escapeHtml(message || '-') + '</span></li>' + html;
    }
    elements.eventLog.innerHTML = html;
  }

  function renderMessages(messages) {
    if (!elements.messagesPanel || !elements.messagesList) {
      return;
    }
    if (!messages || !messages.length) {
      elements.messagesPanel.style.display = 'none';
      elements.messagesList.innerHTML = '';
      return;
    }
    var html = '';
    var i;
    for (i = 0; i < messages.length; i += 1) {
      html += '<li>' + escapeHtml(messages[i]) + '</li>';
    }
    elements.messagesList.innerHTML = html;
    elements.messagesPanel.style.display = '';
  }

  function renderWallets(wallets) {
    if (!elements.walletTable) {
      return;
    }
    var html = '';
    var i;
    for (i = 0; i < wallets.length; i += 1) {
      var account = wallets[i] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(account.exchange) + '</td>' +
        '<td>' + formatNumber(account.total, 2) + '</td>' +
        '<td>' + formatNumber(account.available, 2) + '</td>' +
        '<td>' + formatNumber(account.reserved, 2) + '</td>' +
        '<td>' + formatNumber(account.in_positions, 2) + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="5" class="muted">No balances yet.</td></tr>';
    }
    elements.walletTable.innerHTML = html;
  }

  function renderReservations(reservations) {
    if (!elements.reservationTable) {
      return;
    }
    var html = '';
    var i;
    for (i = 0; i < reservations.length; i += 1) {
      var row = reservations[i] || {};
      var exchanges = row.long_exchange && row.short_exchange ? row.long_exchange + ' / ' + row.short_exchange : '-';
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol) + '</td>' +
        '<td>' + escapeHtml(exchanges) + '</td>' +
        '<td>' + formatNumber(row.notional, 2) + '</td>' +
        '<td>' + formatDate(row.created_at) + '</td>' +
        '<td>' + escapeHtml(row.allocation_id || '-') + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="5" class="muted">No active reservations.</td></tr>';
    }
    elements.reservationTable.innerHTML = html;
  }

  function renderPositions(positions) {
    if (!elements.positionTable) {
      return;
    }
    var html = '';
    var i;
    for (i = 0; i < positions.length; i += 1) {
      var row = positions[i] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol) + '</td>' +
        '<td>' + escapeHtml(row.strategy || '-') + '</td>' +
        '<td>' + escapeHtml(row.status || '-') + '</td>' +
        '<td>' + formatNumber(row.notional, 2) + '</td>' +
        '<td>' + formatDate(row.hedged_at) + '</td>' +
        '<td>' + escapeHtml(row.position_id || '-') + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="6" class="muted">No open positions.</td></tr>';
    }
    elements.positionTable.innerHTML = html;
  }

  function renderExecutionLog(entries) {
    if (!elements.executionLog) {
      return;
    }
    if (!entries || !entries.length) {
      elements.executionLog.innerHTML = '<li class="muted">No execution events yet.</li>';
      return;
    }
    var html = '';
    var count = entries.length;
    var start = count > MAX_RENDERED_EVENTS ? count - MAX_RENDERED_EVENTS : 0;
    var i;
    for (i = start; i < count; i += 1) {
      var entry = entries[i] || {};
      var payloadText = '';
      try {
        payloadText = JSON.stringify(entry.payload || {});
      } catch (_err) {
        payloadText = String(entry.payload || '');
      }
      html = '<li class="event-log__item"><span class="event-log__time">' + escapeHtml(formatDate(entry.timestamp)) + '</span><span class="event-log__message">' + escapeHtml(entry.event || '-') + ' ' + escapeHtml(payloadText) + '</span></li>' + html;
    }
    elements.executionLog.innerHTML = html;
  }

  function renderExecution(execution) {
    var data = execution || defaultExecution;
    renderWallets(data.wallets || []);
    renderReservations(data.reservations || []);
    renderPositions(data.positions || []);
    renderExecutionLog(data.telemetry || []);
  }

  function toggleEmptyState(show) {
    if (elements.emptyState) {
      elements.emptyState.style.display = show ? '' : 'none';
    }
  }

  function collectMessages(state) {
    var messages = [];
    if (!state.snapshot && state.status === 'pending') {
      messages.push('Initial data is being collected. This may take a couple of minutes.');
    }
    if (state.last_error) {
      messages.push('Last refresh error: ' + state.last_error);
    }
    if (state.snapshot && state.snapshot.messages && state.snapshot.messages.length) {
      var i;
      for (i = 0; i < state.snapshot.messages.length; i += 1) {
        messages.push(state.snapshot.messages[i]);
      }
    }
    return messages;
  }

  function renderSnapshotData(snapshot) {
    if (!snapshot) {
      renderScreener([]);
      renderCoinglass([]);
      renderUniverse([]);
      renderOpportunities([]);
      renderExchangeStatus(globalState.exchange_status || []);
      return;
    }
    renderScreener(snapshot.screener_rows || []);
    renderCoinglass(snapshot.coinglass_rows || []);
    renderUniverse(snapshot.universe || []);
    renderOpportunities(snapshot.opportunities || []);
    var exchangeEntries = snapshot.exchange_status && snapshot.exchange_status.length
      ? snapshot.exchange_status
      : (globalState.exchange_status || []);
    renderExchangeStatus(exchangeEntries);
    globalState.exchange_status = exchangeEntries.slice ? exchangeEntries.slice() : exchangeEntries;
  }

  function updateHint(state) {
    if (!elements.hint) {
      return;
    }
    var tableSeconds = getRefreshInterval(state);
    var parserSeconds = getParserInterval(state);
    elements.hint.textContent = 'UI refresh: ' + tableSeconds + ' s | Parser: ' + parserSeconds + ' s';
  }

  function renderAll() {
    updateStatusPill(globalState.status);
    updateMetadata(globalState);
    renderSnapshotData(globalState.snapshot);
    renderEvents(globalState.events || []);
    renderExecution(globalState.execution);
    renderMessages(collectMessages(globalState));
    toggleEmptyState(!globalState.snapshot);
    updateHint(globalState);
    updateRefreshButton();
  }

  function getRefreshInterval(state) {
    var interval = defaultState.refresh_interval;
    if (state.settings && typeof state.settings.table_refresh_seconds === 'number') {
      interval = state.settings.table_refresh_seconds;
    }
    if (typeof state.refresh_interval === 'number') {
      interval = state.refresh_interval;
    }
    return clamp(interval, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
  }

  function getParserInterval(state) {
    var interval = defaultState.parser_refresh_interval;
    if (state.settings && typeof state.settings.parser_refresh_seconds === 'number') {
      interval = state.settings.parser_refresh_seconds;
    }
    if (typeof state.parser_refresh_interval === 'number') {
      interval = state.parser_refresh_interval;
    }
    return clamp(interval, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
  }

  function ensurePolling() {
    var interval = getRefreshInterval(globalState);
    if (interval !== currentPollInterval) {
      currentPollInterval = interval;
      if (pollingTimer) {
        window.clearInterval(pollingTimer);
      }
      pollingTimer = window.setInterval(function () {
        pollSnapshot(false);
      }, interval * 1000);
    }
  }

  function updateRefreshButton() {
    if (!elements.refreshButton) {
      return;
    }
    if (globalState.refresh_in_progress) {
      elements.refreshButton.disabled = true;
      elements.refreshButton.textContent = 'Refreshing...';
    } else {
      elements.refreshButton.disabled = false;
      elements.refreshButton.textContent = 'Manual refresh';
    }
  }

  function request(method, url, payload, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open(method, url, true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState === 4) {
        var error = null;
        var data = null;
        if (xhr.status >= 200 && xhr.status < 300) {
          try {
            data = xhr.responseText ? JSON.parse(xhr.responseText) : null;
          } catch (err) {
            error = err;
          }
        } else {
          error = new Error('Request failed (' + xhr.status + ')');
        }
        callback(error, data);
      }
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

  function pollSnapshot(force) {
    if (pollingInFlight) {
      return;
    }
    pollingInFlight = true;
    request('GET', '/api/snapshot', null, function (err, data) {
      pollingInFlight = false;
      if (err) {
        if (window.console && window.console.error) {
          window.console.error('Snapshot load failed', err);
        }
        renderMessages(['Snapshot load error: ' + err.message]);
        return;
      }
      if (data) {
        globalState = normalizeState(data);
        renderAll();
        ensurePolling();
        if (force && data.status === 'pending') {
          window.setTimeout(function () {
            pollSnapshot(true);
          }, 2000);
        }
      }
    });
  }

  function triggerManualRefresh(event) {
    if (event && typeof event.preventDefault === 'function') {
      event.preventDefault();
    }
    if (globalState.refresh_in_progress) {
      return;
    }
    globalState.refresh_in_progress = true;
    updateRefreshButton();
    request('POST', '/api/refresh', null, function (err, data) {
      if (err) {
        renderMessages(['Manual refresh error: ' + err.message]);
        globalState.refresh_in_progress = false;
        updateRefreshButton();
        return;
      }
      if (data && data.state) {
        globalState = normalizeState(data.state);
      }
      renderAll();
      ensurePolling();
      if (data && data.status === 'pending') {
        window.setTimeout(function () {
          pollSnapshot(true);
        }, 2000);
      }
    });
  }

  function collectSettingsFromForm() {
    var result = {
      sources: {},
      exchanges: {},
      parser_refresh_seconds: defaultSettings.parser_refresh_seconds,
      table_refresh_seconds: defaultSettings.table_refresh_seconds
    };
    if (!elements.settingsForm) {
      return result;
    }
    var i;
    var inputs;
    inputs = elements.settingsForm.querySelectorAll('input[name="sources"]');
    for (i = 0; i < inputs.length; i += 1) {
      result.sources[inputs[i].value] = !!inputs[i].checked;
    }
    inputs = elements.settingsForm.querySelectorAll('input[name="exchanges"]');
    for (i = 0; i < inputs.length; i += 1) {
      result.exchanges[inputs[i].value] = !!inputs[i].checked;
    }
    if (elements.parserInput) {
      var parserValue = parseInt(elements.parserInput.value, 10);
      if (!isNaN(parserValue)) {
        result.parser_refresh_seconds = clamp(parserValue, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    if (elements.tableInput) {
      var tableValue = parseInt(elements.tableInput.value, 10);
      if (!isNaN(tableValue)) {
        result.table_refresh_seconds = clamp(tableValue, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    return result;
  }

  function syncSettingsForm(settings) {
    if (!elements.settingsForm || !settings) {
      return;
    }
    var sources = settings.sources || {};
    var exchanges = settings.exchanges || {};
    var inputs;
    var i;
    inputs = elements.settingsForm.querySelectorAll('input[name="sources"]');
    for (i = 0; i < inputs.length; i += 1) {
      var name = inputs[i].value;
      inputs[i].checked = sources.hasOwnProperty(name) ? !!sources[name] : !!defaultSettings.sources[name];
    }
    inputs = elements.settingsForm.querySelectorAll('input[name="exchanges"]');
    for (i = 0; i < inputs.length; i += 1) {
      var exchange = inputs[i].value;
      inputs[i].checked = exchanges.hasOwnProperty(exchange) ? !!exchanges[exchange] : !!defaultSettings.exchanges[exchange];
    }
    if (elements.parserInput) {
      elements.parserInput.value = settings.parser_refresh_seconds;
    }
    if (elements.tableInput) {
      elements.tableInput.value = settings.table_refresh_seconds;
    }
  }

  function setSettingsStatus(message, tone) {
    if (!elements.settingsStatus) {
      return;
    }
    var className = 'settings-status';
    if (tone === 'error') {
      className += ' settings-status--error';
    } else if (tone === 'success') {
      className += ' settings-status--success';
    } else if (tone === 'info') {
      className += ' settings-status--info';
    }
    elements.settingsStatus.className = className;
    elements.settingsStatus.textContent = message || '';
  }

  function handleSettingsSubmit(event) {
    if (event && typeof event.preventDefault === 'function') {
      event.preventDefault();
    }
    var payload = collectSettingsFromForm();
    setSettingsStatus('Saving settings…', 'info');
    request('POST', '/api/settings', payload, function (err, data) {
      if (err) {
        setSettingsStatus(err.message, 'error');
        return;
      }
      if (data && data.settings) {
        globalState.settings = normalizeSettings(data.settings);
      }
      if (data && data.state) {
        globalState = normalizeState(data.state);
      }
      syncSettingsForm(globalState.settings);
      renderAll();
      ensurePolling();
      setSettingsStatus('Settings saved', 'success');
      window.setTimeout(function () {
        setSettingsStatus('', '');
      }, 2500);
    });
  }

  function appendTelemetry(entry) {
    if (!globalState.execution) {
      globalState.execution = clone(defaultExecution);
    }
    if (!Array.isArray(globalState.execution.telemetry)) {
      globalState.execution.telemetry = [];
    }
    globalState.execution.telemetry.push(entry);
    if (globalState.execution.telemetry.length > MAX_TELEMETRY) {
      globalState.execution.telemetry = globalState.execution.telemetry.slice(-MAX_TELEMETRY);
    }
    renderExecutionLog(globalState.execution.telemetry);
  }

  function connectTelemetry() {
    if (!window.WebSocket) {
      return;
    }
    if (telemetrySocket && telemetrySocket.readyState !== window.WebSocket.CLOSED && telemetrySocket.readyState !== window.WebSocket.CLOSING) {
      return;
    }
    var protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
    var url = protocol + window.location.host + '/ws/telemetry';
    try {
      telemetrySocket = new window.WebSocket(url);
    } catch (_err) {
      return;
    }
    telemetrySocket.onmessage = function (event) {
      try {
        var payload = JSON.parse(event.data);
        appendTelemetry(payload);
      } catch (err) {
        if (window.console && window.console.error) {
          window.console.error('Telemetry parse failed', err);
        }
      }
    };
    telemetrySocket.onclose = function () {
      window.setTimeout(connectTelemetry, 5000);
    };
    telemetrySocket.onerror = function () {
      try {
        telemetrySocket.close();
      } catch (_err) {
      }
    };
  }

  function init() {
    globalState = normalizeState(globalState);
    syncSettingsForm(globalState.settings);
    renderAll();
    ensurePolling();

    if (elements.refreshButton) {
      elements.refreshButton.addEventListener('click', triggerManualRefresh);
    }
    if (elements.settingsForm) {
      elements.settingsForm.addEventListener('submit', handleSettingsSubmit);
      elements.settingsForm.addEventListener('change', function () {
        setSettingsStatus('', '');
      });
    }

    connectTelemetry();
    pollSnapshot(true);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
