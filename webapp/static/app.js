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

  var protectiveDefaults = {
    auto_protect_enabled: true,
    auto_take_enabled: true,
    send_margin_alerts: true,
    send_missing_stop_alerts: true,
    auto_margin_enabled: true,
    auto_margin_reduce_enabled: true,
    auto_rebalance_enabled: false,
    stop_gap_from_liq_pct: 0.07,
    stop_requote_threshold_pct: 0.005,
    fallback_liq_factor_long: 0.33,
    fallback_liq_factor_short: 1.66,
    rebalance_delta_pct: 0.2,
    rebalance_cooldown_sec: 120,
    rebalance_limit_timeout_sec: 10,
    rebalance_limit_offset_bps: 2,
    rebalance_max_slippage_bps: 8
  };

  var defaultSettings = {
    sources: { arbitragescanner: true, coinglass: true },
    exchanges: { binance: true, okx: true },
    analysis_exchanges: { binance: true, bybit: true, bingx: true, bitget: true, okx: true, gate: true, mexc: true, kucoin: true },
    parser_refresh_seconds: 1200,
    exchange_refresh_seconds: 300,
    table_refresh_seconds: 60,
    account_refresh_seconds: 60,
    positions_market_refresh_seconds: 60,
    stop_gap_from_liq_pct: protectiveDefaults.stop_gap_from_liq_pct,
    stop_requote_threshold_pct: protectiveDefaults.stop_requote_threshold_pct,
    fallback_liq_factor_long: protectiveDefaults.fallback_liq_factor_long,
    fallback_liq_factor_short: protectiveDefaults.fallback_liq_factor_short,
    protective: clone(protectiveDefaults)
  };

  var defaultExecution = {
    wallets: [],
    reservations: [],
    positions: [],
    telemetry: []
  };

  var defaultAccounts = {
    balances: [],
    status: [],
    positions: [],
    positions_by_symbol: [],
    last_updated: null,
    positions_market: null
  };

  var defaultAutoExit = {
    defaults: {
      max_runtime_sec: 600,
      cooldown_sec: 300,
      require_live: true
    },
    rules: {},
    live_spreads: {},
    events: []
  };

  var defaultState = {
    status: 'idle',
    refresh_interval: defaultSettings.table_refresh_seconds,
    parser_refresh_interval: defaultSettings.parser_refresh_seconds,
    exchange_refresh_interval: defaultSettings.exchange_refresh_seconds,
    account_refresh_interval: defaultSettings.account_refresh_seconds,
    positions_market_refresh_interval: defaultSettings.positions_market_refresh_seconds,
    last_error: null,
    last_updated: null,
    snapshot: null,
    refresh_in_progress: false,
    events: [],
    exchange_status: [],
    settings: clone(defaultSettings),
    execution: clone(defaultExecution),
    accounts: clone(defaultAccounts),
    auto_exit: clone(defaultAutoExit)
  };

  var globalState = normalizeState(window.__INITIAL_STATE__);
  var pollingTimer = null;
  var currentPollInterval = 0;
  var pollingInFlight = false;
  var autoExitExecState = {
    execId: null,
    status: null,
    logs: [],
    errors: [],
    error: null,
    logPath: null,
    lastFetched: 0
  };
  var staleAutoExitExecIds = {};
  var autoExitExecTimer = null;

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
    exchangeInput: document.getElementById('exchange-interval'),
    tableInput: document.getElementById('table-interval'),
    accountInput: document.getElementById('account-interval'),
    positionsMarketInput: document.getElementById('positions-market-interval'),
    protectAuto: document.getElementById('protect-auto'),
    takeAuto: document.getElementById('take-auto'),
    alertMargin: document.getElementById('alert-margin'),
    autoMarginAdd: document.getElementById('auto-margin-add'),
    autoMarginReduce: document.getElementById('auto-margin-reduce'),
    alertMissingStops: document.getElementById('alert-missing-stops'),
    stopGapInput: document.getElementById('stop-gap'),
    requoteInput: document.getElementById('stop-requote'),
    fallbackLongInput: document.getElementById('fallback-long'),
    fallbackShortInput: document.getElementById('fallback-short'),
    rebalanceAuto: document.getElementById('rebalance-auto'),
    rebalanceDeltaInput: document.getElementById('rebalance-delta'),
    rebalanceCooldownInput: document.getElementById('rebalance-cooldown'),
    rebalanceTimeoutInput: document.getElementById('rebalance-timeout'),
    rebalanceOffsetInput: document.getElementById('rebalance-offset'),
    rebalanceSlippageInput: document.getElementById('rebalance-slippage'),
    autoExitRuntimeInput: document.getElementById('auto-exit-runtime'),
    autoExitCooldownInput: document.getElementById('auto-exit-cooldown'),
    autoExitRequireLive: document.getElementById('auto-exit-require-live'),
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
    executionLog: document.getElementById('execution-activity'),
    accountLastUpdated: document.getElementById('account-last-updated'),
    accountStatusTable: document.getElementById('account-status-body'),
    accountBalanceTable: document.getElementById('account-balance-body'),
    symbolPositionsTable: document.getElementById('symbol-positions-body'),
    symbolPositionsMeta: document.getElementById('symbol-positions-meta'),
    symbolPositionsDiffs: document.getElementById('symbol-positions-diffs'),
    autoExitLog: document.getElementById('auto-exit-log'),
    autoExitLogEmpty: document.getElementById('auto-exit-log-empty'),
    autoExitLogCopy: document.getElementById('auto-exit-log-copy'),
    autoExitAgentStatus: document.getElementById('auto-exit-agent-status'),
    autoExitAgentMeta: document.getElementById('auto-exit-agent-meta'),
    autoExitAgentErrors: document.getElementById('auto-exit-agent-errors'),
    autoExitAgentLog: document.getElementById('auto-exit-agent-log'),
    autoExitAgentLogEmpty: document.getElementById('auto-exit-agent-log-empty'),
    autoExitAgentCopy: document.getElementById('auto-exit-agent-copy'),
    autoExitAgentOpenLog: document.getElementById('auto-exit-agent-open-log'),
    autoExitAgentStop: document.getElementById('auto-exit-agent-stop'),
    quickAnalyzeForm: document.getElementById('quick-analyze-form'),
    quickAnalyzeInput: document.getElementById('quick-analyze-input'),
    quickWindowInput: document.getElementById('quick-window-input'),
    quickFundingInput: document.getElementById('quick-funding-input'),
    quickAnalyzeStatus: document.getElementById('quick-analyze-status'),
    quickAnalyzeButton: document.getElementById('quick-analyze-button')
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
      if (settings.analysis_exchanges) {
        normalized.analysis_exchanges = {};
        for (key in settings.analysis_exchanges) {
          if (Object.prototype.hasOwnProperty.call(settings.analysis_exchanges, key)) {
            normalized.analysis_exchanges[key] = !!settings.analysis_exchanges[key];
          }
        }
      } else {
        normalized.analysis_exchanges = clone(normalized.exchanges);
      }
      parsed = parseInt(settings.parser_refresh_seconds, 10);
      if (!isNaN(parsed)) {
        normalized.parser_refresh_seconds = clamp(parsed, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
      parsed = parseInt(settings.exchange_refresh_seconds, 10);
      if (!isNaN(parsed)) {
        normalized.exchange_refresh_seconds = clamp(parsed, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
      parsed = parseInt(settings.table_refresh_seconds, 10);
      if (!isNaN(parsed)) {
        normalized.table_refresh_seconds = clamp(parsed, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
      parsed = parseInt(settings.account_refresh_seconds, 10);
      if (!isNaN(parsed)) {
        normalized.account_refresh_seconds = clamp(parsed, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
      parsed = parseInt(settings.positions_market_refresh_seconds, 10);
      if (!isNaN(parsed)) {
        normalized.positions_market_refresh_seconds = clamp(parsed, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
      // Merge protective toggles/thresholds, preserving falsey values.
      var incomingProtective = (settings && typeof settings.protective === 'object') ? settings.protective : null;
      var mergedProtective = clone(protectiveDefaults) || {};
      if (incomingProtective) {
        for (key in incomingProtective) {
          if (Object.prototype.hasOwnProperty.call(incomingProtective, key)) {
            mergedProtective[key] = incomingProtective[key];
          }
        }
      }
      normalized.protective = mergedProtective;
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

  function normalizeAccounts(accounts) {
    var normalized = clone(defaultAccounts) || {
      balances: [],
      status: [],
      positions: [],
      positions_by_symbol: [],
      last_updated: null,
      positions_market: null
    };
    if (!accounts || typeof accounts !== 'object') {
      return normalized;
    }
    if (Array.isArray(accounts.balances)) {
      normalized.balances = clone(accounts.balances) || [];
    }
    if (Array.isArray(accounts.status)) {
      normalized.status = clone(accounts.status) || [];
    }
    if (Array.isArray(accounts.positions)) {
      normalized.positions = clone(accounts.positions) || [];
    }
    if (Array.isArray(accounts.positions_by_symbol)) {
      normalized.positions_by_symbol = clone(accounts.positions_by_symbol) || [];
    }
    normalized.last_updated = accounts.last_updated || null;
    normalized.positions_market = accounts.positions_market || null;
    return normalized;
  }

  function normalizeAutoExit(config) {
    var normalized = clone(defaultAutoExit) || { defaults: {}, rules: {} };
    if (!config || typeof config !== 'object') {
      return normalized;
    }
    if (config.defaults && typeof config.defaults === 'object') {
      if (config.defaults.max_runtime_sec !== undefined && config.defaults.max_runtime_sec !== null) {
        var runtimeVal = parseInt(config.defaults.max_runtime_sec, 10);
        if (!isNaN(runtimeVal)) {
          normalized.defaults.max_runtime_sec = runtimeVal;
        }
      }
      if (config.defaults.cooldown_sec !== undefined && config.defaults.cooldown_sec !== null) {
        var cooldownVal = parseInt(config.defaults.cooldown_sec, 10);
        if (!isNaN(cooldownVal)) {
          normalized.defaults.cooldown_sec = cooldownVal;
        }
      }
      if (config.defaults.require_live !== undefined && config.defaults.require_live !== null) {
        normalized.defaults.require_live = !!config.defaults.require_live;
      }
    }
    if (config.rules && typeof config.rules === 'object') {
      normalized.rules = clone(config.rules) || {};
    }
    if (config.live_spreads && typeof config.live_spreads === 'object') {
      normalized.live_spreads = clone(config.live_spreads) || {};
    }
    if (Array.isArray(config.events)) {
      normalized.events = clone(config.events) || [];
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
      if (typeof source.exchange_refresh_interval === 'number') {
        state.exchange_refresh_interval = source.exchange_refresh_interval;
      }
      if (typeof source.account_refresh_interval === 'number') {
        state.account_refresh_interval = source.account_refresh_interval;
      }
      if (typeof source.positions_market_refresh_interval === 'number') {
        state.positions_market_refresh_interval = source.positions_market_refresh_interval;
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
    state.accounts = normalizeAccounts(source ? source.accounts : null);
    state.auto_exit = normalizeAutoExit(source ? source.auto_exit : null);
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
        '<td><a class="symbol-link" href="/coin/' + encodeURIComponent(row.symbol || '') + '">' + escapeHtml(row.symbol) + '</a></td>' +
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
        '<td><a class="symbol-link" href="/coin/' + encodeURIComponent(row.symbol || '') + '">' + escapeHtml(row.symbol) + '</a></td>' +
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
    renderExecutionLog(data.telemetry || []);
  }

  function renderAccountStatus(entries) {
    if (!elements.accountStatusTable) {
      return;
    }
    var rows = entries || [];
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(row.exchange || '-') + '</td>' +
        '<td>' + escapeHtml(row.status || '-') + '</td>' +
        '<td>' + escapeHtml(row.message || row.error || '-') + '</td>' +
        '<td>' + escapeHtml(formatDate(row.checked_at)) + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="4" class="muted">No credential checks yet.</td></tr>';
    }
    elements.accountStatusTable.innerHTML = html;
  }

  function renderAccountBalances(entries) {
    if (!elements.accountBalanceTable) {
      return;
    }
    var rows = entries || [];
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var marginRatioText = typeof row.margin_ratio === 'number' ? formatNumber(row.margin_ratio, 4) : '-';
      var equityText = typeof row.equity === 'number' ? formatNumber(row.equity, 2) : '-';
      var bufferText = typeof row.buffer_pct === 'number' ? formatNumber(row.buffer_pct, 2) + '%' : '-';
      html += '<tr>' +
        '<td>' + escapeHtml(row.exchange || '-') + '</td>' +
        '<td>' + escapeHtml(row.asset || '-') + '</td>' +
        '<td>' + formatNumber(row.total, 2) + '</td>' +
        '<td>' + formatNumber(row.available, 2) + '</td>' +
        '<td>' + formatNumber(row.used, 2) + '</td>' +
        '<td>' + marginRatioText + '</td>' +
        '<td>' + equityText + '</td>' +
        '<td>' + bufferText + '</td>' +
        '<td>' + escapeHtml(formatDate(row.timestamp)) + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="9" class="muted">Balances will appear after the first refresh.</td></tr>';
    }
    elements.accountBalanceTable.innerHTML = html;
  }

  function formatNextFunding(ts) {
    if (!ts) {
      return '-';
    }
    var dt = new Date(ts);
    if (isNaN(dt.getTime())) {
      return '-';
    }
    var diffMs = dt.getTime() - Date.now();
    if (diffMs <= 0) {
      return 'due';
    }
    var minutes = Math.floor(diffMs / 60000);
    var hours = Math.floor(minutes / 60);
    var remMinutes = minutes % 60;
    return hours + 'h ' + remMinutes + 'm';
  }

  function autoExitKey(symbol, longExchange, shortExchange) {
    if (!symbol || !longExchange || !shortExchange) {
      return '';
    }
    return String(symbol).toUpperCase() + '|' + String(longExchange).toLowerCase() + '|' + String(shortExchange).toLowerCase();
  }

  function autoExitRuleFor(symbol, longExchange, shortExchange) {
    var key = autoExitKey(symbol, longExchange, shortExchange);
    if (!key) {
      return null;
    }
    var rules = (globalState.auto_exit && globalState.auto_exit.rules) ? globalState.auto_exit.rules : {};
    return rules && rules.hasOwnProperty(key) ? rules[key] : null;
  }

  function autoExitLiveSpreadFor(symbol, longExchange, shortExchange) {
    var key = autoExitKey(symbol, longExchange, shortExchange);
    if (!key) {
      return null;
    }
    var spreads = (globalState.auto_exit && globalState.auto_exit.live_spreads) ? globalState.auto_exit.live_spreads : {};
    if (spreads && spreads.hasOwnProperty(key)) {
      return spreads[key];
    }
    return null;
  }

  function formatAutoExitEvent(entry) {
    if (!entry || !entry.event) {
      return 'event';
    }
    if (entry.event === 'trigger') {
      var triggerMsg = 'Trigger ' + (entry.symbol || '-') + ' ' + (entry.long_exchange || '-') + '/' + (entry.short_exchange || '-') +
        ' spread=' + formatNumber(entry.spread_pct, 2) + '% target=' + formatNumber(entry.target_pct, 2) + '% qty=' + formatNumber(entry.qty, 4);
      if (entry.spread_scope) {
        triggerMsg += ' scope=' + entry.spread_scope;
      }
      if (entry.pair_spread_pct !== undefined && entry.pair_spread_pct !== null) {
        triggerMsg += ' pair=' + formatNumber(entry.pair_spread_pct, 2) + '%';
      }
      if (entry.overall_spread_pct !== undefined && entry.overall_spread_pct !== null) {
        triggerMsg += ' overall=' + formatNumber(entry.overall_spread_pct, 2) + '%';
      }
      return triggerMsg;
    }
    if (entry.event === 'wait') {
      var waitMsg = 'Wait ' + (entry.symbol || '-') + ' ' + (entry.long_exchange || '-') + '/' + (entry.short_exchange || '-') +
        ' spread=' + formatNumber(entry.spread_pct, 2) + '% target=' + formatNumber(entry.target_pct, 2) + '%';
      if (entry.spread_scope) {
        waitMsg += ' scope=' + entry.spread_scope;
      }
      return waitMsg;
    }
    if (entry.event === 'skip') {
      var msg = 'Skip ' + (entry.symbol || '-') + ' ' + (entry.long_exchange || '-') + '/' + (entry.short_exchange || '-') +
        ' reason=' + (entry.reason || '-');
      if (entry.remaining_sec !== undefined && entry.remaining_sec !== null) {
        msg += ' remaining=' + formatNumber(entry.remaining_sec, 1) + 's';
      }
      if (entry.long_legs !== undefined && entry.short_legs !== undefined) {
        msg += ' legs=' + entry.long_legs + '/' + entry.short_legs;
      }
      return msg;
    }
    if (entry.event === 'skip_running') {
      var runningId = entry.execution_id ? (' exec_id=' + entry.execution_id) : '';
      var runningAction = entry.action ? (' action=' + entry.action) : '';
      return 'Skip cycle: execution running' + runningId + runningAction;
    }
    if (entry.event === 'start') {
      var execId = entry.result && entry.result.execution_id ? entry.result.execution_id : '-';
      return 'Started ' + (entry.symbol || '-') + ' exec_id=' + execId;
    }
    return entry.event;
  }

  function renderSymbolPositions(rows) {
    if (!elements.symbolPositionsTable) {
      return;
    }
    rows = rows || [];
    var html = '';
    var i;
    var lastSymbol = null;

    function toneClass(row) {
      var pnlVal = parseFloat(row.unrealized_pnl);
      var expVal = parseFloat(row.expected_funding);
      var pnlSign = isFinite(pnlVal) && Math.abs(pnlVal) > 1e-9 ? (pnlVal > 0 ? 1 : -1) : 0;
      var expSign = isFinite(expVal) && Math.abs(expVal) > 1e-9 ? (expVal > 0 ? 1 : -1) : 0;
      if (pnlSign === 1 && expSign === 1) {
        return 'tone-pos';
      }
      if (pnlSign === -1 && expSign === -1) {
        return 'tone-neg';
      }
      return 'tone-mixed';
    }

    function valueClass(value) {
      var numeric = parseFloat(value);
      if (isFinite(numeric) && numeric > 0) {
        return 'value-pos';
      }
      if (isFinite(numeric) && numeric < 0) {
        return 'value-neg';
      }
      return '';
    }

    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var isSummary = row.type === 'summary';
      var showSymbol = row.symbol !== lastSymbol || isSummary;
      var entryText = isSummary
        ? (row.entry_price !== null && row.entry_price !== undefined ? formatNumber(row.entry_price, 2) + '%' : '-')
        : formatNumber(row.entry_price, 6);
      var markText = isSummary
        ? (row.mark_price !== null && row.mark_price !== undefined ? formatNumber(row.mark_price, 2) + '%' : '-')
        : formatNumber(row.mark_price, 6);
      var fundingText = row.funding_rate !== null && row.funding_rate !== undefined
        ? formatPercent(row.funding_rate, 4)
        : '-';
      var marginUsedText = isSummary ? '-' : formatNumber(row.margin_used, 2);
      var liqPriceText = isSummary ? '-' : formatNumber(row.liquidation_price, 4);
      var liqDistText = row.dist_to_liq_pct !== null && row.dist_to_liq_pct !== undefined
        ? formatNumber(row.dist_to_liq_pct, 3) + '%'
        : '-';
      var leverageText = isSummary ? '-' : formatNumber(row.leverage, 2);
      var expectedFundingText = row.expected_funding !== null && row.expected_funding !== undefined
        ? formatNumber(row.expected_funding, 6)
        : '-';
      var stopPriceText = row.stop_price !== null && row.stop_price !== undefined
        ? formatNumber(row.stop_price, 6)
        : '-';
      var takePriceText = row.take_price !== null && row.take_price !== undefined
        ? formatNumber(row.take_price, 6)
        : '-';
      var nextFundingText = row.next_funding_eta || formatNextFunding(row.next_funding);
      var autoExitToggle = '-';
      var autoExitTarget = '-';
      var liveSpreadText = '-';
      if (isSummary) {
        var longEx = row.long_exchange;
        var shortEx = row.short_exchange;
        var longCount = parseInt(row.long_legs_count, 10);
        var shortCount = parseInt(row.short_legs_count, 10);
        if (!isFinite(longCount)) {
          longCount = 0;
        }
        if (!isFinite(shortCount)) {
          shortCount = 0;
        }
        var hasBothSides = longCount > 0 && shortCount > 0;
        var isPair = !!(longEx && shortEx);
        var isMultileg = hasBothSides && !isPair;
        var ruleLong = isMultileg ? 'multileg' : longEx;
        var ruleShort = isMultileg ? 'multileg' : shortEx;
        if (hasBothSides && ruleLong && ruleShort) {
          var rule = autoExitRuleFor(row.symbol, ruleLong, ruleShort);
          var enabled = rule && rule.enabled;
          var targetVal = rule && rule.target_spread_pct !== undefined && rule.target_spread_pct !== null
            ? formatNumber(rule.target_spread_pct, 2)
            : '';
          var liveSpread = autoExitLiveSpreadFor(row.symbol, ruleLong, ruleShort);
          liveSpreadText = liveSpread !== null && liveSpread !== undefined
            ? formatNumber(liveSpread, 2) + '%'
            : (isMultileg ? '<span class="muted">n/a</span>' : '-');
          var key = autoExitKey(row.symbol, ruleLong, ruleShort);
          var checkbox = '<input type="checkbox" class="auto-exit-toggle" data-key="' + escapeHtml(key) + '" data-symbol="' +
            escapeHtml(row.symbol || '') + '" data-long="' + escapeHtml(ruleLong) + '" data-short="' + escapeHtml(ruleShort) + '"' +
            (enabled ? ' checked' : '') + ' />';
          var input = '<input type="number" class="auto-exit-target" step="0.01" placeholder="-7.9" value="' + escapeHtml(targetVal) +
            '" data-key="' + escapeHtml(key) + '" data-symbol="' + escapeHtml(row.symbol || '') + '" data-long="' +
            escapeHtml(ruleLong) + '" data-short="' + escapeHtml(ruleShort) + '" />';
          autoExitToggle = checkbox + (isMultileg ? ' <span class="muted">multi-leg</span>' : '');
          autoExitTarget = input;
        } else {
          autoExitToggle = '<span class="muted">one-side</span>';
          autoExitTarget = '<span class="muted">n/a</span>';
          liveSpreadText = '<span class="muted">n/a</span>';
        }
      }
      var summaryTone = isSummary ? toneClass(row) : '';
      var classes = isSummary ? ('summary-row ' + summaryTone) : '';
      var symbolAttr = row.symbol ? ' data-symbol="' + escapeHtml(row.symbol) + '"' : '';
      var pnlClass = valueClass(row.unrealized_pnl);
      var expClass = valueClass(row.expected_funding);
      html += '<tr class="' + classes + '"'+ symbolAttr + '>' +
        '<td>' + (showSymbol ? escapeHtml(row.symbol || '-') : '-') + '</td>' +
        '<td>' + escapeHtml(isSummary ? (row.exchange || 'TOTAL') : (row.exchange || '-')) + '</td>' +
        '<td>' + formatNumber(row.quantity, 4) + '</td>' +
        '<td>' + formatNumber(row.amount, 2) + '</td>' +
        '<td>' + marginUsedText + '</td>' +
        '<td>' + entryText + '</td>' +
        '<td>' + markText + '</td>' +
        '<td class="' + pnlClass + '">' + formatNumber(row.unrealized_pnl, 4) + '</td>' +
        '<td>' + liqPriceText + '</td>' +
        '<td>' + liqDistText + '</td>' +
        '<td>' + leverageText + '</td>' +
        '<td>' + fundingText + '</td>' +
        '<td class="' + expClass + '">' + expectedFundingText + '</td>' +
        '<td>' + stopPriceText + '</td>' +
        '<td>' + takePriceText + '</td>' +
        '<td>' + escapeHtml(nextFundingText) + '</td>' +
        '<td>' + liveSpreadText + '</td>' +
        '<td>' + autoExitToggle + '</td>' +
        '<td>' + autoExitTarget + '</td>' +
      '</tr>';
      lastSymbol = row.symbol;
    }
    if (!html) {
      html = '<tr><td colspan="19" class="muted">No live positions.</td></tr>';
    }
    elements.symbolPositionsTable.innerHTML = html;
    attachSymbolHover(elements.symbolPositionsTable);
  }

  function renderSymbolPositionsDiagnostics(meta) {
    if (!elements.symbolPositionsMeta || !elements.symbolPositionsDiffs) {
      return;
    }
    if (!meta || typeof meta !== 'object') {
      elements.symbolPositionsMeta.textContent = 'Positions market diagnostics unavailable.';
      elements.symbolPositionsDiffs.textContent = '';
      return;
    }
    var lastUpdated = meta.last_updated ? formatDate(meta.last_updated) : '-';
    var symbolCount = typeof meta.symbols === 'number' ? meta.symbols : 0;
    var exchangeCount = typeof meta.exchanges === 'number' ? meta.exchanges : 0;
      var metaLine = 'Positions market: last=' + lastUpdated + ' | symbols=' + symbolCount + ' | exchanges=' + exchangeCount;
      if (meta.last_error) {
        metaLine += ' | last_error=' + meta.last_error;
      }
    if (Array.isArray(meta.status) && meta.status.length) {
      var parts = [];
      var i;
      for (i = 0; i < meta.status.length; i += 1) {
        var row = meta.status[i] || {};
        if (!row.exchange) {
          continue;
        }
        var line = row.exchange + ':' + (row.status || 'unknown');
        if (typeof row.count === 'number' && typeof row.symbols === 'number') {
          line += ' (' + row.count + '/' + row.symbols + ')';
        } else if (typeof row.symbols === 'number') {
          line += ' (' + row.symbols + ')';
        }
        parts.push(line);
      }
      if (parts.length) {
        metaLine += ' | per-exchange: ' + parts.join(', ');
      }
    }
    elements.symbolPositionsMeta.textContent = metaLine;

      var diffs = Array.isArray(meta.diffs) ? meta.diffs : [];
      var marginIssues = Array.isArray(meta.margin_issues) ? meta.margin_issues : [];
      if (marginIssues.length) {
        metaLine += ' | margin_issues=' + marginIssues.length;
      }
      var lines = [];
      if (diffs.length) {
        lines.push('Diffs (position vs market snapshot):');
        var j;
        for (j = 0; j < diffs.length; j += 1) {
          var diff = diffs[j] || {};
          var label = (diff.exchange || '-') + ' ' + (diff.symbol || '-');
          var field = diff.field || 'value';
          var posVal = diff.position !== undefined ? diff.position : '-';
          var snapVal = diff.snapshot !== undefined ? diff.snapshot : '-';
          var delta = diff.delta_pct !== undefined ? (formatNumber(diff.delta_pct, 3) + '%') : (diff.delta !== undefined ? formatNumber(diff.delta, 6) : '-');
          lines.push('  ' + label + ' ' + field + ': pos=' + posVal + ' snap=' + snapVal + ' delta=' + delta);
        }
      } else {
        lines.push('No mark/funding diffs above threshold.');
      }
      if (marginIssues.length) {
        lines.push('Margin mode/leverage issues:');
        var k;
        for (k = 0; k < marginIssues.length; k += 1) {
          var issue = marginIssues[k] || {};
          var issueLabel = (issue.exchange || '-') + ' ' + (issue.symbol || '-');
          var sideText = issue.side ? (' ' + issue.side) : '';
          var modeText = issue.margin_mode !== undefined && issue.margin_mode !== null ? issue.margin_mode : '-';
          var levText = issue.leverage !== undefined && issue.leverage !== null ? formatNumber(issue.leverage, 2) : '-';
          var modeSrc = issue.margin_mode_source || '-';
          var levSrc = issue.leverage_source || '-';
          var issueBits = Array.isArray(issue.issues) ? issue.issues.join(',') : '-';
          lines.push('  ' + issueLabel + sideText + ' mode=' + modeText + ' (' + modeSrc + ') lev=' + levText + ' (' + levSrc + ') [' + issueBits + ']');
        }
      } else {
        lines.push('No margin mode/leverage issues detected.');
      }
    elements.symbolPositionsDiffs.innerHTML = lines.map(escapeHtml).join('<br>');
  }

  function renderAutoExitLog(autoExit) {
    if (!elements.autoExitLog || !elements.autoExitLogEmpty) {
      return;
    }
    var events = (autoExit && Array.isArray(autoExit.events)) ? autoExit.events : [];
    if (!events.length) {
      elements.autoExitLog.innerHTML = '';
      elements.autoExitLogEmpty.style.display = '';
      return;
    }
    elements.autoExitLogEmpty.style.display = 'none';
    var html = '';
    var i;
    for (i = events.length - 1; i >= 0; i -= 1) {
      var entry = events[i] || {};
      var ts = formatDate(entry.ts);
      var message = formatAutoExitEvent(entry);
      var messageHtml = escapeHtml(message);
      if (entry.event === 'skip_running' && entry.execution_id) {
        var execId = entry.execution_id;
        var logUrl = '/api/manual/exec/' + encodeURIComponent(execId) + '/log';
        messageHtml += ' <a class="event-log__link" href="' + logUrl + '" target="_blank" rel="noreferrer">log</a>';
      }
      html += '<li class="event-log__item"><span class="event-log__time">' + escapeHtml(ts) +
        '</span><span class="event-log__message">' + messageHtml + '</span></li>';
    }
    elements.autoExitLog.innerHTML = html;
  }

  function latestAutoExitExecId(autoExit) {
    if (!autoExit || !Array.isArray(autoExit.events)) {
      return null;
    }
    var events = autoExit.events;
    var i;
    for (i = events.length - 1; i >= 0; i -= 1) {
      var entry = events[i] || {};
      if (entry.event === 'start') {
        var execId = entry.result && entry.result.execution_id ? entry.result.execution_id : null;
        if (execId && !staleAutoExitExecIds[execId]) {
          return execId;
        }
      }
    }
    return null;
  }

  function resetAutoExitExecState(clearExecId) {
    if (clearExecId !== false) {
      autoExitExecState.execId = null;
    }
    autoExitExecState.status = null;
    autoExitExecState.logs = [];
    autoExitExecState.errors = [];
    autoExitExecState.error = null;
    autoExitExecState.logPath = null;
    autoExitExecState.lastFetched = 0;
  }

  function syncAutoExitExecId(autoExit) {
    var execId = latestAutoExitExecId(autoExit);
    if (!execId) {
      if (autoExitExecState.execId !== null) {
        resetAutoExitExecState(true);
      }
      return;
    }
    if (autoExitExecState.execId !== execId) {
      resetAutoExitExecState(true);
      autoExitExecState.execId = execId;
      fetchAutoExitExec();
    }
  }

  function renderAutoExitAgent() {
    if (!elements.autoExitAgentStatus || !elements.autoExitAgentLog || !elements.autoExitAgentLogEmpty) {
      return;
    }
    if (!autoExitExecState.execId) {
      elements.autoExitAgentStatus.textContent = 'No auto-exit execution yet.';
      elements.autoExitAgentLog.innerHTML = '';
      elements.autoExitAgentLogEmpty.style.display = '';
      if (elements.autoExitAgentMeta) {
        elements.autoExitAgentMeta.textContent = '';
      }
      if (elements.autoExitAgentErrors) {
        elements.autoExitAgentErrors.textContent = '';
      }
      return;
    }
    var statusText = 'Execution ' + autoExitExecState.execId;
    if (autoExitExecState.status) {
      statusText += ' | status=' + autoExitExecState.status;
    }
    elements.autoExitAgentStatus.textContent = statusText;
    if (elements.autoExitAgentMeta) {
      elements.autoExitAgentMeta.textContent = autoExitExecState.logPath
        ? ('Log file: ' + autoExitExecState.logPath)
        : '';
    }
    if (elements.autoExitAgentErrors) {
      var errorLines = [];
      if (autoExitExecState.error) {
        errorLines.push('Exception: ' + autoExitExecState.error);
      }
      var iErr;
      var errors = autoExitExecState.errors || [];
      for (iErr = 0; iErr < errors.length; iErr += 1) {
        errorLines.push('Error: ' + errors[iErr]);
      }
      elements.autoExitAgentErrors.textContent = errorLines.join(' | ');
    }
    var logs = autoExitExecState.logs || [];
    if (!logs.length) {
      elements.autoExitAgentLog.innerHTML = '';
      elements.autoExitAgentLogEmpty.style.display = '';
      return;
    }
    elements.autoExitAgentLogEmpty.style.display = 'none';
    var html = '';
    var i;
    for (i = logs.length - 1; i >= 0; i -= 1) {
      var entry = logs[i] || {};
      var ts = formatDate(entry.ts);
      var message = entry.message || entry.event || '-';
      html += '<li class="event-log__item"><span class="event-log__time">' + escapeHtml(ts) +
        '</span><span class="event-log__message">' + escapeHtml(message) + '</span></li>';
    }
    elements.autoExitAgentLog.innerHTML = html;
  }

  function fetchAutoExitExec() {
    if (!autoExitExecState.execId) {
      return;
    }
    var requestedExecId = autoExitExecState.execId;
    var now = Date.now();
    if (autoExitExecState.lastFetched && (now - autoExitExecState.lastFetched) < 1500) {
      return;
    }
    autoExitExecState.lastFetched = now;
    request('GET', '/api/manual/exec/' + encodeURIComponent(requestedExecId), null, function (err, data) {
      if (autoExitExecState.execId !== requestedExecId) {
        return;
      }
      if (err) {
        if (err.status === 404) {
          staleAutoExitExecIds[requestedExecId] = true;
          resetAutoExitExecState(true);
          renderAutoExitAgent();
        }
        return;
      }
      if (!data) {
        return;
      }
      autoExitExecState.status = data.status || null;
      autoExitExecState.logs = Array.isArray(data.logs) ? data.logs.slice(-200) : [];
      autoExitExecState.error = data.error || null;
      autoExitExecState.errors = (data.result && Array.isArray(data.result.errors)) ? data.result.errors.slice(0, 20) : [];
      autoExitExecState.logPath = data.log_path || null;
      renderAutoExitAgent();
    });
  }

  function autoExitLogText(events) {
    if (!events || !events.length) {
      return '';
    }
    var lines = [];
    var i;
    for (i = 0; i < events.length; i += 1) {
      var entry = events[i] || {};
      var ts = formatDate(entry.ts);
      var message = formatAutoExitEvent(entry);
      lines.push(ts + ' | ' + message);
    }
    return lines.join('\n');
  }

  function autoExitAgentLogText(state) {
    var logs = state && state.logs ? state.logs : [];
    var lines = [];
    if (state) {
      if (state.logPath) {
        lines.push('Log file: ' + state.logPath);
      }
      if (state.error) {
        lines.push('Exception: ' + state.error);
      }
      if (state.errors && state.errors.length) {
        var errIndex;
        for (errIndex = 0; errIndex < state.errors.length; errIndex += 1) {
          lines.push('Error: ' + state.errors[errIndex]);
        }
      }
      if (lines.length) {
        lines.push('');
      }
    }
    if (!logs || !logs.length) {
      return lines.join('\n');
    }
    var i;
    for (i = 0; i < logs.length; i += 1) {
      var entry = logs[i] || {};
      var ts = formatDate(entry.ts);
      var message = entry.message || entry.event || '-';
      lines.push(ts + ' | ' + message);
    }
    return lines.join('\n');
  }

  function copyToClipboard(text) {
    if (!text) {
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text);
      return;
    }
    var textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    try {
      document.execCommand('copy');
    } catch (_err) {
      // ignore
    }
    document.body.removeChild(textarea);
  }

  function attachSymbolHover(tbody) {
    if (!tbody) {
      return;
    }
    var rows = tbody.querySelectorAll('tr');
    function clear() {
      rows.forEach(function (r) { r.classList.remove('pos-hover'); });
    }
    tbody.onmouseover = function (evt) {
      var tr = evt.target.closest('tr');
      if (!tr || !tr.dataset || !tr.dataset.symbol) {
        return;
      }
      clear();
      var sym = tr.dataset.symbol;
      rows.forEach(function (r) {
        if (r.dataset && r.dataset.symbol === sym) {
          r.classList.add('pos-hover');
        }
      });
    };
    tbody.onmouseout = function () {
      clear();
    };
  }

  function renderAccounts(accounts) {
    var data = accounts || defaultAccounts;
    renderAccountStatus(data.status || []);
    renderAccountBalances(data.balances || []);
    renderSymbolPositions(data.positions_by_symbol || []);
    renderSymbolPositionsDiagnostics(data.positions_market || null);
    if (elements.accountLastUpdated) {
      elements.accountLastUpdated.textContent = data.last_updated ? formatDate(data.last_updated) : '-';
    }
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
    var exchangeSeconds = getExchangeInterval(state);
    var accountSeconds = getAccountInterval(state);
    var positionsSeconds = getPositionsMarketInterval(state);
    elements.hint.textContent = 'UI refresh: ' + tableSeconds + ' s | Parser: ' + parserSeconds + ' s | Exchange poll: ' + exchangeSeconds + ' s | Account refresh: ' + accountSeconds + ' s | Positions market: ' + positionsSeconds + ' s';
  }

  function renderAll() {
    updateStatusPill(globalState.status);
    updateMetadata(globalState);
    renderSnapshotData(globalState.snapshot);
    renderEvents(globalState.events || []);
    renderExecution(globalState.execution);
    renderAccounts(globalState.accounts);
    renderAutoExitLog(globalState.auto_exit || {});
    syncAutoExitExecId(globalState.auto_exit || {});
    renderAutoExitAgent();
    renderMessages(collectMessages(globalState));
    toggleEmptyState(!globalState.snapshot);
    updateHint(globalState);
    updateRefreshButton();
    syncAutoExitDefaults(globalState.auto_exit);
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

  function getExchangeInterval(state) {
    var interval = defaultState.exchange_refresh_interval;
    if (state.settings && typeof state.settings.exchange_refresh_seconds === 'number') {
      interval = state.settings.exchange_refresh_seconds;
    }
    if (typeof state.exchange_refresh_interval === 'number') {
      interval = state.exchange_refresh_interval;
    }
    return clamp(interval, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
  }

  function getAccountInterval(state) {
    var interval = defaultState.account_refresh_interval;
    if (state.settings && typeof state.settings.account_refresh_seconds === 'number') {
      interval = state.settings.account_refresh_seconds;
    }
    if (typeof state.account_refresh_interval === 'number') {
      interval = state.account_refresh_interval;
    }
    return clamp(interval, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
  }

  function getPositionsMarketInterval(state) {
    var interval = defaultState.positions_market_refresh_interval;
    if (state.settings && typeof state.settings.positions_market_refresh_seconds === 'number') {
      interval = state.settings.positions_market_refresh_seconds;
    }
    if (typeof state.positions_market_refresh_interval === 'number') {
      interval = state.positions_market_refresh_interval;
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
          error.status = xhr.status;
          try {
            data = xhr.responseText ? JSON.parse(xhr.responseText) : null;
            if (data && data.detail) {
              error.detail = String(data.detail);
            }
          } catch (_parseErr) {
            data = null;
          }
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

  function setQuickAnalyzeStatus(message, tone) {
    if (!elements.quickAnalyzeStatus) {
      return;
    }
    var cls = 'settings-status';
    if (tone === 'error') {
      cls += ' settings-status--error';
    } else if (tone === 'success') {
      cls += ' settings-status--success';
    }
    elements.quickAnalyzeStatus.className = cls;
    elements.quickAnalyzeStatus.textContent = message || '';
  }

  function navigateToAnalysis(symbol, windowMinutes, fundingPoints) {
    var params = [];
    if (windowMinutes) {
      params.push('window_minutes=' + encodeURIComponent(windowMinutes));
    }
    if (fundingPoints) {
      params.push('funding_points=' + encodeURIComponent(fundingPoints));
    }
    var url = '/coin/' + encodeURIComponent(symbol);
    if (params.length) {
      url += '?' + params.join('&');
    }
    window.location.href = url;
  }

  function handleQuickAnalyzeSubmit(event) {
    if (event && typeof event.preventDefault === 'function') {
      event.preventDefault();
    }
    if (!elements.quickAnalyzeInput) {
      return;
    }
    var symbol = (elements.quickAnalyzeInput.value || '').trim().toUpperCase();
    var windowMinutes = elements.quickWindowInput ? parseInt(elements.quickWindowInput.value || '4320', 10) : 4320;
    var fundingPoints = elements.quickFundingInput ? parseInt(elements.quickFundingInput.value || '120', 10) : 120;
    if (!symbol) {
      setQuickAnalyzeStatus('Введите символ.', 'error');
      return;
    }
    setQuickAnalyzeStatus('Открываю анализ...', 'success');
    navigateToAnalysis(symbol, windowMinutes, fundingPoints);
  }

  function collectSettingsFromForm() {
    var result = {
      sources: {},
      exchanges: {},
      parser_refresh_seconds: defaultSettings.parser_refresh_seconds,
      exchange_refresh_seconds: defaultSettings.exchange_refresh_seconds,
      table_refresh_seconds: defaultSettings.table_refresh_seconds,
      account_refresh_seconds: defaultSettings.account_refresh_seconds,
      positions_market_refresh_seconds: defaultSettings.positions_market_refresh_seconds,
      protective: {}
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
    inputs = elements.settingsForm.querySelectorAll('input[name="analysis_exchanges"]');
    result.analysis_exchanges = {};
    for (i = 0; i < inputs.length; i += 1) {
      result.analysis_exchanges[inputs[i].value] = !!inputs[i].checked;
    }
    if (elements.parserInput) {
      var parserValue = parseInt(elements.parserInput.value, 10);
      if (!isNaN(parserValue)) {
        result.parser_refresh_seconds = clamp(parserValue, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    if (elements.exchangeInput) {
      var exchangeValue = parseInt(elements.exchangeInput.value, 10);
      if (!isNaN(exchangeValue)) {
        result.exchange_refresh_seconds = clamp(exchangeValue, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    if (elements.tableInput) {
      var tableValue = parseInt(elements.tableInput.value, 10);
      if (!isNaN(tableValue)) {
        result.table_refresh_seconds = clamp(tableValue, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    if (elements.accountInput) {
      var accountValue = parseInt(elements.accountInput.value, 10);
      if (!isNaN(accountValue)) {
        result.account_refresh_seconds = clamp(accountValue, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    if (elements.positionsMarketInput) {
      var positionsValue = parseInt(elements.positionsMarketInput.value, 10);
      if (!isNaN(positionsValue)) {
        result.positions_market_refresh_seconds = clamp(positionsValue, MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS);
      }
    }
    result.protective = {
      auto_protect_enabled: elements.protectAuto ? !!elements.protectAuto.checked : true,
      auto_take_enabled: elements.takeAuto ? !!elements.takeAuto.checked : true,
      send_margin_alerts: elements.alertMargin ? !!elements.alertMargin.checked : true,
      send_missing_stop_alerts: elements.alertMissingStops ? !!elements.alertMissingStops.checked : true,
      auto_margin_enabled: elements.autoMarginAdd ? !!elements.autoMarginAdd.checked : true,
      auto_margin_reduce_enabled: elements.autoMarginReduce ? !!elements.autoMarginReduce.checked : true,
      stop_gap_from_liq_pct: elements.stopGapInput ? parseFloat(elements.stopGapInput.value) || defaultSettings.stop_gap_from_liq_pct || 0.07 : 0.07,
      stop_requote_threshold_pct: elements.requoteInput ? parseFloat(elements.requoteInput.value) || 0.005 : 0.005,
      fallback_liq_factor_long: elements.fallbackLongInput ? parseFloat(elements.fallbackLongInput.value) || 0.33 : 0.33,
      fallback_liq_factor_short: elements.fallbackShortInput ? parseFloat(elements.fallbackShortInput.value) || 1.66 : 1.66,
      auto_rebalance_enabled: elements.rebalanceAuto ? !!elements.rebalanceAuto.checked : false,
      rebalance_delta_pct: elements.rebalanceDeltaInput ? parseFloat(elements.rebalanceDeltaInput.value) || 0.2 : 0.2,
      rebalance_cooldown_sec: elements.rebalanceCooldownInput ? parseInt(elements.rebalanceCooldownInput.value, 10) || 120 : 120,
      rebalance_limit_timeout_sec: elements.rebalanceTimeoutInput ? parseInt(elements.rebalanceTimeoutInput.value, 10) || 10 : 10,
      rebalance_limit_offset_bps: elements.rebalanceOffsetInput ? parseFloat(elements.rebalanceOffsetInput.value) || 2 : 2,
      rebalance_max_slippage_bps: elements.rebalanceSlippageInput ? parseFloat(elements.rebalanceSlippageInput.value) || 8 : 8
    };
    return result;
  }

  function collectAutoExitDefaults() {
    if (!elements.autoExitRuntimeInput && !elements.autoExitCooldownInput && !elements.autoExitRequireLive) {
      return null;
    }
    var defaults = {
      max_runtime_sec: defaultAutoExit.defaults.max_runtime_sec,
      cooldown_sec: defaultAutoExit.defaults.cooldown_sec,
      require_live: defaultAutoExit.defaults.require_live
    };
    if (elements.autoExitRuntimeInput) {
      var runtimeValue = parseInt(elements.autoExitRuntimeInput.value, 10);
      if (!isNaN(runtimeValue)) {
        defaults.max_runtime_sec = Math.max(30, runtimeValue);
      }
    }
    if (elements.autoExitCooldownInput) {
      var cooldownValue = parseInt(elements.autoExitCooldownInput.value, 10);
      if (!isNaN(cooldownValue)) {
        defaults.cooldown_sec = Math.max(0, cooldownValue);
      }
    }
    if (elements.autoExitRequireLive) {
      defaults.require_live = !!elements.autoExitRequireLive.checked;
    }
    return defaults;
  }

  function syncAutoExitDefaults(autoExit) {
    if (!autoExit || !autoExit.defaults) {
      return;
    }
    if (elements.autoExitRuntimeInput) {
      elements.autoExitRuntimeInput.value = autoExit.defaults.max_runtime_sec;
    }
    if (elements.autoExitCooldownInput) {
      elements.autoExitCooldownInput.value = autoExit.defaults.cooldown_sec;
    }
    if (elements.autoExitRequireLive) {
      elements.autoExitRequireLive.checked = !!autoExit.defaults.require_live;
    }
  }

  function submitAutoExitDefaults(defaults, callback) {
    if (!defaults) {
      if (typeof callback === 'function') {
        callback(null, null);
      }
      return;
    }
    request('POST', '/api/auto-exit/defaults', defaults, function (err, data) {
      if (!err && data && data.auto_exit) {
        globalState.auto_exit = normalizeAutoExit(data.auto_exit);
      }
      if (typeof callback === 'function') {
        callback(err, data);
      }
    });
  }

  function toggleRebalanceFields(enabled) {
    var isEnabled = !!enabled;
    if (elements.rebalanceDeltaInput) {
      elements.rebalanceDeltaInput.disabled = !isEnabled;
    }
    if (elements.rebalanceCooldownInput) {
      elements.rebalanceCooldownInput.disabled = !isEnabled;
    }
    if (elements.rebalanceTimeoutInput) {
      elements.rebalanceTimeoutInput.disabled = !isEnabled;
    }
    if (elements.rebalanceOffsetInput) {
      elements.rebalanceOffsetInput.disabled = !isEnabled;
    }
    if (elements.rebalanceSlippageInput) {
      elements.rebalanceSlippageInput.disabled = !isEnabled;
    }
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
    var analysis = settings.analysis_exchanges || exchanges || {};
    inputs = elements.settingsForm.querySelectorAll('input[name="analysis_exchanges"]');
    for (i = 0; i < inputs.length; i += 1) {
      var name = inputs[i].value;
      inputs[i].checked = analysis.hasOwnProperty(name) ? !!analysis[name] : !!defaultSettings.analysis_exchanges[name];
    }
    if (elements.parserInput) {
      elements.parserInput.value = settings.parser_refresh_seconds;
    }
    if (elements.exchangeInput) {
      elements.exchangeInput.value = settings.exchange_refresh_seconds;
    }
    if (elements.tableInput) {
      elements.tableInput.value = settings.table_refresh_seconds;
    }
    if (elements.accountInput) {
      elements.accountInput.value = settings.account_refresh_seconds;
    }
    if (elements.positionsMarketInput) {
      elements.positionsMarketInput.value = settings.positions_market_refresh_seconds;
    }
    var protective = settings.protective || {};
    if (elements.protectAuto) {
      elements.protectAuto.checked = protective.hasOwnProperty('auto_protect_enabled') ? !!protective.auto_protect_enabled : true;
    }
    if (elements.takeAuto) {
      elements.takeAuto.checked = protective.hasOwnProperty('auto_take_enabled') ? !!protective.auto_take_enabled : true;
    }
    if (elements.alertMargin) {
      elements.alertMargin.checked = protective.hasOwnProperty('send_margin_alerts') ? !!protective.send_margin_alerts : true;
    }
    if (elements.autoMarginAdd) {
      elements.autoMarginAdd.checked = protective.hasOwnProperty('auto_margin_enabled') ? !!protective.auto_margin_enabled : true;
    }
    if (elements.autoMarginReduce) {
      elements.autoMarginReduce.checked = protective.hasOwnProperty('auto_margin_reduce_enabled') ? !!protective.auto_margin_reduce_enabled : true;
    }
    if (elements.alertMissingStops) {
      elements.alertMissingStops.checked = protective.hasOwnProperty('send_missing_stop_alerts') ? !!protective.send_missing_stop_alerts : true;
    }
    if (elements.stopGapInput) {
      elements.stopGapInput.value = protective.stop_gap_from_liq_pct !== undefined ? protective.stop_gap_from_liq_pct : 0.07;
    }
    if (elements.requoteInput) {
      elements.requoteInput.value = protective.stop_requote_threshold_pct !== undefined ? protective.stop_requote_threshold_pct : 0.005;
    }
    if (elements.fallbackLongInput) {
      elements.fallbackLongInput.value = protective.fallback_liq_factor_long !== undefined ? protective.fallback_liq_factor_long : 0.33;
    }
    if (elements.fallbackShortInput) {
      elements.fallbackShortInput.value = protective.fallback_liq_factor_short !== undefined ? protective.fallback_liq_factor_short : 1.66;
    }
    if (elements.rebalanceAuto) {
      elements.rebalanceAuto.checked = protective.hasOwnProperty('auto_rebalance_enabled') ? !!protective.auto_rebalance_enabled : false;
    }
    if (elements.rebalanceDeltaInput) {
      elements.rebalanceDeltaInput.value = protective.rebalance_delta_pct !== undefined ? protective.rebalance_delta_pct : 0.2;
    }
    if (elements.rebalanceCooldownInput) {
      elements.rebalanceCooldownInput.value = protective.rebalance_cooldown_sec !== undefined ? protective.rebalance_cooldown_sec : 120;
    }
    if (elements.rebalanceTimeoutInput) {
      elements.rebalanceTimeoutInput.value = protective.rebalance_limit_timeout_sec !== undefined ? protective.rebalance_limit_timeout_sec : 10;
    }
    if (elements.rebalanceOffsetInput) {
      elements.rebalanceOffsetInput.value = protective.rebalance_limit_offset_bps !== undefined ? protective.rebalance_limit_offset_bps : 2;
    }
    if (elements.rebalanceSlippageInput) {
      elements.rebalanceSlippageInput.value = protective.rebalance_max_slippage_bps !== undefined ? protective.rebalance_max_slippage_bps : 8;
    }
    toggleRebalanceFields(elements.rebalanceAuto ? elements.rebalanceAuto.checked : false);
  }

  function handleAutoExitChange(event) {
    if (!event || !event.target) {
      return;
    }
    var target = event.target;
    if (!(target.classList && (target.classList.contains('auto-exit-toggle') || target.classList.contains('auto-exit-target')))) {
      return;
    }
    var row = target.closest('tr');
    if (!row) {
      return;
    }
    var toggle = row.querySelector('.auto-exit-toggle');
    var input = row.querySelector('.auto-exit-target');
    if (!toggle || !input) {
      return;
    }
    var symbol = input.dataset.symbol || toggle.dataset.symbol;
    var longExchange = input.dataset.long || toggle.dataset.long;
    var shortExchange = input.dataset.short || toggle.dataset.short;
    if (!symbol || !longExchange || !shortExchange) {
      return;
    }
    var enabled = !!toggle.checked;
    var targetVal = parseFloat(input.value);
    if (enabled && (isNaN(targetVal) || !isFinite(targetVal))) {
      renderMessages(['Auto-exit target spread is required.']);
      return;
    }
    var payload = {
      symbol: symbol,
      long_exchange: longExchange,
      short_exchange: shortExchange,
      enabled: enabled,
      target_spread_pct: enabled ? targetVal : null
    };
    request('POST', '/api/auto-exit/rule', payload, function (err, data) {
      if (err) {
        renderMessages(['Auto-exit update failed: ' + err.message]);
        return;
      }
      if (data && data.auto_exit) {
        globalState.auto_exit = normalizeAutoExit(data.auto_exit);
        renderSymbolPositions(globalState.accounts.positions_by_symbol || []);
      }
    });
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
    var autoExitDefaults = collectAutoExitDefaults();
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
      submitAutoExitDefaults(autoExitDefaults, function (err2) {
        if (err2) {
          setSettingsStatus('Auto-exit settings error: ' + err2.message, 'error');
          return;
        }
        syncAutoExitDefaults(globalState.auto_exit);
        setSettingsStatus('Settings saved', 'success');
        window.setTimeout(function () {
          setSettingsStatus('', '');
        }, 2500);
      });
    });
  }

  function init() {
    globalState = normalizeState(globalState);
    syncSettingsForm(globalState.settings);
    syncAutoExitDefaults(globalState.auto_exit);
    renderAll();
    ensurePolling();

    if (elements.refreshButton) {
      elements.refreshButton.addEventListener('click', triggerManualRefresh);
    }
    if (elements.quickAnalyzeForm) {
      elements.quickAnalyzeForm.addEventListener('submit', handleQuickAnalyzeSubmit);
    }
    if (elements.settingsForm) {
      elements.settingsForm.addEventListener('submit', handleSettingsSubmit);
      elements.settingsForm.addEventListener('change', function () {
        setSettingsStatus('', '');
      });
    }
    if (elements.rebalanceAuto) {
      elements.rebalanceAuto.addEventListener('change', function () {
        toggleRebalanceFields(!!elements.rebalanceAuto.checked);
      });
    }
    if (elements.symbolPositionsTable) {
      elements.symbolPositionsTable.addEventListener('change', handleAutoExitChange);
    }
    if (elements.autoExitLogCopy) {
      elements.autoExitLogCopy.addEventListener('click', function () {
        var text = autoExitLogText((globalState.auto_exit && globalState.auto_exit.events) ? globalState.auto_exit.events : []);
        copyToClipboard(text);
      });
    }
    if (elements.autoExitAgentCopy) {
      elements.autoExitAgentCopy.addEventListener('click', function () {
        var text = autoExitAgentLogText(autoExitExecState);
        copyToClipboard(text);
      });
    }
    if (elements.autoExitAgentStop) {
      elements.autoExitAgentStop.addEventListener('click', function () {
        if (!autoExitExecState.execId) {
          return;
        }
        request('POST', '/api/manual/exec/' + encodeURIComponent(autoExitExecState.execId) + '/stop', null, function () {
          fetchAutoExitExec();
        });
      });
    }
    if (elements.autoExitAgentOpenLog) {
      elements.autoExitAgentOpenLog.addEventListener('click', function () {
        if (!autoExitExecState.execId) {
          return;
        }
        var url = '/api/manual/exec/' + encodeURIComponent(autoExitExecState.execId) + '/log';
        window.open(url, '_blank');
      });
    }

    if (!autoExitExecTimer) {
      autoExitExecTimer = window.setInterval(function () {
        if (autoExitExecState.execId) {
          fetchAutoExitExec();
        }
      }, 3000);
    }

    pollSnapshot(true);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
