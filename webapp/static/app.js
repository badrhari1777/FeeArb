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
    notification_primary_channel: 'ntfy',
    notification_fallback_channel: 'telegram',
    auto_margin_enabled: true,
    auto_margin_reduce_enabled: false,
    enforce_isolated_margin: true,
    enforce_leverage: true,
    target_leverage: 3,
    kucoin_isolated_topup_only: true,
    auto_rebalance_enabled: false,
    stop_gap_from_liq_pct: 0.025,
    stop_requote_threshold_pct: 0.0025,
    fallback_liq_factor_long: 0.33,
    fallback_liq_factor_short: 1.66,
    rebalance_delta_pct: 0.2,
    rebalance_cooldown_sec: 120,
    rebalance_limit_timeout_sec: 10,
    rebalance_limit_offset_bps: 2,
    rebalance_max_slippage_bps: 8,
    auto_derisk_enabled: false,
    auto_derisk_shadow_mode: true,
    orphan_cleanup_enabled: true,
    derisk_poll_sec: 5,
    derisk_target_buffer_pct: 0.3,
    derisk_warning_buffer_pct: 0.2,
    derisk_panic_buffer_pct: 0.15,
    derisk_recovery_buffer_pct: 0.35,
    derisk_min_free_balance_abs: 500,
    derisk_stale_positions_max_sec: 180,
    derisk_failure_block_count: 2,
    derisk_confirm_cycles: 2,
    derisk_cooldown_sec: 120,
    derisk_velocity_trigger_bps: 120,
    derisk_qty_tolerance_pct: 0.1,
    derisk_max_single_action_notional_usd: 500,
    derisk_market_cleanup_only_in_emergency: true,
    derisk_dust_notional_usd: 10,
    derisk_max_candidate_score: 0.25,
    derisk_preflight_ttl_sec: 60
  };

  var manualDefaults = {
    auto_exit_policy: {
      tier1: {
        chunk_notional_cap_usd: 350,
        market_cleanup_notional_cap_usd: 1500,
        edge_buffer_bps: 2
      },
      tier2: {
        chunk_notional_cap_usd: 250,
        market_cleanup_notional_cap_usd: 800,
        edge_buffer_bps: 4
      },
      lower_tier: {
        chunk_notional_cap_usd: 150,
        market_cleanup_notional_cap_usd: 0,
        edge_buffer_bps: 8
      }
    }
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
    protective: clone(protectiveDefaults),
    manual: clone(manualDefaults)
  };

  var defaultExecution = {
    wallets: [],
    reservations: [],
    positions: [],
    telemetry: []
  };

  var defaultAccounts = {
    balances: [],
    balance_summary: {},
    status: [],
    positions: [],
    positions_by_symbol: [],
    margin_diagnostics: [],
    margin_logic_log: [],
    exchange_health: {},
    hedge_clusters: { rules: {} },
    derisk_diagnostics: [],
    derisk_events: [],
    last_updated: null,
    positions_market: null
  };

  var defaultAutoExit = {
    defaults: {
      max_runtime_sec: 600,
      cooldown_sec: 300,
      require_live: true,
      auto_clear_no_position_sec: 120,
      restore_spread_on_missing: true
    },
    rules: {},
    live_spreads: {},
    diagnostics: [],
    v1_diagnostics: [],
    events: []
  };

  var defaultAutoArb = {
    mode: 'live',
    live_limits: {},
    rules: []
  };

  var defaultAutoStrategies = {
    mode: 'live',
    defaults: { completion_tolerance_pct: 1 },
    strategies: [],
    queue: [],
    running: null,
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
    auto_exit: clone(defaultAutoExit),
    auto_arb: clone(defaultAutoArb),
    auto_strategies: clone(defaultAutoStrategies)
  };

  var globalState = normalizeState(window.__INITIAL_STATE__);
  var pollingTimer = null;
  var currentPollInterval = 0;
  var pollingInFlight = false;
  var positionsOverviewState = null;
  var positionsOverviewInFlight = false;
  var positionsActiveTab = 'all';
  var autoExitExecState = {
    execId: null,
    status: null,
    logs: [],
    errors: [],
    error: null,
    logPath: null,
    lastFetched: 0
  };
  var activePositionActions = {};
  var staleAutoExitExecIds = {};
  var autoExitExecTimer = null;
  var autoAgentContext = null;

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
    notificationPrimary: document.getElementById('notification-primary'),
    notificationFallback: document.getElementById('notification-fallback'),
    alertMargin: document.getElementById('alert-margin'),
    autoMarginAdd: document.getElementById('auto-margin-add'),
    autoMarginReduce: document.getElementById('auto-margin-reduce'),
    enforceIsolatedMargin: document.getElementById('enforce-isolated-margin'),
    enforceLeverage: document.getElementById('enforce-leverage'),
    targetLeverage: document.getElementById('target-leverage'),
    kucoinTopupOnly: document.getElementById('kucoin-topup-only'),
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
    deriskEnabled: document.getElementById('derisk-enabled'),
    deriskShadowMode: document.getElementById('derisk-shadow-mode'),
    orphanCleanupEnabled: document.getElementById('orphan-cleanup-enabled'),
    deriskPollSec: document.getElementById('derisk-poll-sec'),
    deriskTargetBuffer: document.getElementById('derisk-target-buffer'),
    deriskWarningBuffer: document.getElementById('derisk-warning-buffer'),
    deriskPanicBuffer: document.getElementById('derisk-panic-buffer'),
    deriskRecoveryBuffer: document.getElementById('derisk-recovery-buffer'),
    deriskMinFreeBalance: document.getElementById('derisk-min-free-balance'),
    deriskStaleMax: document.getElementById('derisk-stale-max'),
    deriskFailureBlock: document.getElementById('derisk-failure-block'),
    deriskConfirmCycles: document.getElementById('derisk-confirm-cycles'),
    deriskCooldownSec: document.getElementById('derisk-cooldown-sec'),
    deriskVelocityBps: document.getElementById('derisk-velocity-bps'),
    deriskQtyTolerance: document.getElementById('derisk-qty-tolerance'),
    deriskMaxActionNotional: document.getElementById('derisk-max-action-notional'),
    deriskDustNotional: document.getElementById('derisk-dust-notional'),
    deriskMaxCandidateScore: document.getElementById('derisk-max-candidate-score'),
    deriskPreflightTtl: document.getElementById('derisk-preflight-ttl'),
    deriskMarketCleanupOnlyEmergency: document.getElementById('derisk-market-cleanup-only-emergency'),
    autoExitRuntimeInput: document.getElementById('auto-exit-runtime'),
    autoExitCooldownInput: document.getElementById('auto-exit-cooldown'),
    autoExitRequireLive: document.getElementById('auto-exit-require-live'),
    autoExitRestoreSpread: document.getElementById('auto-exit-restore-spread'),
    autoExitClearSpreadCache: document.getElementById('auto-exit-clear-spread-cache'),
    autoExitTier1ChunkCap: document.getElementById('auto-exit-tier1-chunk-cap'),
    autoExitTier1CleanupCap: document.getElementById('auto-exit-tier1-cleanup-cap'),
    autoExitTier1EdgeBuffer: document.getElementById('auto-exit-tier1-edge-buffer'),
    autoExitTier2ChunkCap: document.getElementById('auto-exit-tier2-chunk-cap'),
    autoExitTier2CleanupCap: document.getElementById('auto-exit-tier2-cleanup-cap'),
    autoExitTier2EdgeBuffer: document.getElementById('auto-exit-tier2-edge-buffer'),
    autoExitLowerChunkCap: document.getElementById('auto-exit-lower-chunk-cap'),
    autoExitLowerCleanupCap: document.getElementById('auto-exit-lower-cleanup-cap'),
    autoExitLowerEdgeBuffer: document.getElementById('auto-exit-lower-edge-buffer'),
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
    balanceSummaryOverall: document.getElementById('balance-summary-overall'),
    balanceSummaryOverallAvailable: document.getElementById('balance-summary-overall-available'),
    balanceSummaryBybitMain: document.getElementById('balance-summary-bybit-main'),
    balanceSummaryBybitMainAvailable: document.getElementById('balance-summary-bybit-main-available'),
    balanceSummaryBybitPump: document.getElementById('balance-summary-bybit-pump'),
    balanceSummaryBybitPumpAvailable: document.getElementById('balance-summary-bybit-pump-available'),
    balanceSummaryBybitPumpTemporary: document.getElementById('balance-summary-bybit-pump-temporary'),
    balanceSummaryBybitCombined: document.getElementById('balance-summary-bybit-combined'),
    balanceSummaryBybitCombinedAvailable: document.getElementById('balance-summary-bybit-combined-available'),
    balanceSummaryBybitCombinedTemporary: document.getElementById('balance-summary-bybit-combined-temporary'),
    symbolPositionsTable: document.getElementById('symbol-positions-body'),
    symbolPositionsMeta: document.getElementById('symbol-positions-meta'),
    symbolPositionsDiffs: document.getElementById('symbol-positions-diffs'),
    positionsSummaryMain: document.getElementById('positions-summary-main'),
    positionsSummaryPump: document.getElementById('positions-summary-pump'),
    positionsSummaryPnl: document.getElementById('positions-summary-pnl'),
    positionsSummaryLiq: document.getElementById('positions-summary-liq'),
    positionsSummaryProtection: document.getElementById('positions-summary-protection'),
    positionsSummaryPumpCycle: document.getElementById('positions-summary-pump-cycle'),
    positionsPumpBody: document.getElementById('positions-pump-body'),
    positionsTabs: document.querySelectorAll('[data-positions-tab]'),
    positionsPanes: document.querySelectorAll('[data-positions-pane]'),
    gridStrategiesList: document.getElementById('grid-strategies-list'),
    liveStrategiesList: document.getElementById('live-strategies-list'),
    marginDiagnosticsBody: document.getElementById('margin-diagnostics-body'),
    marginDiagnosticsEmpty: document.getElementById('margin-diagnostics-empty'),
    marginLogicLog: document.getElementById('margin-logic-log'),
    marginLogicLogEmpty: document.getElementById('margin-logic-log-empty'),
    deriskExchangeHealthBody: document.getElementById('derisk-exchange-health-body'),
    hedgeClusterForm: document.getElementById('hedge-cluster-form'),
    hedgeClusterStatus: document.getElementById('hedge-cluster-status'),
    hedgeClusterSymbol: document.getElementById('hedge-cluster-symbol'),
    hedgeClusterKind: document.getElementById('hedge-cluster-kind'),
    hedgeClusterLongExchange: document.getElementById('hedge-cluster-long-exchange'),
    hedgeClusterShortExchange: document.getElementById('hedge-cluster-short-exchange'),
    hedgeClusterExchange: document.getElementById('hedge-cluster-exchange'),
    hedgeClusterSide: document.getElementById('hedge-cluster-side'),
    hedgeClusterQtyTolerance: document.getElementById('hedge-cluster-qty-tolerance'),
    hedgeClusterEnabled: document.getElementById('hedge-cluster-enabled'),
    hedgeClusterRehedge: document.getElementById('hedge-cluster-rehedge'),
    hedgeClustersBody: document.getElementById('hedge-clusters-body'),
    deriskDiagnosticsBody: document.getElementById('derisk-diagnostics-body'),
    deriskDiagnosticsEmpty: document.getElementById('derisk-diagnostics-empty'),
    deriskEventLog: document.getElementById('derisk-event-log'),
    deriskEventLogEmpty: document.getElementById('derisk-event-log-empty'),
    autoExitLog: document.getElementById('auto-exit-log'),
    autoExitLogEmpty: document.getElementById('auto-exit-log-empty'),
    autoExitLogCopy: document.getElementById('auto-exit-log-copy'),
    autoExitDiagnosticsBody: document.getElementById('auto-exit-diagnostics-body'),
    autoExitDiagnosticsEmpty: document.getElementById('auto-exit-diagnostics-empty'),
    autoExitV1DiagnosticsBody: document.getElementById('auto-exit-v1-diagnostics-body'),
    autoExitV1DiagnosticsEmpty: document.getElementById('auto-exit-v1-diagnostics-empty'),
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

  function mergeAutoExitPolicy(policy) {
    var merged = clone(manualDefaults.auto_exit_policy) || {};
    var tierKey;
    var fieldName;
    var defaults;
    var section;
    if (!policy || typeof policy !== 'object') {
      return merged;
    }
    for (tierKey in merged) {
      if (!Object.prototype.hasOwnProperty.call(merged, tierKey)) {
        continue;
      }
      defaults = merged[tierKey] || {};
      section = policy[tierKey];
      if (!section || typeof section !== 'object') {
        continue;
      }
      for (fieldName in defaults) {
        if (Object.prototype.hasOwnProperty.call(defaults, fieldName) && section[fieldName] !== undefined && section[fieldName] !== null && section[fieldName] !== '') {
          var numericValue = parseFloat(section[fieldName]);
          if (!isNaN(numericValue)) {
            merged[tierKey][fieldName] = numericValue;
          }
        }
      }
    }
    return merged;
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
      var incomingManual = (settings && typeof settings.manual === 'object') ? settings.manual : null;
      var mergedManual = clone(manualDefaults) || {};
      if (incomingManual) {
        for (key in incomingManual) {
          if (Object.prototype.hasOwnProperty.call(incomingManual, key) && key !== 'auto_exit_policy') {
            mergedManual[key] = clone(incomingManual[key]);
          }
        }
      }
      mergedManual.auto_exit_policy = mergeAutoExitPolicy(incomingManual ? incomingManual.auto_exit_policy : null);
      normalized.manual = mergedManual;
    } else {
      normalized.manual = clone(manualDefaults) || {};
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
      balance_summary: {},
      status: [],
      positions: [],
      positions_by_symbol: [],
      margin_diagnostics: [],
      margin_logic_log: [],
      exchange_health: {},
      hedge_clusters: { rules: {} },
      derisk_diagnostics: [],
      derisk_events: [],
      last_updated: null,
      positions_market: null
    };
    if (!accounts || typeof accounts !== 'object') {
      return normalized;
    }
    if (Array.isArray(accounts.balances)) {
      normalized.balances = clone(accounts.balances) || [];
    }
    if (accounts.balance_summary && typeof accounts.balance_summary === 'object') {
      normalized.balance_summary = clone(accounts.balance_summary) || {};
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
    if (Array.isArray(accounts.margin_diagnostics)) {
      normalized.margin_diagnostics = clone(accounts.margin_diagnostics) || [];
    }
    if (Array.isArray(accounts.margin_logic_log)) {
      normalized.margin_logic_log = clone(accounts.margin_logic_log) || [];
    }
    if (accounts.exchange_health && typeof accounts.exchange_health === 'object') {
      normalized.exchange_health = clone(accounts.exchange_health) || {};
    }
    if (accounts.hedge_clusters && typeof accounts.hedge_clusters === 'object') {
      normalized.hedge_clusters = clone(accounts.hedge_clusters) || { rules: {} };
    }
    if (Array.isArray(accounts.derisk_diagnostics)) {
      normalized.derisk_diagnostics = clone(accounts.derisk_diagnostics) || [];
    }
    if (Array.isArray(accounts.derisk_events)) {
      normalized.derisk_events = clone(accounts.derisk_events) || [];
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
      if (config.defaults.auto_clear_no_position_sec !== undefined && config.defaults.auto_clear_no_position_sec !== null) {
        var autoClearVal = parseInt(config.defaults.auto_clear_no_position_sec, 10);
        if (!isNaN(autoClearVal)) {
          normalized.defaults.auto_clear_no_position_sec = Math.max(0, autoClearVal);
        }
      }
      if (config.defaults.restore_spread_on_missing !== undefined && config.defaults.restore_spread_on_missing !== null) {
        normalized.defaults.restore_spread_on_missing = !!config.defaults.restore_spread_on_missing;
      }
    }
    if (config.rules && typeof config.rules === 'object') {
      normalized.rules = clone(config.rules) || {};
    }
    if (config.live_spreads && typeof config.live_spreads === 'object') {
      normalized.live_spreads = clone(config.live_spreads) || {};
    }
    if (Array.isArray(config.diagnostics)) {
      normalized.diagnostics = clone(config.diagnostics) || [];
    }
    if (Array.isArray(config.v1_diagnostics)) {
      normalized.v1_diagnostics = clone(config.v1_diagnostics) || [];
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
    state.auto_arb = source && source.auto_arb ? clone(source.auto_arb) : clone(defaultAutoArb);
    state.auto_strategies = source && source.auto_strategies
      ? clone(source.auto_strategies)
      : clone(defaultAutoStrategies);
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

  function formatTrimmedNumber(value, maxDigits, minDigits) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(number)) {
      return '-';
    }
    var maxPlaces = typeof maxDigits === 'number' ? maxDigits : 2;
    var minPlaces = typeof minDigits === 'number' ? minDigits : 0;
    var text = number.toFixed(maxPlaces);
    if (maxPlaces <= minPlaces || text.indexOf('.') === -1) {
      return text;
    }
    var parts = text.split('.');
    var fraction = parts[1] || '';
    while (fraction.length > minPlaces && fraction.charAt(fraction.length - 1) === '0') {
      fraction = fraction.slice(0, -1);
    }
    if (!fraction.length) {
      return parts[0];
    }
    return parts[0] + '.' + fraction;
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
      var statusText = row.status || 'ok';
      var noteText = row.error || row.message || '-';
      html += '<tr>' +
        '<td>' + escapeHtml(row.exchange || '-') + '</td>' +
        '<td>' + escapeHtml(row.account_label || 'Main account') + '</td>' +
        '<td>' + escapeHtml(row.asset || '-') + '</td>' +
        '<td>' + formatNumber(row.total, 2) + '</td>' +
        '<td>' + formatNumber(row.available, 2) + '</td>' +
        '<td>' + formatNumber(row.used, 2) + '</td>' +
        '<td>' + formatNumber(row.temporary_occupied_usd, 2) + '</td>' +
        '<td>' + marginRatioText + '</td>' +
        '<td>' + equityText + '</td>' +
        '<td>' + bufferText + '</td>' +
        '<td>' + escapeHtml(statusText) + '</td>' +
        '<td>' + escapeHtml(noteText) + '</td>' +
        '<td>' + escapeHtml(formatDate(row.timestamp || row.updated_at)) + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="13" class="muted">Balances will appear after the first refresh.</td></tr>';
    }
    elements.accountBalanceTable.innerHTML = html;
  }

  function renderBalanceSummary(summary) {
    var data = summary || {};
    var mappings = [
      [elements.balanceSummaryOverall, elements.balanceSummaryOverallAvailable, data.overall],
      [elements.balanceSummaryBybitMain, elements.balanceSummaryBybitMainAvailable, data.bybit_main],
      [elements.balanceSummaryBybitPump, elements.balanceSummaryBybitPumpAvailable, data.bybit_pump],
      [elements.balanceSummaryBybitCombined, elements.balanceSummaryBybitCombinedAvailable, data.bybit_combined]
    ];
    mappings.forEach(function (mapping) {
      var totalNode = mapping[0];
      var availableNode = mapping[1];
      var row = mapping[2] || {};
      if (totalNode) {
        totalNode.textContent = typeof row.total === 'number' ? formatNumber(row.total, 2) : '-';
      }
      if (availableNode) {
        availableNode.textContent = 'Available ' +
          (typeof row.available === 'number' ? formatNumber(row.available, 2) : '-') +
          ' USDT';
      }
    });
    var temporaryMappings = [
      [elements.balanceSummaryBybitPumpTemporary, data.bybit_pump],
      [elements.balanceSummaryBybitCombinedTemporary, data.bybit_combined]
    ];
    temporaryMappings.forEach(function (mapping) {
      var node = mapping[0];
      var row = mapping[1] || {};
      if (node) {
        node.textContent = 'Temporarily occupied ' +
          (typeof row.temporary_occupied_usd === 'number' ? formatNumber(row.temporary_occupied_usd, 2) : '-') +
          ' USDT';
      }
    });
  }

  function renderMarginDiagnostics(entries) {
    if (!elements.marginDiagnosticsBody || !elements.marginDiagnosticsEmpty) {
      return;
    }
    var rows = entries || [];
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var liqBufferText = row.liq_buffer_pct !== null && row.liq_buffer_pct !== undefined
        ? formatNumber(row.liq_buffer_pct, 2) + '%'
        : '-';
      var levText = row.leverage !== null && row.leverage !== undefined
        ? formatNumber(row.leverage, 2)
        : '-';
      var targetLevText = row.target_leverage !== null && row.target_leverage !== undefined
        ? formatNumber(row.target_leverage, 2)
        : '-';
      var decisionStatus = 'unknown';
      if (row.decision === 'add_margin') {
        decisionStatus = 'ok';
      } else if (row.decision === 'reduce_margin' || row.decision === 'blocked') {
        decisionStatus = 'error';
      } else if (row.decision === 'observe' || row.decision === 'set_mode') {
        decisionStatus = 'pending';
      }
      html += '<tr>' +
        '<td>' + escapeHtml(row.exchange || '-') + '</td>' +
        '<td>' + escapeHtml(row.symbol || '-') + '</td>' +
        '<td>' + escapeHtml(row.side || '-') + '</td>' +
        '<td>' + escapeHtml(row.margin_mode || '-') + '</td>' +
        '<td>' + levText + '</td>' +
        '<td>' + escapeHtml(row.leverage_source || '-') + '</td>' +
        '<td>' + targetLevText + '</td>' +
        '<td>' + liqBufferText + '</td>' +
        '<td>' + formatNumber(row.base_margin_est, 2) + '</td>' +
        '<td>' + formatNumber(row.min_required_margin_est, 2) + '</td>' +
        '<td>' + formatNumber(row.max_add_est, 2) + '</td>' +
        '<td>' + formatNumber(row.max_reduce_est, 2) + '</td>' +
        '<td><span class="status-chip status-chip--' + decisionStatus + '">' + escapeHtml(row.decision || '-') + '</span></td>' +
        '<td>' + escapeHtml(row.reason || '-') + '</td>' +
        '<td>' + escapeHtml(formatDate(row.updated_at)) + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="15" class="muted">Margin diagnostics will appear after the first refresh.</td></tr>';
      elements.marginDiagnosticsEmpty.style.display = '';
    } else {
      elements.marginDiagnosticsEmpty.style.display = 'none';
    }
    elements.marginDiagnosticsBody.innerHTML = html;
  }

  function formatMarginLogicEvent(entry) {
    if (!entry || typeof entry !== 'object') {
      return '-';
    }
    if (entry.event === 'action') {
      var amount = entry.amount !== undefined && entry.amount !== null ? formatNumber(entry.amount, 2) : '-';
      var buffer = entry.buffer_pct !== undefined && entry.buffer_pct !== null ? formatNumber(entry.buffer_pct, 2) + '%' : '-';
      var targetBuffer = entry.target_buffer_pct !== undefined && entry.target_buffer_pct !== null ? formatNumber(entry.target_buffer_pct, 2) + '%' : '-';
      return (entry.exchange || '-') + ' ' + (entry.symbol || '-') + ' ' + (entry.side || '-') +
        ' action=' + (entry.action || '-') + ' amount=' + amount + ' buffer=' + buffer + ' target=' + targetBuffer;
    }
    var leverage = entry.leverage !== undefined && entry.leverage !== null ? formatNumber(entry.leverage, 2) : '-';
    var liqBuffer = entry.liq_buffer_pct !== undefined && entry.liq_buffer_pct !== null ? formatNumber(entry.liq_buffer_pct, 2) + '%' : '-';
    return (entry.exchange || '-') + ' ' + (entry.symbol || '-') + ' ' + (entry.side || '-') +
      ' decision=' + (entry.decision || '-') + ' reason=' + (entry.reason || '-') +
      ' lev=' + leverage + ' buffer=' + liqBuffer + ' mode=' + (entry.margin_mode || '-');
  }

  function renderMarginLogicLog(entries) {
    if (!elements.marginLogicLog || !elements.marginLogicLogEmpty) {
      return;
    }
    var rows = entries || [];
    if (!rows.length) {
      elements.marginLogicLog.innerHTML = '';
      elements.marginLogicLogEmpty.style.display = '';
      return;
    }
    elements.marginLogicLogEmpty.style.display = 'none';
    var html = '';
    var i;
    var start = rows.length > MAX_RENDERED_EVENTS ? rows.length - MAX_RENDERED_EVENTS : 0;
    for (i = rows.length - 1; i >= start; i -= 1) {
      var row = rows[i] || {};
      html += '<li class="event-log__item"><span class="event-log__time">' +
        escapeHtml(formatDate(row.timestamp)) + '</span><span class="event-log__message">' +
        escapeHtml(formatMarginLogicEvent(row)) + '</span></li>';
    }
    elements.marginLogicLog.innerHTML = html;
  }

  function statusChipClass(status) {
    var slug = String(status || 'unknown').toLowerCase();
    if (slug === 'healthy' || slug === 'ok' || slug === 'closable_normal' || slug === 'flat' || slug === 'ranked') {
      return 'status-chip status-chip--ok';
    }
    if (slug === 'stress' || slug === 'pending' || slug === 'suspected_orphan' || slug === 'stale') {
      return 'status-chip status-chip--pending';
    }
    if (slug === 'panic' || slug === 'confirmed_orphan' || slug === 'blocked_by_exchange_health' || slug === 'untrusted' || slug === 'degraded') {
      return 'status-chip status-chip--error';
    }
    return 'status-chip status-chip--unknown';
  }

  function formatSignedMoney(value) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(number)) {
      return '-';
    }
    if (number > 0) {
      return '+' + formatTrimmedNumber(number, 2);
    }
    return formatTrimmedNumber(number, 2);
  }

  function renderDeriskExchangeHealth(entries) {
    if (!elements.deriskExchangeHealthBody) {
      return;
    }
    var html = '';
    var keys = [];
    var key;
    var payload = entries || {};
    for (key in payload) {
      if (Object.prototype.hasOwnProperty.call(payload, key)) {
        keys.push(key);
      }
    }
    keys.sort();
    var i;
    for (i = 0; i < keys.length; i += 1) {
      var exchange = keys[i];
      var row = payload[exchange] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(exchange) + '</td>' +
        '<td><span class="' + statusChipClass(row.health) + '">' + escapeHtml(row.health || '-') + '</span></td>' +
        '<td>' + escapeHtml(row.last_status || '-') + '</td>' +
        '<td>' + escapeHtml(String(row.consecutive_failures !== undefined && row.consecutive_failures !== null ? row.consecutive_failures : '-')) + '</td>' +
        '<td>' + (row.stale_sec !== undefined && row.stale_sec !== null ? formatNumber(row.stale_sec, 1) : '-') + '</td>' +
        '<td>' + escapeHtml(row.last_error_kind || '-') + '</td>' +
        '<td>' + escapeHtml(row.last_error || '-') + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="7" class="muted">No exchange health data yet.</td></tr>';
    }
    elements.deriskExchangeHealthBody.innerHTML = html;
  }

  function renderHedgeClusters(payload) {
    if (!elements.hedgeClustersBody) {
      return;
    }
    var rules = payload && payload.rules && typeof payload.rules === 'object' ? payload.rules : {};
    var rows = [];
    var key;
    for (key in rules) {
      if (Object.prototype.hasOwnProperty.call(rules, key)) {
        var item = clone(rules[key]) || {};
        item._key = key;
        rows.push(item);
      }
    }
    rows.sort(function (a, b) {
      var ak = String(a.symbol || '') + '|' + String(a.kind || '') + '|' + String(a.long_exchange || a.exchange || '');
      var bk = String(b.symbol || '') + '|' + String(b.kind || '') + '|' + String(b.long_exchange || b.exchange || '');
      if (ak < bk) {
        return -1;
      }
      if (ak > bk) {
        return 1;
      }
      return 0;
    });
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var pairText = row.kind === 'standalone'
        ? String(row.exchange || '-')
        : String(row.long_exchange || '-') + ' / ' + String(row.short_exchange || '-');
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol || '-') + '</td>' +
        '<td>' + escapeHtml(row.kind || '-') + '</td>' +
        '<td>' + escapeHtml(pairText) + '</td>' +
        '<td>' + escapeHtml(row.side || '-') + '</td>' +
        '<td><span class="' + statusChipClass(row.enabled ? 'ok' : 'unknown') + '">' + escapeHtml(row.enabled ? 'on' : 'off') + '</span></td>' +
        '<td>' + (row.qty_tolerance_pct !== undefined && row.qty_tolerance_pct !== null ? formatNumber(row.qty_tolerance_pct, 2) : '-') + '</td>' +
        '<td>' + escapeHtml(row.rehedge_allowed ? 'yes' : 'no') + '</td>' +
        '<td>' + escapeHtml(row.source || '-') + '</td>' +
        '<td>' + escapeHtml(formatDate(row.updated_at)) + '</td>' +
      '</tr>';
    }
    if (!html) {
      html = '<tr><td colspan="9" class="muted">No hedge clusters yet.</td></tr>';
    }
    elements.hedgeClustersBody.innerHTML = html;
  }

  function renderDeriskDiagnostics(entries) {
    if (!elements.deriskDiagnosticsBody || !elements.deriskDiagnosticsEmpty) {
      return;
    }
    var rows = entries || [];
    if (!rows.length) {
      elements.deriskDiagnosticsBody.innerHTML = '<tr><td colspan="16" class="muted">No emergency de-risk diagnostics yet.</td></tr>';
      elements.deriskDiagnosticsEmpty.style.display = '';
      return;
    }
    elements.deriskDiagnosticsEmpty.style.display = 'none';
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var healthText = String(row.long_health || '-') + ' / ' + String(row.short_health || '-');
      var stressText = row.stress_exchange
        ? String(row.stress_exchange) + ' ' + String(row.stress_status || '-')
        : '-';
      html += '<tr>' +
        '<td>' + escapeHtml(row.kind || '-') + '</td>' +
        '<td>' + escapeHtml(row.symbol || '-') + '</td>' +
        '<td><span class="' + statusChipClass(row.status) + '">' + escapeHtml(row.status || '-') + '</span></td>' +
        '<td>' + escapeHtml(row.reason || '-') + '</td>' +
        '<td>' + escapeHtml(row.long_exchange || '-') + '</td>' +
        '<td>' + escapeHtml(row.short_exchange || '-') + '</td>' +
        '<td>' + escapeHtml(healthText) + '</td>' +
        '<td>' + escapeHtml(String(row.missing_cycles !== undefined && row.missing_cycles !== null ? row.missing_cycles : '-')) + '</td>' +
        '<td>' + escapeHtml(stressText) + '</td>' +
        '<td>' + (row.action_qty !== undefined && row.action_qty !== null ? formatTrimmedNumber(row.action_qty, 6) : '-') + '</td>' +
        '<td>' + escapeHtml(row.action_mode || '-') + '</td>' +
        '<td>' + (row.candidate_score !== undefined && row.candidate_score !== null ? formatNumber(row.candidate_score, 3) : '-') + '</td>' +
        '<td><span class="' + statusChipClass(row.residual_status) + '">' + escapeHtml(row.residual_status || '-') + '</span></td>' +
        '<td>' + formatSignedMoney(row.funding_to_next_usd) + '</td>' +
        '<td>' + formatSignedMoney(row.cluster_unrealized_pnl_usd) + '</td>' +
        '<td>' + escapeHtml(formatDate(row.updated_at)) + '</td>' +
      '</tr>';
    }
    elements.deriskDiagnosticsBody.innerHTML = html;
  }

  function formatDeriskEvent(entry) {
    if (!entry || !entry.event) {
      return '-';
    }
    if (entry.event === 'cluster_status') {
      return String(entry.symbol || '-') + ' status=' + String(entry.status || '-') +
        ' reason=' + String(entry.reason || '-') +
        ' pair=' + String(entry.long_exchange || '-') + '/' + String(entry.short_exchange || '-') +
        ' health=' + String(entry.long_health || '-') + '/' + String(entry.short_health || '-');
    }
    if (entry.event === 'preempt_requested') {
      return String(entry.symbol || '-') + ' requested preempt exec_id=' + String(entry.execution_id || '-') +
        ' reason=' + String(entry.reason || '-');
    }
    if (entry.event === 'trigger') {
      return String(entry.symbol || '-') + ' trigger venue=' + String(entry.stress_exchange || '-') +
        ' status=' + String(entry.stress_status || '-') +
        ' qty=' + formatTrimmedNumber(entry.action_qty, 6) +
        ' score=' + formatNumber(entry.candidate_score, 3);
    }
    return String(entry.event);
  }

  function renderDeriskEvents(entries) {
    if (!elements.deriskEventLog || !elements.deriskEventLogEmpty) {
      return;
    }
    var rows = entries || [];
    if (!rows.length) {
      elements.deriskEventLog.innerHTML = '';
      elements.deriskEventLogEmpty.style.display = '';
      return;
    }
    elements.deriskEventLogEmpty.style.display = 'none';
    var html = '';
    var start = rows.length > MAX_RENDERED_EVENTS ? rows.length - MAX_RENDERED_EVENTS : 0;
    var i;
    for (i = rows.length - 1; i >= start; i -= 1) {
      var row = rows[i] || {};
      html += '<li class="event-log__item"><span class="event-log__time">' +
        escapeHtml(formatDate(row.ts)) + '</span><span class="event-log__message">' +
        escapeHtml(formatDeriskEvent(row)) + '</span></li>';
    }
    elements.deriskEventLog.innerHTML = html;
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

  function activeAutoExitRulesForSymbol(symbol) {
    var symbolKey = String(symbol || '').toUpperCase();
    var rules = (globalState.auto_exit && globalState.auto_exit.rules) ? globalState.auto_exit.rules : {};
    return Object.keys(rules || {}).map(function (key) {
      return { key: key, rule: rules[key] };
    }).filter(function (item) {
      var rule = item.rule || {};
      return String(rule.symbol || '').toUpperCase() === symbolKey && (rule.enabled || rule.v1_enabled);
    });
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
      if (entry.trigger_mode) {
        triggerMsg += ' mode=' + entry.trigger_mode;
      }
      if (entry.v1_reason) {
        triggerMsg += ' reason=' + entry.v1_reason;
      }
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
      var startMsg = 'Started ' + (entry.symbol || '-') + ' exec_id=' + execId;
      if (entry.trigger_mode) {
        startMsg += ' mode=' + entry.trigger_mode;
      }
      if (entry.v1_reason) {
        startMsg += ' reason=' + entry.v1_reason;
      }
      return startMsg;
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
        : formatNumber(
            row.current_mark_price !== null && row.current_mark_price !== undefined
              ? row.current_mark_price
              : null,
            6
          );
      var fundingText = row.funding_rate !== null && row.funding_rate !== undefined
        ? formatPercent(row.funding_rate, 4)
        : '-';
      if (!isSummary && fundingText !== '-' && row.funding_interval_hours !== null && row.funding_interval_hours !== undefined) {
        fundingText += ' / ' + formatTrimmedNumber(row.funding_interval_hours, 2) + 'h';
      }
      var liqPriceText = isSummary ? '-' : formatNumber(row.liquidation_price, 4);
      var liqDistText = row.dist_to_liq_pct !== null && row.dist_to_liq_pct !== undefined
        ? formatNumber(row.dist_to_liq_pct, 3) + '%'
        : '-';
      var leverageText = isSummary ? '-' : formatNumber(row.leverage, 2);
      var expectedFundingText = row.expected_funding !== null && row.expected_funding !== undefined
        ? formatTrimmedNumber(row.expected_funding, 3)
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
      var positionAction = '-';
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
          var symbolRules = activeAutoExitRulesForSymbol(row.symbol);
          var displayedStoredRule = false;
          if ((!rule || (!rule.enabled && !rule.v1_enabled)) && symbolRules.length) {
            rule = symbolRules[0].rule;
            ruleLong = rule.long_exchange;
            ruleShort = rule.short_exchange;
            displayedStoredRule = true;
          }
          var spreadEnabled = rule && rule.enabled;
          var v1Enabled = rule && rule.v1_enabled;
          var targetVal = rule && rule.target_spread_pct !== undefined && rule.target_spread_pct !== null
            ? formatNumber(rule.target_spread_pct, 2)
            : '';
          var exitPercent = rule && rule.exit_percent !== undefined && rule.exit_percent !== null
            ? parseFloat(rule.exit_percent)
            : 100;
          if (!isFinite(exitPercent) || exitPercent <= 0 || exitPercent > 100) {
            exitPercent = 100;
          }
          var liveSpread = autoExitLiveSpreadFor(row.symbol, ruleLong, ruleShort);
          liveSpreadText = liveSpread !== null && liveSpread !== undefined
            ? formatNumber(liveSpread, 2) + '%'
            : (isMultileg ? '<span class="muted">n/a</span>' : '-');
          var key = autoExitKey(row.symbol, ruleLong, ruleShort);
          var spreadCheckbox = '<label class="inline-toggle"><input type="checkbox" class="auto-exit-toggle" data-key="' + escapeHtml(key) + '" data-symbol="' +
            escapeHtml(row.symbol || '') + '" data-long="' + escapeHtml(ruleLong) + '" data-short="' + escapeHtml(ruleShort) + '"' +
            (spreadEnabled ? ' checked' : '') + ' /> spread</label>';
          var v1Checkbox = '<label class="inline-toggle"><input type="checkbox" class="auto-exit-v1-toggle" data-key="' + escapeHtml(key) + '" data-symbol="' +
            escapeHtml(row.symbol || '') + '" data-long="' + escapeHtml(ruleLong) + '" data-short="' + escapeHtml(ruleShort) + '"' +
            (v1Enabled ? ' checked' : '') + ' /> v1</label>';
          var input = '<input type="number" class="auto-exit-target" step="0.01" placeholder="-7.9" value="' + escapeHtml(targetVal) +
            '" data-key="' + escapeHtml(key) + '" data-symbol="' + escapeHtml(row.symbol || '') + '" data-long="' +
            escapeHtml(ruleLong) + '" data-short="' + escapeHtml(ruleShort) + '" />';
          var percentSelect = '<input type="number" min="1" max="100" step="1" class="auto-exit-percent" value="' +
            escapeHtml(exitPercent) + '" data-key="' + escapeHtml(key) + '" data-symbol="' +
            escapeHtml(row.symbol || '') + '" data-long="' + escapeHtml(ruleLong) + '" data-short="' + escapeHtml(ruleShort) + '" />';
          autoExitToggle = spreadCheckbox + ' ' + v1Checkbox + ' ' + percentSelect + ' <span class="muted">once</span>' +
            (isMultileg ? ' <span class="muted">multi-leg</span>' : '') +
            (displayedStoredRule ? ' <span class="muted">stored ' + escapeHtml(ruleLong + '/' + ruleShort) + '</span>' : '') +
            (symbolRules.length > 1 ? ' <span class="muted">' + symbolRules.length + ' active rules</span>' : '');
          var linkedExitStrategies = ((globalState.auto_strategies || {}).strategies || []).filter(function (strategy) {
            return strategy.enabled && strategy.type === 'exit_ladder' &&
              String(strategy.symbol || '').toUpperCase() === String(row.symbol || '').toUpperCase();
          });
          if (linkedExitStrategies.length) {
            autoExitToggle += ' <a class="cell-note" href="/strategies">ladder: ' +
              escapeHtml(linkedExitStrategies.length) + '</a>';
          }
          autoExitTarget = input;
          var actionLongExchange = (isMultileg ? (row.selected_long_exchange || '') : longEx) || '';
          var actionShortExchange = (isMultileg ? (row.selected_short_exchange || '') : shortEx) || '';
          var actionRunning = !!activePositionActions[positionActionKey(row.symbol || '', actionLongExchange, actionShortExchange)];
          positionAction = '<div class="position-action-controls" data-symbol="' + escapeHtml(row.symbol || '') +
            '" data-long="' + escapeHtml(actionLongExchange) +
            '" data-short="' + escapeHtml(actionShortExchange) + '">' +
            '<input type="number" min="1" max="100" step="1" value="100" class="position-action-percent" title="Percent of hedged coin quantity" />' +
            '<button type="button" class="position-action-btn" data-action="add"' + (actionRunning ? ' disabled' : '') + '>Add</button>' +
            '<button type="button" class="position-action-btn" data-action="exit"' + (actionRunning ? ' disabled' : '') + '>Exit</button>' +
            (actionRunning ? '<span class="muted">running</span>' : '') + '</div>';
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
        '<td>' + formatTrimmedNumber(row.quantity, 2) + '</td>' +
        '<td>' + formatNumber(row.amount, 2) + '</td>' +
        '<td>' + entryText + '</td>' +
        '<td>' + markText + '</td>' +
        '<td class="' + pnlClass + '">' + formatTrimmedNumber(row.unrealized_pnl, 2) + '</td>' +
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
        '<td>' + positionAction + '</td>' +
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

  function positionsMoney(value) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(parsed)) {
      return '-';
    }
    return (parsed < 0 ? '-$' : '$') + Math.abs(parsed).toFixed(2);
  }

  function positionsRiskBadge(level) {
    var normalized = String(level || 'unknown').toLowerCase();
    var cls = normalized === 'high'
      ? 'status-pill status-pill--error'
      : (normalized === 'warn' ? 'status-pill status-pill--pending' : 'status-pill status-pill--ready');
    return '<span class="' + cls + '">' + escapeHtml(normalized) + '</span>';
  }

  function renderPositionsOverview(payload) {
    positionsOverviewState = payload || null;
    var summary = (payload && payload.summary) || {};
    var pump = (payload && payload.pump) || {};
    var pumpPositions = Array.isArray(pump.positions) ? pump.positions : [];
    setText(elements.positionsSummaryMain, String(summary.main_positions || 0));
    setText(
      elements.positionsSummaryPump,
      String(summary.pump_positions || 0) + ' / ' + String(summary.pump_cap || 0)
    );
    setText(elements.positionsSummaryPnl, positionsMoney(summary.total_unrealized_pnl_usd || 0));
    setText(
      elements.positionsSummaryLiq,
      summary.min_liq_buffer_pct === null || summary.min_liq_buffer_pct === undefined
        ? '-'
        : formatNumber(summary.min_liq_buffer_pct, 2) + '%'
    );
    var issues = Number(summary.protection_issues || 0);
    setText(elements.positionsSummaryProtection, issues ? issues + ' issue(s)' : 'OK');
    if (elements.positionsSummaryProtection) {
      elements.positionsSummaryProtection.className = issues ? 'value-negative' : 'value-positive';
    }
    setText(
      elements.positionsSummaryPumpCycle,
      pump.last_cycle_at_ms ? formatDate(Number(pump.last_cycle_at_ms)) : '-'
    );
    if (!elements.positionsPumpBody) {
      return;
    }
    if (!pumpPositions.length) {
      elements.positionsPumpBody.innerHTML =
        '<tr><td colspan="10" class="muted">No open Pump Live positions. Status: ' +
        escapeHtml(pump.status || 'unknown') + '.</td></tr>';
      return;
    }
    elements.positionsPumpBody.innerHTML = pumpPositions.map(function (row) {
      var pnl = Number(row.unrealized_pnl_usd || 0);
      var pnlClass = pnl > 0 ? 'value-positive' : (pnl < 0 ? 'value-negative' : '');
      var topupCap = row.margin_topup_cap_usd === null || row.margin_topup_cap_usd === undefined
        ? '-'
        : positionsMoney(row.margin_topup_cap_usd);
      var legs = String(row.legs_filled || 0) + ' filled / ' +
        String(row.legs_open || 0) + ' open / ' + String((row.legs || []).length);
      var timeLeft = row.remaining_hold_h === null || row.remaining_hold_h === undefined
        ? '-'
        : formatNumber(row.remaining_hold_h, 1) + 'h';
      return '<tr>' +
        '<td><strong>' + escapeHtml(row.symbol || '-') + '</strong><span class="cell-note">SHORT · bybit_pump</span></td>' +
        '<td>' + positionsRiskBadge(row.risk_level) + '<span class="cell-note">' + escapeHtml(row.status || '-') + '</span></td>' +
        '<td>' + formatTrimmedNumber(row.qty, 8) + '</td>' +
        '<td>' + formatTrimmedNumber(row.avg_entry_price, 8) + ' / ' + formatTrimmedNumber(row.mark_price, 8) + '</td>' +
        '<td class="' + pnlClass + '">' + positionsMoney(pnl) + '</td>' +
        '<td>' + formatTrimmedNumber(row.tp_price, 8) + ' / ' + formatTrimmedNumber(row.stop_price, 8) + '</td>' +
        '<td>' + formatTrimmedNumber(row.liq_price, 8) + '<span class="cell-note">' +
          (row.liq_buffer_pct === null || row.liq_buffer_pct === undefined ? '-' : formatNumber(row.liq_buffer_pct, 2) + '%') +
        '</span></td>' +
        '<td>' + positionsMoney(row.margin_topup_usd || 0) + ' / ' +
          positionsMoney(row.margin_prefund_floor_usd || 0) + ' / ' + topupCap + '</td>' +
        '<td>' + escapeHtml(legs) + '</td>' +
        '<td>' + escapeHtml(timeLeft) + '</td>' +
      '</tr>';
    }).join('');
  }

  function setPositionsTab(tab) {
    positionsActiveTab = ['all', 'main', 'pump'].indexOf(tab) >= 0 ? tab : 'all';
    var i;
    for (i = 0; i < elements.positionsTabs.length; i += 1) {
      var button = elements.positionsTabs[i];
      var active = String(button.getAttribute('data-positions-tab') || '') === positionsActiveTab;
      button.classList.toggle('is-active', active);
      button.setAttribute('aria-selected', active ? 'true' : 'false');
    }
    for (i = 0; i < elements.positionsPanes.length; i += 1) {
      var pane = elements.positionsPanes[i];
      var paneName = String(pane.getAttribute('data-positions-pane') || '');
      pane.hidden = positionsActiveTab !== 'all' && paneName !== positionsActiveTab;
    }
  }

  function pollPositionsOverview() {
    if (positionsOverviewInFlight) {
      return;
    }
    positionsOverviewInFlight = true;
    request('GET', '/api/positions/overview', null, function (err, data) {
      positionsOverviewInFlight = false;
      if (err) {
        if (elements.positionsPumpBody) {
          elements.positionsPumpBody.innerHTML =
            '<tr><td colspan="10" class="value-negative">Positions overview error: ' +
            escapeHtml(err.message || 'unknown') + '</td></tr>';
        }
        return;
      }
      renderPositionsOverview(data || {});
    });
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

  function formatAutoExitPair(longExchange, shortExchange) {
    if (!longExchange || !shortExchange) {
      return '-';
    }
    return String(longExchange).toUpperCase() + ' / ' + String(shortExchange).toUpperCase();
  }

  function formatAutoExitTier(row) {
    if (!row) {
      return '-';
    }
    if (row.policy_key === 'tier1') {
      return 'tier1';
    }
    if (row.policy_key === 'tier2') {
      return 'tier2';
    }
    if (row.policy_key === 'lower_tier') {
      return 'lower-tier';
    }
    if (row.worst_tier !== undefined && row.worst_tier !== null) {
      return 'tier' + row.worst_tier;
    }
    return '-';
  }

  function formatAutoExitLeg(exchange, label) {
    if (!exchange || !label) {
      return '-';
    }
    return String(exchange).toUpperCase() + ' ' + String(label);
  }

  function formatSignedBps(value) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (!isFinite(number)) {
      return '-';
    }
    return (number > 0 ? '+' : '') + formatNumber(number, 1);
  }

  function autoExitValueClass(value) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (!isFinite(number)) {
      return '';
    }
    if (number > 0) {
      return 'value-pos';
    }
    if (number < 0) {
      return 'value-neg';
    }
    return '';
  }

  function autoExitStatusChipClass(status) {
    var normalized = String(status || '').toLowerCase();
    if (normalized === 'trigger') {
      return 'status-chip status-chip--ok';
    }
    if (normalized === 'wait' || normalized === 'running' || normalized === 'hold' || normalized === 'shadow') {
      return 'status-chip status-chip--pending';
    }
    if (normalized === 'skip' || normalized === 'cooldown') {
      return 'status-chip status-chip--unknown';
    }
    return 'status-chip status-chip--unknown';
  }

  function autoExitCleanupClass(summary) {
    var text = String(summary || '').toLowerCase();
    if (!text || text === '-') {
      return '';
    }
    if (text.indexOf('block:') >= 0) {
      return 'value-neg';
    }
    if (text.indexOf(':allow') >= 0) {
      return 'value-pos';
    }
    return '';
  }

  function renderAutoExitDiagnostics(autoExit) {
    if (!elements.autoExitDiagnosticsBody || !elements.autoExitDiagnosticsEmpty) {
      return;
    }
    var rows = (autoExit && Array.isArray(autoExit.diagnostics)) ? autoExit.diagnostics : [];
    if (!rows.length) {
      elements.autoExitDiagnosticsBody.innerHTML = '<tr><td colspan="18" class="muted">No auto-exit diagnostics yet.</td></tr>';
      elements.autoExitDiagnosticsEmpty.style.display = '';
      return;
    }
    elements.autoExitDiagnosticsEmpty.style.display = 'none';
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var statusText = String(row.status || '-');
      if (row.reason) {
        statusText += ' (' + row.reason + ')';
      }
      var selectedPair = formatAutoExitPair(row.selected_long_exchange, row.selected_short_exchange);
      if ((!row.selected_long_exchange || !row.selected_short_exchange) && row.selection_mode === 'multileg_min_leg') {
        selectedPair = 'multileg pending';
      }
      var chunkText = row.chunk_qty !== undefined && row.chunk_qty !== null
        ? formatNumber(row.chunk_qty, 4)
        : '-';
      if (row.safety_factor !== undefined && row.safety_factor !== null) {
        chunkText += ' @' + formatNumber(row.safety_factor, 2) + 'x';
      }
      var edgeDeltaClass = autoExitValueClass(row.edge_delta_bps);
      var cleanupClass = autoExitCleanupClass(row.market_cleanup_summary);
      var statusChip = '<span class="' + autoExitStatusChipClass(row.status) + '">' + escapeHtml(statusText) + '</span>';
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol || '-') + '</td>' +
        '<td>' + escapeHtml(formatAutoExitPair(row.rule_long_exchange, row.rule_short_exchange)) + '</td>' +
        '<td>' + escapeHtml(selectedPair) + '</td>' +
        '<td>' + escapeHtml(formatAutoExitTier(row)) + '</td>' +
        '<td>' + statusChip + '</td>' +
        '<td>' + escapeHtml(formatAutoExitLeg(row.primary_exchange, row.primary_label)) + '</td>' +
        '<td>' + escapeHtml(formatAutoExitLeg(row.hedge_exchange, row.hedge_label)) + '</td>' +
        '<td>' + escapeHtml(row.decision_reason || '-') + '</td>' +
        '<td>' + (row.gross_spread_pct !== undefined && row.gross_spread_pct !== null ? formatNumber(row.gross_spread_pct, 2) + '%' : '-') + '</td>' +
        '<td>' + (row.net_spread_pct !== undefined && row.net_spread_pct !== null ? formatNumber(row.net_spread_pct, 2) + '%' : '-') + '</td>' +
        '<td>' + (row.required_net_spread_pct !== undefined && row.required_net_spread_pct !== null ? formatNumber(row.required_net_spread_pct, 2) + '%' : '-') + '</td>' +
        '<td class="' + edgeDeltaClass + '">' + escapeHtml(formatSignedBps(row.edge_delta_bps)) + '</td>' +
        '<td>' + escapeHtml(chunkText) + '</td>' +
        '<td>' + (row.chunk_notional_usd !== undefined && row.chunk_notional_usd !== null ? formatNumber(row.chunk_notional_usd, 2) : '-') + '</td>' +
        '<td>' + (row.chunk_notional_cap_usd !== undefined && row.chunk_notional_cap_usd !== null ? formatNumber(row.chunk_notional_cap_usd, 2) : '-') + '</td>' +
        '<td>' + (row.market_cleanup_notional_cap_usd !== undefined && row.market_cleanup_notional_cap_usd !== null ? formatNumber(row.market_cleanup_notional_cap_usd, 2) : '-') + '</td>' +
        '<td class="' + cleanupClass + '">' + escapeHtml(row.market_cleanup_summary || '-') + '</td>' +
        '<td>' + escapeHtml(formatDate(row.updated_at)) + '</td>' +
      '</tr>';
    }
    elements.autoExitDiagnosticsBody.innerHTML = html;
  }

  function renderAutoExitV1Diagnostics(autoExit) {
    if (!elements.autoExitV1DiagnosticsBody || !elements.autoExitV1DiagnosticsEmpty) {
      return;
    }
    var rows = (autoExit && Array.isArray(autoExit.v1_diagnostics)) ? autoExit.v1_diagnostics : [];
    if (!rows.length) {
      elements.autoExitV1DiagnosticsBody.innerHTML = '<tr><td colspan="15" class="muted">No experimental v1 diagnostics yet.</td></tr>';
      elements.autoExitV1DiagnosticsEmpty.style.display = '';
      return;
    }
    elements.autoExitV1DiagnosticsEmpty.style.display = 'none';
    var html = '';
    var i;
    for (i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var statusText = String(row.status || '-');
      if (row.reason) {
        statusText += ' (' + row.reason + ')';
      }
      var selectedPair = formatAutoExitPair(row.selected_long_exchange, row.selected_short_exchange);
      var statusChip = '<span class="' + autoExitStatusChipClass(row.status) + '">' + escapeHtml(statusText) + '</span>';
      var waitScoreClass = autoExitValueClass(row.wait_score_bps);
      var fundingClass = autoExitValueClass(row.funding_to_next_bps);
      var closeClass = autoExitValueClass(row.close_now_bps);
      html += '<tr>' +
        '<td>' + escapeHtml(row.symbol || '-') + '</td>' +
        '<td>' + escapeHtml(formatAutoExitPair(row.rule_long_exchange, row.rule_short_exchange)) + '</td>' +
        '<td>' + escapeHtml(selectedPair) + '</td>' +
        '<td>' + statusChip + '</td>' +
        '<td>' + escapeHtml(row.window_stage || '-') + '</td>' +
        '<td>' + (row.effective_interval_minutes !== undefined && row.effective_interval_minutes !== null ? formatNumber(row.effective_interval_minutes, 1) : '-') + '</td>' +
        '<td>' + (row.minutes_to_event !== undefined && row.minutes_to_event !== null ? formatNumber(row.minutes_to_event, 1) : '-') + '</td>' +
        '<td class="' + fundingClass + '">' + escapeHtml(formatSignedBps(row.funding_to_next_bps)) + '</td>' +
        '<td class="' + closeClass + '">' + escapeHtml(formatSignedBps(row.close_now_bps)) + '</td>' +
        '<td>' + escapeHtml(formatSignedBps(row.reversion_credit_bps)) + '</td>' +
        '<td class="' + waitScoreClass + '">' + escapeHtml(formatSignedBps(row.wait_score_bps)) + '</td>' +
        '<td>' + (row.take_profit_threshold_bps !== undefined && row.take_profit_threshold_bps !== null ? formatSignedBps(row.take_profit_threshold_bps) : '-') + '</td>' +
        '<td>' + (row.take_profit_k !== undefined && row.take_profit_k !== null ? formatNumber(row.take_profit_k, 1) + 'x' : '-') + '</td>' +
        '<td>' + escapeHtml(row.decision || '-') + '</td>' +
        '<td>' + escapeHtml(String(row.pending_exit_cycles !== undefined && row.pending_exit_cycles !== null ? row.pending_exit_cycles : '-')) + '</td>' +
      '</tr>';
    }
    elements.autoExitV1DiagnosticsBody.innerHTML = html;
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

  function syncAutoExitExecId(autoExit, autoStrategies) {
    var running = autoStrategies && autoStrategies.running ? autoStrategies.running : null;
    autoAgentContext = running;
    var execId = running && running.execution_id
      ? running.execution_id
      : latestAutoExitExecId(autoExit);
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
      elements.autoExitAgentStatus.textContent = 'No auto-agent execution yet.';
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
      var meta = [];
      if (autoAgentContext) {
        meta.push('Action: ' + (autoAgentContext.action || '-'));
        if (autoAgentContext.strategy_id) {
          meta.push('Strategy: ' + autoAgentContext.strategy_id);
        } else if (autoAgentContext.auto_exit_agent) {
          meta.push('Source: Spread / V1');
        } else if (autoAgentContext.auto_arb_agent) {
          meta.push('Source: Grid');
        }
        if (autoAgentContext.step_id) {
          meta.push('Step: ' + autoAgentContext.step_id);
        }
        if (autoAgentContext.message || autoAgentContext.stage) {
          meta.push('Now: ' + (autoAgentContext.message || autoAgentContext.stage));
        }
        if (autoAgentContext.created_at) {
          var started = Date.parse(autoAgentContext.created_at);
          if (!isNaN(started)) {
            meta.push('Elapsed: ' + Math.max(0, Math.round((Date.now() - started) / 1000)) + ' s');
          }
        }
      }
      if (autoExitExecState.logPath) {
        meta.push('Log: ' + autoExitExecState.logPath);
      }
      elements.autoExitAgentMeta.textContent = meta.join(' | ');
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

  function renderGridStrategies(autoArb) {
    if (!elements.gridStrategiesList) {
      return;
    }
    var rules = autoArb && Array.isArray(autoArb.rules) ? autoArb.rules : [];
    if (!rules.length) {
      elements.gridStrategiesList.innerHTML = '<p class="muted">No grid strategies.</p>';
      return;
    }
    function levelByNumber(rule, levelNumber) {
      var levels = Array.isArray(rule.levels) ? rule.levels : [];
      for (var i = 0; i < levels.length; i += 1) {
        if (Number(levels[i].level) === Number(levelNumber)) {
          return levels[i];
        }
      }
      return null;
    }
    function formatSpreadTarget(value, prefix) {
      if (value === null || value === undefined || isNaN(parseFloat(value))) {
        return '-';
      }
      return prefix + ' ' + formatNumber(value, 3) + '%';
    }
    function gridActionText(rule, level, action) {
      var levelCount = Number(rule.level_count || 0);
      var pending = rule.pending_transition || null;
      if (pending && pending.action === action) {
        var pendingTo = Number(pending.to_level || 0);
        var pendingRemaining = formatTrimmedNumber(pending.remaining_qty || 0, 8);
        var pendingLevel = levelByNumber(rule, action === 'enter' ? pendingTo : Number(pending.from_level || 0));
        var pendingSpread = pendingLevel
          ? (action === 'enter'
            ? formatSpreadTarget(pendingLevel.entry_spread_pct, '<=')
            : formatSpreadTarget(pendingLevel.exit_spread_pct, '>='))
          : '-';
        return 'добить ' + pendingRemaining + ' H -> уровень ' + pendingTo + ' @ ' + pendingSpread;
      }
      if (action === 'enter') {
        if (level >= levelCount) {
          return 'максимум достигнут';
        }
        var nextLevel = level + 1;
        var next = levelByNumber(rule, nextLevel);
        if (!next) {
          return '-';
        }
        return '+' + formatTrimmedNumber(next.qty || 0, 8) + ' H -> уровень ' +
          nextLevel + ' @ ' + formatSpreadTarget(next.entry_spread_pct, '<=');
      }
      if (level <= 0) {
        return 'позиции нет';
      }
      var current = levelByNumber(rule, level);
      if (!current) {
        return '-';
      }
      return '-' + formatTrimmedNumber(current.qty || 0, 8) + ' H -> уровень ' +
        (level - 1) + ' @ ' + formatSpreadTarget(current.exit_spread_pct, '>=');
    }
    elements.gridStrategiesList.innerHTML = rules.map(function (rule) {
      var mode = rule.mode || 'shadow';
      var level = Number(mode === 'live' ? (rule.live_level || 0) : (rule.shadow_level || 0));
      var qty = mode === 'live' ? (rule.actual_hedged_qty || 0) : (rule.shadow_qty || 0);
      var waitingText = level === 0 ? 'waiting first entry' : 'managing real/planned position';
      var levelCount = Number(rule.level_count || 0);
      var rangeText = formatNumber(rule.range_start_pct, 2) + '% … ' +
        formatNumber(rule.range_end_pct, 2) + '%';
      var maxQtyText = formatTrimmedNumber(rule.max_qty || 0, 8) + ' H';
      var stepText = formatNumber(rule.exit_gap_pct, 3) + '%';
      var marketText = 'entry ' + formatNumber(rule.live_entry_spread_pct, 3) +
        '% / exit ' + formatNumber(rule.live_exit_spread_pct, 3) + '%';
      var nextEntry = gridActionText(rule, level, 'enter');
      var nextExit = gridActionText(rule, level, 'exit');
      return '<article class="auto-arb-rule-card">' +
        '<div class="auto-arb-rule-head">' +
          '<div><strong>' + escapeHtml(rule.symbol || '-') + '</strong>' +
          '<div class="cell-note">' + escapeHtml(rule.long_exchange || '-') + ' long / ' +
          escapeHtml(rule.short_exchange || '-') + ' short</div></div>' +
          '<span class="status-pill status-pill--' + (rule.enabled ? 'ready' : 'idle') + '">' +
          escapeHtml(mode.toUpperCase()) + '</span>' +
        '</div>' +
        '<div class="auto-arb-metrics">' +
          '<div><span>Уровень</span><strong>' + escapeHtml(level) + ' / ' +
          escapeHtml(levelCount) + '</strong></div>' +
          '<div><span>Позиция / максимум</span><strong>' +
          formatTrimmedNumber(qty, 8) + ' / ' + escapeHtml(maxQtyText) + '</strong></div>' +
          '<div><span>Диапазон входа</span><strong>' + escapeHtml(rangeText) + '</strong></div>' +
          '<div><span>Шаг / exit gap</span><strong>' + escapeHtml(stepText) + '</strong></div>' +
          '<div><span>Рынок сейчас</span><strong>' + escapeHtml(marketText) + '</strong></div>' +
          '<div><span>Статус</span><strong>' + escapeHtml(rule.status || waitingText) + '</strong></div>' +
        '</div>' +
        '<div class="auto-arb-next-actions">' +
          '<div><span>Следующий вход</span><strong>' + escapeHtml(nextEntry) + '</strong></div>' +
          '<div><span>Ближайший выход</span><strong>' + escapeHtml(nextExit) + '</strong></div>' +
        '</div>' +
        '<div class="cell-note">' + escapeHtml(waitingText) +
        (rule.blocked_reason ? ' · ' + escapeHtml(rule.blocked_reason) : '') + '</div>' +
      '</article>';
    }).join('');
  }

  function renderLiveStrategies(autoStrategies) {
    if (!elements.liveStrategiesList) {
      return;
    }
    var strategies = autoStrategies && Array.isArray(autoStrategies.strategies)
      ? autoStrategies.strategies
      : [];
    if (!strategies.length) {
      elements.liveStrategiesList.innerHTML = '<p class="muted">No live entry or exit strategies.</p>';
      return;
    }
    elements.liveStrategiesList.innerHTML = strategies.map(function (strategy) {
      var steps = Array.isArray(strategy.steps) ? strategy.steps : [];
      var current = null;
      var i;
      for (i = 0; i < steps.length; i += 1) {
        if (['completed', 'completed_with_dust', 'cancelled'].indexOf(steps[i].status) === -1) {
          current = steps[i];
          break;
        }
      }
      var typeLabel = strategy.type === 'exit_ladder' ? 'AUTO EXIT' : 'AUTO ENTER';
      var trigger = current
        ? ('spread ' + (strategy.action === 'exit' ? '≥ ' : '≤ ') +
          formatNumber(current.spread_target_pct, 3) + '%' +
          (current.funding_min_pct !== null && current.funding_min_pct !== undefined
            ? ', funding ≥ ' + formatNumber(current.funding_min_pct, 4) + '%'
            : ''))
        : 'all steps completed';
      return '<article class="auto-arb-rule-card">' +
        '<div class="auto-arb-rule-head"><div><strong>' + escapeHtml(strategy.symbol || '-') +
        '</strong><div class="cell-note">' + escapeHtml(strategy.long_exchange || '-') +
        ' long / ' + escapeHtml(strategy.short_exchange || '-') + ' short</div></div>' +
        '<span class="status-pill status-pill--' + (strategy.enabled ? 'ready' : 'idle') + '">' +
        typeLabel + '</span></div>' +
        '<div class="auto-arb-metrics">' +
        '<div><span>Current step</span><strong>' +
        escapeHtml(current ? ((current.index || 0) + 1) + ' / ' + steps.length : steps.length + ' / ' + steps.length) +
        '</strong></div><div><span>Status</span><strong>' +
        escapeHtml(current ? (current.status || 'waiting') : 'completed') +
        '</strong></div><div><span>Target / filled</span><strong>' +
        formatTrimmedNumber(current && current.target_qty, 8) + ' / ' +
        formatTrimmedNumber(current && current.filled_qty, 8) +
        '</strong></div><div><span>Remaining</span><strong>' +
        formatTrimmedNumber(current && current.remaining_qty, 8) +
        '</strong></div></div><div class="cell-note">' + escapeHtml(trigger) + '</div></article>';
    }).join('');
  }

  function renderAccounts(accounts) {
    var data = accounts || defaultAccounts;
    renderAccountStatus(data.status || []);
    renderBalanceSummary(data.balance_summary || {});
    renderAccountBalances(data.balances || []);
    renderSymbolPositions(data.positions_by_symbol || []);
    renderSymbolPositionsDiagnostics(data.positions_market || null);
    renderMarginDiagnostics(data.margin_diagnostics || []);
    renderMarginLogicLog(data.margin_logic_log || []);
    renderDeriskExchangeHealth(data.exchange_health || {});
    renderHedgeClusters(data.hedge_clusters || { rules: {} });
    renderDeriskDiagnostics(data.derisk_diagnostics || []);
    renderDeriskEvents(data.derisk_events || []);
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
    renderGridStrategies(globalState.auto_arb || {});
    renderLiveStrategies(globalState.auto_strategies || {});
    renderAutoExitLog(globalState.auto_exit || {});
    renderAutoExitDiagnostics(globalState.auto_exit || {});
    renderAutoExitV1Diagnostics(globalState.auto_exit || {});
    syncAutoExitExecId(globalState.auto_exit || {}, globalState.auto_strategies || {});
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
        pollPositionsOverview();
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
      pollPositionsOverview();
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
      protective: {},
      manual: {}
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
      notification_primary_channel: elements.notificationPrimary ? String(elements.notificationPrimary.value || 'ntfy') : 'ntfy',
      notification_fallback_channel: elements.notificationFallback ? String(elements.notificationFallback.value || 'telegram') : 'telegram',
      auto_margin_enabled: elements.autoMarginAdd ? !!elements.autoMarginAdd.checked : true,
      auto_margin_reduce_enabled: elements.autoMarginReduce ? !!elements.autoMarginReduce.checked : false,
      enforce_isolated_margin: elements.enforceIsolatedMargin ? !!elements.enforceIsolatedMargin.checked : true,
      enforce_leverage: elements.enforceLeverage ? !!elements.enforceLeverage.checked : true,
      target_leverage: elements.targetLeverage ? parseFloat(elements.targetLeverage.value) || 3 : 3,
      kucoin_isolated_topup_only: elements.kucoinTopupOnly ? !!elements.kucoinTopupOnly.checked : true,
      stop_gap_from_liq_pct: elements.stopGapInput ? parseFloat(elements.stopGapInput.value) || defaultSettings.stop_gap_from_liq_pct || 0.025 : 0.025,
      stop_requote_threshold_pct: elements.requoteInput ? parseFloat(elements.requoteInput.value) || 0.0025 : 0.0025,
      fallback_liq_factor_long: elements.fallbackLongInput ? parseFloat(elements.fallbackLongInput.value) || 0.33 : 0.33,
      fallback_liq_factor_short: elements.fallbackShortInput ? parseFloat(elements.fallbackShortInput.value) || 1.66 : 1.66,
      auto_rebalance_enabled: elements.rebalanceAuto ? !!elements.rebalanceAuto.checked : false,
      rebalance_delta_pct: elements.rebalanceDeltaInput ? parseFloat(elements.rebalanceDeltaInput.value) || 0.2 : 0.2,
      rebalance_cooldown_sec: elements.rebalanceCooldownInput ? parseInt(elements.rebalanceCooldownInput.value, 10) || 120 : 120,
      rebalance_limit_timeout_sec: elements.rebalanceTimeoutInput ? parseInt(elements.rebalanceTimeoutInput.value, 10) || 10 : 10,
      rebalance_limit_offset_bps: elements.rebalanceOffsetInput ? parseFloat(elements.rebalanceOffsetInput.value) || 2 : 2,
      rebalance_max_slippage_bps: elements.rebalanceSlippageInput ? parseFloat(elements.rebalanceSlippageInput.value) || 8 : 8,
      auto_derisk_enabled: elements.deriskEnabled ? !!elements.deriskEnabled.checked : false,
      auto_derisk_shadow_mode: elements.deriskShadowMode ? !!elements.deriskShadowMode.checked : true,
      orphan_cleanup_enabled: elements.orphanCleanupEnabled ? !!elements.orphanCleanupEnabled.checked : true,
      derisk_poll_sec: elements.deriskPollSec ? parseInt(elements.deriskPollSec.value, 10) || 5 : 5,
      derisk_target_buffer_pct: elements.deriskTargetBuffer ? parseFloat(elements.deriskTargetBuffer.value) || 0.3 : 0.3,
      derisk_warning_buffer_pct: elements.deriskWarningBuffer ? parseFloat(elements.deriskWarningBuffer.value) || 0.2 : 0.2,
      derisk_panic_buffer_pct: elements.deriskPanicBuffer ? parseFloat(elements.deriskPanicBuffer.value) || 0.15 : 0.15,
      derisk_recovery_buffer_pct: elements.deriskRecoveryBuffer ? parseFloat(elements.deriskRecoveryBuffer.value) || 0.35 : 0.35,
      derisk_min_free_balance_abs: elements.deriskMinFreeBalance ? parseFloat(elements.deriskMinFreeBalance.value) || 500 : 500,
      derisk_stale_positions_max_sec: elements.deriskStaleMax ? parseInt(elements.deriskStaleMax.value, 10) || 180 : 180,
      derisk_failure_block_count: elements.deriskFailureBlock ? parseInt(elements.deriskFailureBlock.value, 10) || 2 : 2,
      derisk_confirm_cycles: elements.deriskConfirmCycles ? parseInt(elements.deriskConfirmCycles.value, 10) || 2 : 2,
      derisk_cooldown_sec: elements.deriskCooldownSec ? parseInt(elements.deriskCooldownSec.value, 10) || 120 : 120,
      derisk_velocity_trigger_bps: elements.deriskVelocityBps ? parseFloat(elements.deriskVelocityBps.value) || 120 : 120,
      derisk_qty_tolerance_pct: elements.deriskQtyTolerance ? parseFloat(elements.deriskQtyTolerance.value) || 0.1 : 0.1,
      derisk_max_single_action_notional_usd: elements.deriskMaxActionNotional ? parseFloat(elements.deriskMaxActionNotional.value) || 500 : 500,
      derisk_market_cleanup_only_in_emergency: elements.deriskMarketCleanupOnlyEmergency ? !!elements.deriskMarketCleanupOnlyEmergency.checked : true,
      derisk_dust_notional_usd: elements.deriskDustNotional ? parseFloat(elements.deriskDustNotional.value) || 10 : 10,
      derisk_max_candidate_score: elements.deriskMaxCandidateScore && elements.deriskMaxCandidateScore.value !== '' ? parseFloat(elements.deriskMaxCandidateScore.value) : 0.25,
      derisk_preflight_ttl_sec: elements.deriskPreflightTtl ? parseInt(elements.deriskPreflightTtl.value, 10) || 60 : 60
    };
    var manual = clone((globalState.settings && globalState.settings.manual) ? globalState.settings.manual : defaultSettings.manual) || {};
    manual.auto_exit_policy = mergeAutoExitPolicy({
      tier1: {
        chunk_notional_cap_usd: elements.autoExitTier1ChunkCap ? elements.autoExitTier1ChunkCap.value : null,
        market_cleanup_notional_cap_usd: elements.autoExitTier1CleanupCap ? elements.autoExitTier1CleanupCap.value : null,
        edge_buffer_bps: elements.autoExitTier1EdgeBuffer ? elements.autoExitTier1EdgeBuffer.value : null
      },
      tier2: {
        chunk_notional_cap_usd: elements.autoExitTier2ChunkCap ? elements.autoExitTier2ChunkCap.value : null,
        market_cleanup_notional_cap_usd: elements.autoExitTier2CleanupCap ? elements.autoExitTier2CleanupCap.value : null,
        edge_buffer_bps: elements.autoExitTier2EdgeBuffer ? elements.autoExitTier2EdgeBuffer.value : null
      },
      lower_tier: {
        chunk_notional_cap_usd: elements.autoExitLowerChunkCap ? elements.autoExitLowerChunkCap.value : null,
        market_cleanup_notional_cap_usd: elements.autoExitLowerCleanupCap ? elements.autoExitLowerCleanupCap.value : null,
        edge_buffer_bps: elements.autoExitLowerEdgeBuffer ? elements.autoExitLowerEdgeBuffer.value : null
      }
    });
    result.manual = manual;
    return result;
  }

  function collectAutoExitDefaults() {
    if (!elements.autoExitRuntimeInput && !elements.autoExitCooldownInput && !elements.autoExitRequireLive && !elements.autoExitRestoreSpread) {
      return null;
    }
    var defaults = {
      max_runtime_sec: defaultAutoExit.defaults.max_runtime_sec,
      cooldown_sec: defaultAutoExit.defaults.cooldown_sec,
      require_live: defaultAutoExit.defaults.require_live,
      auto_clear_no_position_sec: defaultAutoExit.defaults.auto_clear_no_position_sec,
      restore_spread_on_missing: defaultAutoExit.defaults.restore_spread_on_missing
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
    if (elements.autoExitRestoreSpread) {
      defaults.restore_spread_on_missing = !!elements.autoExitRestoreSpread.checked;
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
    if (elements.autoExitRestoreSpread) {
      elements.autoExitRestoreSpread.checked = autoExit.defaults.restore_spread_on_missing !== false;
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

  function clearAutoExitSpreadCache() {
    if (elements.autoExitClearSpreadCache) {
      elements.autoExitClearSpreadCache.disabled = true;
    }
    request('POST', '/api/auto-exit/clear-spread-cache', {}, function (err, data) {
      if (elements.autoExitClearSpreadCache) {
        elements.autoExitClearSpreadCache.disabled = false;
      }
      if (err) {
        setSettingsStatus('Auto-exit spread cache clear failed: ' + err.message, 'error');
        return;
      }
      if (data && data.auto_exit) {
        globalState.auto_exit = normalizeAutoExit(data.auto_exit);
        renderSymbolPositions(globalState.accounts.positions_by_symbol || []);
        syncAutoExitDefaults(globalState.auto_exit);
      }
      var removed = data && data.removed !== undefined ? data.removed : 0;
      var disabled = data && data.disabled !== undefined ? data.disabled : 0;
      setSettingsStatus('Auto-exit spread cache cleared: removed ' + removed + ', disabled ' + disabled + '.', 'success');
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
    if (elements.notificationPrimary) {
      elements.notificationPrimary.value = protective.notification_primary_channel !== undefined ? protective.notification_primary_channel : 'ntfy';
    }
    if (elements.notificationFallback) {
      elements.notificationFallback.value = protective.notification_fallback_channel !== undefined ? protective.notification_fallback_channel : 'telegram';
    }
    if (elements.alertMargin) {
      elements.alertMargin.checked = protective.hasOwnProperty('send_margin_alerts') ? !!protective.send_margin_alerts : true;
    }
    if (elements.autoMarginAdd) {
      elements.autoMarginAdd.checked = protective.hasOwnProperty('auto_margin_enabled') ? !!protective.auto_margin_enabled : true;
    }
    if (elements.autoMarginReduce) {
      elements.autoMarginReduce.checked = protective.hasOwnProperty('auto_margin_reduce_enabled') ? !!protective.auto_margin_reduce_enabled : false;
    }
    if (elements.enforceIsolatedMargin) {
      elements.enforceIsolatedMargin.checked = protective.hasOwnProperty('enforce_isolated_margin') ? !!protective.enforce_isolated_margin : true;
    }
    if (elements.enforceLeverage) {
      elements.enforceLeverage.checked = protective.hasOwnProperty('enforce_leverage') ? !!protective.enforce_leverage : true;
    }
    if (elements.targetLeverage) {
      elements.targetLeverage.value = protective.target_leverage !== undefined ? protective.target_leverage : 3;
    }
    if (elements.kucoinTopupOnly) {
      elements.kucoinTopupOnly.checked = protective.hasOwnProperty('kucoin_isolated_topup_only') ? !!protective.kucoin_isolated_topup_only : true;
    }
    if (elements.alertMissingStops) {
      elements.alertMissingStops.checked = protective.hasOwnProperty('send_missing_stop_alerts') ? !!protective.send_missing_stop_alerts : true;
    }
    if (elements.stopGapInput) {
      elements.stopGapInput.value = protective.stop_gap_from_liq_pct !== undefined ? protective.stop_gap_from_liq_pct : 0.025;
    }
    if (elements.requoteInput) {
      elements.requoteInput.value = protective.stop_requote_threshold_pct !== undefined ? protective.stop_requote_threshold_pct : 0.0025;
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
    if (elements.deriskEnabled) {
      elements.deriskEnabled.checked = protective.hasOwnProperty('auto_derisk_enabled') ? !!protective.auto_derisk_enabled : false;
    }
    if (elements.deriskShadowMode) {
      elements.deriskShadowMode.checked = protective.hasOwnProperty('auto_derisk_shadow_mode') ? !!protective.auto_derisk_shadow_mode : true;
    }
    if (elements.orphanCleanupEnabled) {
      elements.orphanCleanupEnabled.checked = protective.hasOwnProperty('orphan_cleanup_enabled') ? !!protective.orphan_cleanup_enabled : true;
    }
    if (elements.deriskPollSec) {
      elements.deriskPollSec.value = protective.derisk_poll_sec !== undefined ? protective.derisk_poll_sec : 5;
    }
    if (elements.deriskTargetBuffer) {
      elements.deriskTargetBuffer.value = protective.derisk_target_buffer_pct !== undefined ? protective.derisk_target_buffer_pct : 0.3;
    }
    if (elements.deriskWarningBuffer) {
      elements.deriskWarningBuffer.value = protective.derisk_warning_buffer_pct !== undefined ? protective.derisk_warning_buffer_pct : 0.2;
    }
    if (elements.deriskPanicBuffer) {
      elements.deriskPanicBuffer.value = protective.derisk_panic_buffer_pct !== undefined ? protective.derisk_panic_buffer_pct : 0.15;
    }
    if (elements.deriskRecoveryBuffer) {
      elements.deriskRecoveryBuffer.value = protective.derisk_recovery_buffer_pct !== undefined ? protective.derisk_recovery_buffer_pct : 0.35;
    }
    if (elements.deriskMinFreeBalance) {
      elements.deriskMinFreeBalance.value = protective.derisk_min_free_balance_abs !== undefined ? protective.derisk_min_free_balance_abs : 500;
    }
    if (elements.deriskStaleMax) {
      elements.deriskStaleMax.value = protective.derisk_stale_positions_max_sec !== undefined ? protective.derisk_stale_positions_max_sec : 180;
    }
    if (elements.deriskFailureBlock) {
      elements.deriskFailureBlock.value = protective.derisk_failure_block_count !== undefined ? protective.derisk_failure_block_count : 2;
    }
    if (elements.deriskConfirmCycles) {
      elements.deriskConfirmCycles.value = protective.derisk_confirm_cycles !== undefined ? protective.derisk_confirm_cycles : 2;
    }
    if (elements.deriskCooldownSec) {
      elements.deriskCooldownSec.value = protective.derisk_cooldown_sec !== undefined ? protective.derisk_cooldown_sec : 120;
    }
    if (elements.deriskVelocityBps) {
      elements.deriskVelocityBps.value = protective.derisk_velocity_trigger_bps !== undefined ? protective.derisk_velocity_trigger_bps : 120;
    }
    if (elements.deriskQtyTolerance) {
      elements.deriskQtyTolerance.value = protective.derisk_qty_tolerance_pct !== undefined ? protective.derisk_qty_tolerance_pct : 0.1;
    }
    if (elements.deriskMaxActionNotional) {
      elements.deriskMaxActionNotional.value = protective.derisk_max_single_action_notional_usd !== undefined ? protective.derisk_max_single_action_notional_usd : 500;
    }
    if (elements.deriskDustNotional) {
      elements.deriskDustNotional.value = protective.derisk_dust_notional_usd !== undefined ? protective.derisk_dust_notional_usd : 10;
    }
    if (elements.deriskMaxCandidateScore) {
      elements.deriskMaxCandidateScore.value = protective.derisk_max_candidate_score !== undefined ? protective.derisk_max_candidate_score : 0.25;
    }
    if (elements.deriskPreflightTtl) {
      elements.deriskPreflightTtl.value = protective.derisk_preflight_ttl_sec !== undefined ? protective.derisk_preflight_ttl_sec : 60;
    }
    if (elements.deriskMarketCleanupOnlyEmergency) {
      elements.deriskMarketCleanupOnlyEmergency.checked = protective.hasOwnProperty('derisk_market_cleanup_only_in_emergency')
        ? !!protective.derisk_market_cleanup_only_in_emergency
        : true;
    }
    var manual = settings.manual || {};
    var autoExitPolicy = mergeAutoExitPolicy(manual.auto_exit_policy);
    if (elements.autoExitTier1ChunkCap) {
      elements.autoExitTier1ChunkCap.value = autoExitPolicy.tier1.chunk_notional_cap_usd;
    }
    if (elements.autoExitTier1CleanupCap) {
      elements.autoExitTier1CleanupCap.value = autoExitPolicy.tier1.market_cleanup_notional_cap_usd;
    }
    if (elements.autoExitTier1EdgeBuffer) {
      elements.autoExitTier1EdgeBuffer.value = autoExitPolicy.tier1.edge_buffer_bps;
    }
    if (elements.autoExitTier2ChunkCap) {
      elements.autoExitTier2ChunkCap.value = autoExitPolicy.tier2.chunk_notional_cap_usd;
    }
    if (elements.autoExitTier2CleanupCap) {
      elements.autoExitTier2CleanupCap.value = autoExitPolicy.tier2.market_cleanup_notional_cap_usd;
    }
    if (elements.autoExitTier2EdgeBuffer) {
      elements.autoExitTier2EdgeBuffer.value = autoExitPolicy.tier2.edge_buffer_bps;
    }
    if (elements.autoExitLowerChunkCap) {
      elements.autoExitLowerChunkCap.value = autoExitPolicy.lower_tier.chunk_notional_cap_usd;
    }
    if (elements.autoExitLowerCleanupCap) {
      elements.autoExitLowerCleanupCap.value = autoExitPolicy.lower_tier.market_cleanup_notional_cap_usd;
    }
    if (elements.autoExitLowerEdgeBuffer) {
      elements.autoExitLowerEdgeBuffer.value = autoExitPolicy.lower_tier.edge_buffer_bps;
    }
    toggleRebalanceFields(elements.rebalanceAuto ? elements.rebalanceAuto.checked : false);
  }

  function handleAutoExitChange(event) {
    if (!event || !event.target) {
      return;
    }
    var target = event.target;
    if (!(target.classList && (target.classList.contains('auto-exit-toggle') || target.classList.contains('auto-exit-v1-toggle') || target.classList.contains('auto-exit-target') || target.classList.contains('auto-exit-percent')))) {
      return;
    }
    var row = target.closest('tr');
    if (!row) {
      return;
    }
    var toggle = row.querySelector('.auto-exit-toggle');
    var v1Toggle = row.querySelector('.auto-exit-v1-toggle');
    var input = row.querySelector('.auto-exit-target');
    var percentInput = row.querySelector('.auto-exit-percent');
    if (!toggle || !v1Toggle || !input || !percentInput) {
      return;
    }
    var symbol = input.dataset.symbol || toggle.dataset.symbol;
    var longExchange = input.dataset.long || toggle.dataset.long;
    var shortExchange = input.dataset.short || toggle.dataset.short;
    if (!symbol || !longExchange || !shortExchange) {
      return;
    }
    var spreadEnabled = !!toggle.checked;
    var v1Enabled = !!v1Toggle.checked;
    var targetVal = parseFloat(input.value);
    var exitPercent = parseFloat(percentInput.value);
    if (spreadEnabled && (isNaN(targetVal) || !isFinite(targetVal))) {
      renderMessages(['Auto-exit target spread is required.']);
      return;
    }
    if (!spreadEnabled && !v1Enabled) {
      request('POST', '/api/auto-exit/clear-spread-cache', { symbol: symbol, clear_v1: true }, function (err, data) {
        if (err) {
          renderMessages(['Auto-exit clear failed: ' + err.message]);
          return;
        }
        if (data && data.auto_exit) {
          globalState.auto_exit = normalizeAutoExit(data.auto_exit);
          renderSymbolPositions(globalState.accounts.positions_by_symbol || []);
        }
      });
      return;
    }
    var payload = {
      symbol: symbol,
      long_exchange: longExchange,
      short_exchange: shortExchange,
      enabled: spreadEnabled,
      spread_enabled: spreadEnabled,
      v1_enabled: v1Enabled,
      target_spread_pct: spreadEnabled ? targetVal : null,
      exit_percent: isFinite(exitPercent) ? exitPercent : 100,
      exit_once: true
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

  function handlePositionActionClick(event) {
    var target = event && event.target;
    if (!(target && target.classList && target.classList.contains('position-action-btn'))) {
      return;
    }
    var controls = target.closest('.position-action-controls');
    if (!controls) {
      return;
    }
    var percentInput = controls.querySelector('.position-action-percent');
    var percent = percentInput ? parseFloat(percentInput.value) : 100;
    var payload = {
      symbol: controls.dataset.symbol || '',
      long_exchange: controls.dataset.long || '',
      short_exchange: controls.dataset.short || '',
      action: target.dataset.action || '',
      percent: isFinite(percent) ? percent : 100,
      dry_run: true,
      async_run: false
    };
    if (!payload.symbol || !payload.long_exchange || !payload.short_exchange) {
      renderMessages(['Position action is unavailable: exchange pair is not resolved.']);
      return;
    }
    target.disabled = true;
    request('POST', '/api/position/action', payload, function (err, plan) {
      target.disabled = false;
      if (err) {
        renderMessages(['Position action preflight failed: ' + (err.detail || err.message)]);
        return;
      }
      var errors = plan && Array.isArray(plan.errors) ? plan.errors : [];
      if (errors.length) {
        renderMessages(['Position action preflight: ' + errors.join('; ')]);
        return;
      }
      var meta = (plan && plan.position_action) || {};
      var verb = payload.action === 'add' ? 'Add' : 'Exit';
      var prompt = verb + ' ' + formatNumber(meta.action_qty, 8) + ' ' + payload.symbol +
        ' (' + payload.percent + '% of hedged ' + formatNumber(meta.hedged_qty, 8) + ')?\n' +
        'Long: ' + formatNumber(meta.long_qty, 8) + ' | Short: ' + formatNumber(meta.short_qty, 8) +
        ' | Imbalance: ' + formatNumber(meta.imbalance_qty, 8);
      if (!window.confirm(prompt)) {
        return;
      }
      payload.dry_run = false;
      payload.async_run = true;
      target.disabled = true;
      request('POST', '/api/position/action', payload, function (executeErr, result) {
        if (executeErr) {
          target.disabled = false;
          renderMessages(['Position action failed: ' + (executeErr.detail || executeErr.message)]);
          return;
        }
        var executeErrors = result && Array.isArray(result.errors) ? result.errors : [];
        if (executeErrors.length) {
          target.disabled = false;
          renderMessages(['Position action failed: ' + executeErrors.join('; ')]);
          return;
        }
        var execId = result && result.execution_id;
        renderMessages([verb + ' started for ' + payload.symbol + ' at ' + payload.percent + '%' +
          (execId ? ' | execution=' + execId : '')]);
        if (execId) {
          var actionKey = positionActionKey(payload.symbol, payload.long_exchange, payload.short_exchange);
          activePositionActions[actionKey] = execId;
          renderSymbolPositions(globalState.accounts.positions_by_symbol || []);
          pollPositionActionExecution(execId, payload.symbol, verb, target, actionKey);
        } else {
          target.disabled = false;
          pollSnapshot(true);
        }
      });
    });
  }

  function positionActionKey(symbol, longExchange, shortExchange) {
    return String(symbol || '').toUpperCase() + '|' +
      String(longExchange || '').toLowerCase() + '|' +
      String(shortExchange || '').toLowerCase();
  }

  function pollPositionActionExecution(execId, symbol, verb, button, actionKey) {
    request('GET', '/api/manual/exec/' + encodeURIComponent(execId), null, function (err, data) {
      if (err) {
        delete activePositionActions[actionKey];
        if (button) {
          button.disabled = false;
        }
        renderSymbolPositions(globalState.accounts.positions_by_symbol || []);
        renderMessages([verb + ' status failed for ' + symbol + ': ' + (err.detail || err.message)]);
        return;
      }
      if (data && data.status === 'running') {
        window.setTimeout(function () {
          pollPositionActionExecution(execId, symbol, verb, button, actionKey);
        }, 1500);
        return;
      }
      delete activePositionActions[actionKey];
      if (button) {
        button.disabled = false;
      }
      var resultErrors = data && data.result && Array.isArray(data.result.errors) ? data.result.errors : [];
      if (data && data.status === 'completed' && !resultErrors.length) {
        renderMessages([verb + ' completed for ' + symbol + ' | execution=' + execId]);
      } else {
        var detail = resultErrors.length ? resultErrors.join('; ') : ((data && data.error) || (data && data.status) || 'unknown error');
        renderMessages([verb + ' failed for ' + symbol + ': ' + detail + ' | execution=' + execId]);
      }
      pollSnapshot(true);
    });
  }

  function setHedgeClusterStatus(message, tone) {
    if (!elements.hedgeClusterStatus) {
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
    elements.hedgeClusterStatus.className = className;
    elements.hedgeClusterStatus.textContent = message || '';
  }

  function handleHedgeClusterSubmit(event) {
    if (event && typeof event.preventDefault === 'function') {
      event.preventDefault();
    }
    if (!elements.hedgeClusterForm) {
      return;
    }
    var symbol = elements.hedgeClusterSymbol ? String(elements.hedgeClusterSymbol.value || '').trim().toUpperCase() : '';
    var kind = elements.hedgeClusterKind ? String(elements.hedgeClusterKind.value || 'hedged_pair') : 'hedged_pair';
    if (!symbol) {
      setHedgeClusterStatus('Symbol is required.', 'error');
      return;
    }
    var payload = {
      symbol: symbol,
      kind: kind,
      enabled: elements.hedgeClusterEnabled ? !!elements.hedgeClusterEnabled.checked : true,
      qty_tolerance_pct: elements.hedgeClusterQtyTolerance ? parseFloat(elements.hedgeClusterQtyTolerance.value) || 0.1 : 0.1,
      rehedge_allowed: elements.hedgeClusterRehedge ? !!elements.hedgeClusterRehedge.checked : false
    };
    if (kind === 'standalone') {
      payload.exchange = elements.hedgeClusterExchange ? String(elements.hedgeClusterExchange.value || '').trim().toLowerCase() : '';
      payload.side = elements.hedgeClusterSide ? String(elements.hedgeClusterSide.value || '').trim().toLowerCase() : '';
      if (!payload.exchange) {
        setHedgeClusterStatus('Exchange is required for standalone.', 'error');
        return;
      }
    } else {
      payload.long_exchange = elements.hedgeClusterLongExchange ? String(elements.hedgeClusterLongExchange.value || '').trim().toLowerCase() : '';
      payload.short_exchange = elements.hedgeClusterShortExchange ? String(elements.hedgeClusterShortExchange.value || '').trim().toLowerCase() : '';
      if (!payload.long_exchange || !payload.short_exchange) {
        setHedgeClusterStatus('Both exchanges are required for hedged pair.', 'error');
        return;
      }
    }
    setHedgeClusterStatus('Saving cluster rule…', 'info');
    request('POST', '/api/hedge-clusters/rule', payload, function (err, data) {
      if (err) {
        setHedgeClusterStatus(err.message, 'error');
        return;
      }
      if (data && data.hedge_clusters) {
        if (!globalState.accounts) {
          globalState.accounts = normalizeAccounts(null);
        }
        globalState.accounts.hedge_clusters = clone(data.hedge_clusters);
        renderHedgeClusters(globalState.accounts.hedge_clusters);
      }
      setHedgeClusterStatus('Cluster rule saved', 'success');
      window.setTimeout(function () {
        setHedgeClusterStatus('', '');
      }, 2500);
    });
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
    if (elements.autoExitClearSpreadCache) {
      elements.autoExitClearSpreadCache.addEventListener('click', clearAutoExitSpreadCache);
    }
    if (elements.hedgeClusterForm) {
      elements.hedgeClusterForm.addEventListener('submit', handleHedgeClusterSubmit);
      elements.hedgeClusterForm.addEventListener('change', function () {
        setHedgeClusterStatus('', '');
      });
    }
    if (elements.rebalanceAuto) {
      elements.rebalanceAuto.addEventListener('change', function () {
        toggleRebalanceFields(!!elements.rebalanceAuto.checked);
      });
    }
    if (elements.symbolPositionsTable) {
      elements.symbolPositionsTable.addEventListener('change', handleAutoExitChange);
      elements.symbolPositionsTable.addEventListener('click', handlePositionActionClick);
    }
    if (elements.positionsTabs && elements.positionsTabs.length) {
      Array.prototype.forEach.call(elements.positionsTabs, function (button) {
        button.addEventListener('click', function () {
          setPositionsTab(String(button.getAttribute('data-positions-tab') || 'all'));
        });
      });
      setPositionsTab(positionsActiveTab);
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
    pollPositionsOverview();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
