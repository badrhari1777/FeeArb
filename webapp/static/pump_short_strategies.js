(function () {
  'use strict';

  var rootPath = window.__ROOT_PATH__ || '';
  var state = window.__PUMP_SHORT_STRATEGIES_INITIAL__ || {};
  var pollTimer = null;

  function $(id) {
    return document.getElementById(id);
  }

  function requestJson(method, path, payload, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open(method, rootPath + path, true);
    xhr.setRequestHeader('Accept', 'application/json');
    if (payload !== null && payload !== undefined) {
      xhr.setRequestHeader('Content-Type', 'application/json');
    }
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      var data = {};
      try {
        data = xhr.responseText ? JSON.parse(xhr.responseText) : {};
      } catch (err) {
        data = { detail: xhr.responseText || String(err) };
      }
      if (xhr.status < 200 || xhr.status >= 300) {
        callback(new Error(data.detail || ('HTTP ' + xhr.status)), data);
        return;
      }
      callback(null, data);
    };
    xhr.send(payload !== null && payload !== undefined ? JSON.stringify(payload) : null);
  }

  function refresh() {
    requestJson('GET', '/api/pump-short/strategies', null, function (err, data) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      state = data || {};
      render();
    });
  }

  function startPolling() {
    if (pollTimer) window.clearInterval(pollTimer);
    pollTimer = window.setInterval(refresh, 5000);
  }

  function controlPayload() {
    var symbols = [];
    String(($('pss-symbols') && $('pss-symbols').value) || '').split(',').forEach(function (part) {
      var clean = part.replace(/\s+/g, '').toUpperCase();
      if (clean) symbols.push(clean);
    });
    return {
      lookback_days: 14,
      sleep_sec: 0.8,
      max_symbols: Number(($('pss-max-symbols') && $('pss-max-symbols').value) || 1000),
      symbols: symbols,
      newest_first: true,
      recent_event_hours: Number(($('pss-recent-hours') && $('pss-recent-hours').value) || 168)
    };
  }

  function runScan() {
    setStatus('Starting scan...', false);
    requestJson('POST', '/api/pump-short/bybit/shadow/start', controlPayload(), function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Scan started', false);
      refresh();
      startPolling();
    });
  }

  function startSchedule() {
    var payload = controlPayload();
    payload.interval_sec = Number(($('pss-interval-sec') && $('pss-interval-sec').value) || 7200);
    payload.run_immediately = true;
    setStatus('Starting live-like paper schedule...', false);
    requestJson('POST', '/api/pump-short/bybit/shadow/schedule/start', payload, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Paper schedule started', false);
      refresh();
      startPolling();
    });
  }

  function stopSchedule() {
    setStatus('Stopping schedule...', false);
    requestJson('POST', '/api/pump-short/bybit/shadow/schedule/stop', {}, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Schedule stop requested', false);
      refresh();
      startPolling();
    });
  }

  function render() {
    renderHeader();
    renderOverview();
    renderPumpLive();
    renderStrategies();
    renderActiveWindow();
    renderSlowPumpWatch();
    renderCyclePaper();
    renderCycleTrackSummary();
    renderCandidateShadow();
    renderCycleEvents();
    renderPaperEvents();
    renderLegacyPaper();
    renderAudit();
  }

  function renderHeader() {
    var shadow = state.shadow || {};
    var schedule = state.schedule || {};
    var status = shadow.status || 'idle';
    var pill = $('pss-shadow-pill');
    if (pill) {
      pill.textContent = status;
      pill.className = 'status-pill status-pill--' + statusClass(status);
    }
    setText('pss-schedule-status', schedule.status || 'idle');
    setText('pss-strategy-count', String((state.strategies || []).length));
    setText('pss-audit-status', ((state.audit || {}).latest || []).length ? 'logging' : 'empty');

    var scheduleRunning = ['running', 'starting', 'waiting', 'stopping'].indexOf(schedule.status || '') >= 0;
    var scanRunning = ['running', 'starting'].indexOf(status) >= 0;
    setDisabled('pss-run-scan', scanRunning || scheduleRunning);
    setDisabled('pss-start-schedule', scanRunning || scheduleRunning);
    setDisabled('pss-stop-schedule', !scheduleRunning);
  }

  function renderOverview() {
    var strategies = state.strategies || [];
    var ready = 0;
    var watch = 0;
    var blocked = 0;
    strategies.forEach(function (strategy) {
      var counts = (((strategy || {}).signals || {}).counts || {});
      ready += Number(counts.entry_ready || 0);
      watch += Number(counts.waiting_pullback || 0) + Number(counts.waiting_oi || 0) + Number(counts.waiting_ratio || 0);
      blocked += Number(counts.blocked || 0);
    });
    setText('pss-rows-seen', (state.shadow || {}).rows_seen || 0);
    setText('pss-ready-total', ready);
    setText('pss-watch-total', watch);
    setText('pss-blocked-total', blocked);
    setText('pss-legacy-open', (state.strategy_paper || {}).open_count || 0);
    var cycle = state.cycle_paper || {};
    var summary = cycle.summary || {};
    setText('pss-cycle-equity', money(summary.equity_mark_usd || 0));
    setText('pss-cycle-slots', (summary.open_positions || 0) + ' / ' + ((cycle.config || {}).total_slots || 0));
    setText('pss-cycle-topup', money(summary.current_topup_needed_usd || 0));
    var activeWindow = state.active_window || {};
    setText('pss-active-window-count', activeWindow.symbols || 0);
    setText('pss-slow-watch-count', (state.slow_pump_watch || {}).count || 0);
    setText('pss-updated', shortTime((state.shadow || {}).updated_at_ms || (state.shadow || {}).finished_at_ms));
  }

  function renderPumpLive() {
    var live = state.pump_live || {};
    var config = live.config || {};
    var activePolicy = live.active_risk_policy || config;
    var credentials = live.credentials || {};
    var preflight = live.last_preflight || {};
    var keyInfo = preflight.key || {};
    var balance = live.last_balance || (preflight.account || {});
    var capital = live.capital_manager || {};
    var regime = live.capital_regime || {};
    var autoTransfer = ((live.transfers || {}).auto_risk) || {};
    var open = Number(live.open_positions || 0);
    var cap = Number(config.entry_cap || 1);
    setText('pss-live-status', live.status || 'disabled');
    setText('pss-live-credentials', credentials.ready ? 'ready' : 'missing');
    setText(
      'pss-live-key-expiry',
      keyInfo.ip_bound
        ? 'IP-bound'
        : (keyInfo.expired_at ? ('expires ' + String(keyInfo.expired_at).slice(0, 10)) : (keyInfo.deadline_day !== null && keyInfo.deadline_day !== undefined ? keyInfo.deadline_day + 'd left' : 'not checked'))
    );
    setText('pss-live-preflight-status', preflight.checked_at_ms ? (preflight.ready ? 'ready' : 'blocked') : 'not checked');
    setText('pss-live-balance', money(balance.total || balance.total_usdt || 0));
    setText('pss-live-available', money(balance.available || balance.available_usdt || 0));
    setText('pss-live-slots', open + ' / ' + cap + ' (max ' + Number(config.max_active_positions || 4) + ')');
    setText('pss-live-reserve', money(activePolicy.reserve_usd || 300));
    setText('pss-live-temporary', money(capital.temporary_transfer_outstanding_usd || 0));
    setText('pss-live-regime', String(regime.mode || '-').toUpperCase());
    setText('pss-live-auto-transfer', autoTransfer.enabled ? 'ON' : 'OFF');
    setText('pss-live-auto-main-floor', money(autoTransfer.main_min_available_usd || 0));
    setText(
      'pss-live-auto-daily-left',
      money(autoTransfer.daily_used_usd || 0) + ' / ' + money(autoTransfer.daily_alert_usd || 0)
    );
    setText('pss-live-cycle', shortTime(live.last_cycle_at_ms));
    setText(
      'pss-live-capital-mode',
      (capital.mode || 'observe') + (capital.application_enabled ? ' / active' : ' / calculation only')
    );
    setText('pss-live-capital-wallet', money(capital.account_wallet_usd || balance.wallet || balance.total || 0));
    setText('pss-live-capital-effective', money(capital.effective_strategy_capital_usd || 0));
    setText('pss-live-capital-active-slot', money(capital.active_slot_margin_usd || config.slot_margin_usd || 0));
    setText('pss-live-capital-recommended-slot', money(capital.recommended_slot_margin_usd || 0));
    setText('pss-live-capital-next-slot', money(capital.next_capped_slot_margin_usd || 0));
    setText('pss-live-capital-policy', capital.active_risk_policy_id || 'v1_1000');
    setText('pss-live-capital-external', money(capital.external_strategy_contribution_usd || 0));
    setText('pss-live-capital-profit-reserve', money(capital.profit_reserve_target_usd || 0));
    setText(
      'pss-live-capital-progress',
      fmt(capital.observation_elapsed_days || 0, 1) + 'd / ' +
        Number(capital.observation_closed_trades || 0) + ' trades'
    );
    var capitalInput = $('pss-live-capital-input');
    if (capitalInput && document.activeElement !== capitalInput) {
      capitalInput.value = Number(
        capital.declared_strategy_capital_usd ||
        capital.effective_strategy_capital_usd ||
        balance.wallet ||
        balance.total ||
        1000
      ).toFixed(2);
    }
    var recommendationLabels = {
      increase_ready: 'Growth threshold reached; the capped next slot is calculated but not applied.',
      decrease_ready: 'Reduction threshold reached; the smaller slot is calculated but not applied in observe mode.',
      hold_band: 'Capital remains inside the 10% growth / 5% reduction hold band.'
    };
    setText(
      'pss-live-capital-hint',
      (recommendationLabels[capital.recommendation] || 'Capital observation is waiting for data.') +
        ' Activation requires at least ' + Number(capital.observation_min_days || 14) +
        ' days and ' + Number(capital.observation_min_trades || 10) +
        ' newly closed live trades, followed by a separate operator decision.'
    );
    setDisabled('pss-live-capital-save', Number(capital.account_wallet_usd || balance.wallet || balance.total || 0) <= 0);
    setDisabled(
      'pss-live-capital-promote',
      capital.active_risk_policy_id === 'v2_3000' ||
        !live.entry_armed ||
        Number(capital.temporary_transfer_outstanding_usd || 0) < 0.01 ||
        Number(capital.temporary_transfer_outstanding_usd || 0) + 0.01 <
          Number(capital.target_3000_external_required_usd || 0)
    );

    var warnings = [];
    if (live.blocked_reason) warnings.push('Blocked: ' + live.blocked_reason);
    (preflight.errors || []).forEach(function (item) { warnings.push(item); });
    (preflight.warnings || []).forEach(function (item) { warnings.push(item); });
    if (live.last_error) warnings.push('Monitor error: ' + live.last_error);
    var warningNode = $('pss-live-warning');
    if (warningNode) {
      warningNode.textContent = warnings.join(' / ');
      warningNode.className = warnings.length ? 'settings-status settings-status--error' : 'settings-status';
    }

    var body = $('pss-live-positions-body');
    if (body) {
      var rows = (live.positions || []).filter(function (item) { return item.status !== 'closed'; });
      body.innerHTML = rows.length ? rows.map(function (item) {
        var legs = item.legs || [];
        var filled = legs.filter(function (leg) { return leg.status === 'filled'; }).length;
        var ageHours = item.opened_at_ms ? (Date.now() - Number(item.opened_at_ms)) / 3600000 : 0;
        return '<tr>' +
          cell(strong(item.symbol)) +
          cell(statusBadge(item.status, item.last_error || item.close_reason || '')) +
          cell(filled + ' / ' + legs.length) +
          cell(fmt(item.qty || 0, 6) + '<br>' + price(item.avg_entry_price) + ' / ' + price(item.mark_price)) +
          cell(price(item.tp_price) + ' / ' + price(item.stop_price) + ' / ' + price(item.liq_price)) +
          cell(item.liq_buffer_pct === null || item.liq_buffer_pct === undefined ? '-' : fmt(item.liq_buffer_pct, 2) + '%') +
          cell(money(item.margin_topup_usd || 0) + ' / base ' + money(item.margin_prefund_floor_usd || 0)) +
          cell(fmt(ageHours, 1) + 'h') +
          '</tr>';
      }).join('') : '<tr><td colspan="8" class="muted">No Pump live positions.</td></tr>';
    }

    setDisabled('pss-live-arm', !preflight.ready || !!live.entry_armed);
    setDisabled('pss-live-disarm', !live.entry_armed);
    setDisabled('pss-live-emergency', open <= 0);
  }

  function livePreflight() {
    setStatus('Running Pump live read-only preflight...', false);
    requestJson('POST', '/api/pump-short/live/preflight', null, function (err, data) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus(data.ready ? 'Pump live preflight passed' : 'Pump live preflight blocked', !data.ready);
      refresh();
    });
  }

  function livePrepare() {
    var confirmation = window.prompt('This changes only the Pump subaccount to isolated margin and one-way mode. Type: PREPARE PUMP SUBACCOUNT');
    if (confirmation !== 'PREPARE PUMP SUBACCOUNT') {
      setStatus('Pump live prepare canceled', true);
      return;
    }
    requestJson('POST', '/api/pump-short/live/prepare', { confirmation: confirmation }, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Pump subaccount prepared; run preflight again', false);
      refresh();
    });
  }

  function liveArm() {
    var live = state.pump_live || {};
    var policy = ((live.capital_manager || {}).active_risk_policy_id || 'v1_1000');
    var expected = policy === 'v2_3000' ? 'ARM PUMP LIVE 3000' : 'ARM PUMP LIVE 1000';
    var confirmation = window.prompt('REAL TRADING: new main-tier signals can place orders. Type: ' + expected);
    if (confirmation !== expected) {
      setStatus('Pump live arm canceled', true);
      return;
    }
    requestJson('POST', '/api/pump-short/live/arm', { confirmation: confirmation }, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Pump live armed for new signals', false);
      refresh();
    });
  }

  function liveSetCapital() {
    var input = $('pss-live-capital-input');
    var amount = Number((input && input.value) || 0);
    if (!isFinite(amount) || amount < 100) {
      setStatus('Strategy capital must be at least 100 USDT', true);
      return;
    }
    var confirmation = window.prompt(
      'OBSERVE ONLY: this records sizing-eligible strategy capital but does not change live orders. Type: SET PUMP STRATEGY CAPITAL'
    );
    if (confirmation !== 'SET PUMP STRATEGY CAPITAL') {
      setStatus('Capital update canceled', true);
      return;
    }
    requestJson('POST', '/api/pump-short/live/capital', {
      strategy_capital_usd: amount,
      note: String((($('pss-live-capital-note') || {}).value) || '').trim(),
      confirmation: confirmation
    }, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Observed strategy capital saved; live slot remains unchanged', false);
      refresh();
    });
  }

  function livePromoteCapital() {
    var confirmation = window.prompt(
      'CAPITAL POLICY CHANGE: existing positions remain v1; only one concurrent new v2 $525 canary is enabled. Type: PROMOTE PUMP CAPITAL 3000'
    );
    if (confirmation !== 'PROMOTE PUMP CAPITAL 3000') {
      setStatus('Capital promotion canceled', true);
      return;
    }
    requestJson('POST', '/api/pump-short/live/capital/promote', {
      target_capital_usd: 3000,
      confirmation: confirmation
    }, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('v2_3000 mixed-cohort canary enabled for future entries', false);
      refresh();
    });
  }

  function liveDisarm() {
    requestJson('POST', '/api/pump-short/live/disarm', null, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('New Pump live entries disarmed; open positions remain monitored', false);
      refresh();
    });
  }

  function liveEmergencyClose() {
    var confirmation = window.prompt('EMERGENCY: cancel Pump orders and market-close Pump positions. Type: CLOSE ALL PUMP POSITIONS');
    if (confirmation !== 'CLOSE ALL PUMP POSITIONS') {
      setStatus('Emergency close canceled', true);
      return;
    }
    requestJson('POST', '/api/pump-short/live/emergency-close', { confirmation: confirmation }, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Emergency close requested', true);
      refresh();
    });
  }

  function renderStrategies() {
    var root = $('pss-strategy-cards');
    if (!root) return;
    root.innerHTML = '';
    (state.strategies || []).forEach(function (strategy) {
      var card = document.createElement('section');
      var current = strategy.current_state || {};
      card.className = 'strategy-card strategy-card--monitor strategy-card--' + stateClass(current.state);
      card.innerHTML = [
        '<div class="strategy-card-head">',
        '<div><h2>' + esc(strategy.name || strategy.strategy_id) + '</h2><p class="muted">' + esc(strategy.mode || 'shadow') + '</p></div>',
        statusBadge(current.state || 'waiting_pump', current.label || current.state || '-'),
        '</div>',
        '<p class="hint strategy-note">' + esc(strategy.notes || '') + '</p>',
        renderCapital(strategy.capital || {}),
        renderFilters(strategy.filters || {}),
        renderTierTable(strategy.tiers || []),
        renderStrategyPositions(strategy),
        renderSignalBlocks(strategy)
      ].join('');
      root.appendChild(card);
    });
  }

  function renderCapital(capital) {
    return [
      '<div class="strategy-monitor-metrics">',
      metric('Capital', money(capital.initial_capital_usd)),
      metric('Mark equity', money(capital.equity_mark_usd !== undefined ? capital.equity_mark_usd : capital.initial_capital_usd)),
      metric('PnL', signedMoney(capital.combined_pnl_usd || 0)),
      metric('ROI', pctCell(capital.roi_mark_pct || 0)),
      metric('Slots', esc(capital.open_positions || capital.active_open_positions || 0) + ' / ' + esc(capital.max_active_coins || 0)),
      metric('Used margin', money(capital.used_margin_usd || 0)),
      metric('Top-up now', money(capital.current_topup_needed_usd || 0)),
      metric('Top-up peak', money(capital.peak_topup_needed_usd || 0)),
      metric('First step', money(capital.first_tier_step_notional_usd)),
      metric('Free slots', esc(capital.free_slots || 0)),
      '</div>'
    ].join('');
  }

  function renderFilters(filters) {
    return [
      '<div class="strategy-filter-line">',
      '<span>Funding 24h &gt; ' + fmt(filters.funding_prev_24h_gt_pct, 2) + '%</span>',
      '<span>OI 24h <= ' + fmt(filters.oi_change_24h_lte_pct, 0) + '%</span>',
      '<span>Long ratio ' + fmt(filters.long_ratio_min, 2) + '..' + fmt(filters.long_ratio_max, 2) + '</span>',
      '</div>'
    ].join('');
  }

  function renderTierTable(tiers) {
    if (!tiers.length) return '';
    var rows = tiers.map(function (tier) {
      return '<tr>' + [
        cell('&gt;= ' + fmt(tier.min_pump_pct, 0) + '%'),
        cell(esc(tier.entry || '-') + '<div class="muted mini">pullback ' + fmt(tier.pullback_pct, 0) + '%</div>'),
        cell(esc(tier.sizing || '-') + '<div class="muted mini">' + esc((tier.leg_weights || []).join('/')) + '</div>'),
        cell(esc(tier.ladder_legs || '-') + ' legs x ' + fmt(tier.ladder_step_pct, 0) + '%'),
        cell('TP' + fmt(tier.tp_pct, 0) + '<div class="muted mini">' + esc(tier.max_hold_h || '-') + 'h max</div>')
      ].join('') + '</tr>';
    }).join('');
    return '<div class="table-wrapper"><table class="table table--compact"><thead><tr><th>Pump</th><th>Entry</th><th>Size</th><th>Ladder</th><th>Exit</th></tr></thead><tbody>' + rows + '</tbody></table></div>';
  }

  function renderStrategyPositions(strategy) {
    var positions = ((strategy.positions || {}).open || []);
    var closed = ((strategy.positions || {}).recent_closed || []);
    return [
      '<div class="strategy-position-block">',
      '<h3>Paper positions</h3>',
      positionTable(positions, 'No open paper positions.'),
      closed.length ? '<h3>Recent closed</h3>' + closedTable(closed) : '',
      '</div>'
    ].join('');
  }

  function positionTable(rows, emptyText) {
    var body = '';
    if (!rows.length) {
      body = '<tr><td colspan="10" class="muted">' + esc(emptyText) + '</td></tr>';
    } else {
      body = rows.slice(0, 8).map(function (row) {
        var tier = row.tier || {};
        var trClass = row.current_topup_needed_usd > 0 ? 'row--danger' : row.peak_topup_needed_usd > 0 ? 'row--warning' : '';
        return '<tr class="' + trClass + '">' + [
          cell(strong(row.symbol) + '<div class="muted mini">' + esc(tier.rule_slug || '') + '</div>'),
          cell(statusBadge(row.status || 'open')),
          cell(stepsCellFromLegs(row)),
          cell(priceCell(row)),
          cell(price(row.target_price) + '<div class="muted mini">TP ' + fmt(tier.tp_pct, 0) + '%</div>'),
          cell(money(row.used_margin_usd) + '<div class="muted mini">' + money(row.gross_notional_usd) + ' notional</div>'),
          cell(pctCell(row.current_pnl_pct) + '<div class="muted mini">' + signedMoney(row.current_unrealized_pnl_usd) + '</div>'),
          cell(money(row.current_topup_needed_usd) + '<div class="muted mini">peak ' + money(row.peak_topup_needed_usd) + '</div>'),
          cell(fmt(row.time_in_trade_h, 1) + 'h<div class="muted mini">' + fmt(row.hours_left_h, 1) + 'h left</div>'),
          cell(fmt(row.mae_pct, 1) + '% / ' + fmt(row.mfe_pct, 1) + '%')
        ].join('') + '</tr>';
      }).join('');
    }
    return '<div class="table-wrapper table-wrapper--wide"><table class="table table--compact"><thead><tr><th>Symbol</th><th>Status</th><th>Legs</th><th>Entry / Current / Avg</th><th>Target</th><th>Margin</th><th>PnL</th><th>Top-up</th><th>Age</th><th>MAE/MFE</th></tr></thead><tbody>' + body + '</tbody></table></div>';
  }

  function closedTable(rows) {
    var body = rows.slice(0, 8).map(function (row) {
      return '<tr>' + [
        cell(strong(row.symbol) + '<div class="muted mini">' + esc(((row.tier || {}).rule_slug) || '') + '</div>'),
        cell(row.exit_reason || '-'),
        cell(signedMoney(row.realized_pnl_usd)),
        cell(pctCell(row.current_pnl_pct || 0)),
        cell(formatMs(row.closed_at_ms))
      ].join('') + '</tr>';
    }).join('');
    return '<div class="table-wrapper"><table class="table table--compact"><thead><tr><th>Symbol</th><th>Exit</th><th>Realized</th><th>Last %</th><th>Closed</th></tr></thead><tbody>' + body + '</tbody></table></div>';
  }

  function renderSignalBlocks(strategy) {
    var signals = strategy.signals || {};
    var counts = signals.counts || {};
    return [
      '<div class="strategy-signal-summary">',
      '<span>Ready ' + esc(counts.entry_ready || 0) + '</span>',
      '<span>Pullback ' + esc(counts.waiting_pullback || 0) + '</span>',
      '<span>Confirm ' + esc((counts.waiting_oi || 0) + (counts.waiting_ratio || 0)) + '</span>',
      '<span>Blocked ' + esc(counts.blocked || 0) + '</span>',
      '</div>',
      '<div class="strategy-signal-columns">',
      signalTable('Ready', signals.ready || []),
      signalTable('Watch', signals.watch || []),
      signalTable('Blocked', signals.blocked || []),
      '</div>'
    ].join('');
  }

  function signalTable(title, rows) {
    var body = '';
    if (!rows.length) {
      body = '<tr><td colspan="6" class="muted">No rows.</td></tr>';
    } else {
      body = rows.slice(0, 8).map(function (row) {
        var tier = row.tier || {};
        return '<tr class="' + rowClass(row.state) + '">' + [
          cell(strong(row.symbol) + '<div class="muted mini">' + esc(row.reason || '') + '</div>'),
          cell(fmt(row.pump_pct, 1) + '%'),
          cell(fmt(row.pullback_from_high_pct, 1) + '%<div class="muted mini">need ' + fmt(tier.pullback_pct, 0) + '%</div>'),
          cell(fmt(row.funding_prev_24h_pct, 3) + '%'),
          cell(fmt(row.oi_change_24h_pct, 1) + '%<div class="muted mini">LR ' + fmt(row.long_ratio, 2) + '</div>'),
          cell(esc(tier.rule_slug || '-'))
        ].join('') + '</tr>';
      }).join('');
    }
    return [
      '<div>',
      '<h3>' + esc(title) + '</h3>',
      '<div class="table-wrapper"><table class="table table--compact">',
      '<thead><tr><th>Symbol</th><th>Pump</th><th>PB</th><th>Funding</th><th>OI/LR</th><th>Rule</th></tr></thead>',
      '<tbody>' + body + '</tbody></table></div>',
      '</div>'
    ].join('');
  }

  function renderPaperEvents() {
    var body = $('pss-paper-events-body');
    var events = ((state.strategy_paper || {}).events_latest || []);
    if (!body) return;
    setText('pss-paper-events-note', events.length ? events.length + ' recent events' : 'No paper events');
    body.innerHTML = '';
    if (!events.length) {
      body.innerHTML = '<tr><td colspan="5" class="muted">No paper events.</td></tr>';
      return;
    }
    events.slice().reverse().forEach(function (event) {
      var details = [];
      ['reason', 'step', 'entry_price', 'margin_usd', 'notional_usd', 'topup_needed_usd', 'net_pnl_usd', 'fill_pnl_pct'].forEach(function (key) {
        if (event[key] !== undefined && event[key] !== null) details.push(key + '=' + event[key]);
      });
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(formatMs(event.ts_ms)),
        cell(statusBadge(event.event || '-')),
        cell(esc(event.strategy_id || '-')),
        cell(strong(event.symbol || '-')),
        cell('<span class="mini">' + esc(details.join(', ') || '-') + '</span>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderActiveWindow() {
    var body = $('pss-active-window-body');
    var activeWindow = state.active_window || {};
    var rows = activeWindow.rows || [];
    if (!body) return;
    setText('pss-active-window-note', rows.length
      ? rows.length + ' symbols · ' + esc(activeWindow.requests_made || 0) + ' requests · errors ' + esc(activeWindow.errors || 0)
      : 'No active windows');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted">No active windows.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var probeState = row.long_broad_state_5m || '-';
      var eventId = row.event_id || row.slow_pump_event_id || '';
      var pumpPct = row.trigger_pump_pct;
      if (pumpPct === undefined || pumpPct === null || pumpPct === '') pumpPct = row.slow_pump_return_pct;
      var tr = document.createElement('tr');
      if (probeState === 'entry_ready') tr.className = '';
      else if (String(probeState).indexOf('blocked') === 0) tr.className = 'row--danger';
      else tr.className = 'row--warning';
      tr.innerHTML = [
        cell(strong(row.symbol) + '<div class="muted mini">' + esc(eventId) + '</div>'),
        cell(esc(row.source_status || '-') + '<div class="muted mini">' + esc(row.active_source || '') + '</div>'),
        cell(fmt(pumpPct, 1) + '%<div class="muted mini">' + fmt(row.hours_since_trigger, 2) + 'h ago</div>'),
        cell(price(row.last_close_5m) + '<div class="muted mini">' + fmt(row.return_from_trigger_pct_5m, 2) + '% from trigger</div>'),
        cell(fmt(row.premium_latest_pct_5m, 3) + '%<div class="muted mini">min4h ' + fmt(row.premium_min_4h_pct_5m, 3) + '% · relief ' + fmt(row.premium_relief_1h_pct_5m, 3) + '%</div>'),
        cell(fmt(row.oi_change_1h_pct_5m, 1) + '% / ' + fmt(row.oi_change_4h_pct_5m, 1) + '%<div class="muted mini">1h / 4h</div>'),
        cell(fmt(row.volume_z_24h_5m, 2) + '<div class="muted mini">basis ' + fmt(row.mark_index_basis_pct_5m, 3) + '%</div>'),
        cell(statusBadge(probeState) + '<div class="muted mini">' + esc(row.long_broad_reason_5m || '') + '</div>'),
        cell(esc(row.klines_5m || 0) + ' candles<div class="muted mini">prem ' + esc(row.premium_points_5m || 0) + ' · oi ' + esc(row.oi_points_5m || 0) + '</div>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderSlowPumpWatch() {
    var body = $('pss-slow-watch-body');
    var watch = state.slow_pump_watch || {};
    var rows = watch.rows || [];
    if (!body) return;
    setText('pss-slow-watch-note', rows.length
      ? rows.length + ' research candidates · no paper or live entries'
      : 'Research only · no paper or live entries');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted">No slow-pump research candidates.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.className = row.slow_pump_stage === 'capitulation' ? 'row--danger' : 'row--warning';
      tr.innerHTML = [
        cell(strong(row.symbol) + '<div class="muted mini">' + esc(row.slow_pump_event_id || '') + '</div>'),
        cell(statusBadge(row.slow_pump_stage || 'watch')),
        cell(fmt(row.slow_pump_window_h, 0) + 'h / ' + fmt(row.slow_pump_return_pct, 1) + '%<div class="muted mini">threshold ' + fmt(row.slow_pump_threshold_pct, 0) + '%</div>'),
        cell(price(row.slow_pump_trigger_close) + ' / ' + price(row.last_close)),
        cell('+' + fmt(row.slow_pump_high_since_trigger_pct, 1) + '% / -' + fmt(row.slow_pump_pullback_from_high_pct, 1) + '%'),
        cell(fmt(row.slow_pump_oi_change_4h_pct, 1) + '% / ' + fmt(row.slow_pump_oi_change_24h_pct, 1) + '%<div class="muted mini">4h / 24h</div>'),
        cell(fmt(row.slow_pump_funding_prev_24h_pct, 3) + '% / ' + fmt(row.slow_pump_long_ratio, 3)),
        cell(fmt(row.slow_pump_hours_since_trigger, 1) + 'h'),
        cell(esc(row.research_mode || 'research_only_no_trades'))
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderCyclePaper() {
    var body = $('pss-cycle-body');
    var cycle = state.cycle_paper || {};
    var summary = cycle.summary || {};
    var positions = cycle.positions || [];
    if (!body) return;
    setText('pss-cycle-note', positions.length
      ? [
        (summary.open_positions || 0) + ' open',
        (summary.short_open_positions || 0) + ' short',
        (summary.long_open_positions || 0) + ' long',
        'free ' + (summary.free_total_slots || 0),
        'ROI ' + fmt(summary.roi_mark_pct || 0, 2) + '%',
        'DD ' + fmt(summary.max_drawdown_pct || 0, 2) + '%'
      ].join(' · ')
      : 'No cycle paper positions');
    body.innerHTML = '';
    if (!positions.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted">No cycle paper positions.</td></tr>';
      return;
    }
    positions.slice(0, 40).forEach(function (row) {
      var tr = document.createElement('tr');
      if (row.status === 'open' && row.current_topup_needed_usd > 0) tr.className = 'row--danger';
      else if (row.status === 'closed') tr.className = 'row--muted';
      else if (row.side === 'long') tr.className = row.current_pnl_pct >= 0 ? '' : 'row--warning';
      var targetStop = price(row.target_price);
      if (row.stop_price) targetStop += '<div class="muted mini">SL ' + price(row.stop_price) + '</div>';
      else targetStop += '<div class="muted mini">no hard SL</div>';
      tr.innerHTML = [
        cell(strong(String(row.side || '-').toUpperCase() + ' ' + (row.symbol || '-')) + '<div class="muted mini">' + esc(row.event_id || '') + '</div>'),
        cell(statusBadge(row.exit_reason || row.status || '-')),
        cell(esc(row.track_id || '-') + '<div class="muted mini">' + esc(((row.tier || {}).rule_slug) || '') + '</div>'),
        cell(stepsCellFromLegs(row)),
        cell(priceCell(row)),
        cell(targetStop),
        cell(pctCell(row.current_pnl_pct) + '<div class="muted mini">' + signedMoney(row.combined_pnl_usd || row.current_unrealized_pnl_usd || 0) + '</div>'),
        cell(money(row.current_topup_needed_usd) + '<div class="muted mini">peak ' + money(row.peak_topup_needed_usd) + '</div>'),
        cell(fmt(row.time_in_trade_h, 1) + 'h<div class="muted mini">' + fmt(row.hours_left_h, 1) + 'h left</div>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderCycleTrackSummary() {
    var body = $('pss-cycle-track-body');
    var cycle = state.cycle_paper || {};
    var rows = cycle.track_summaries || [];
    var skip = cycle.skip_summary || {};
    if (!body) return;
    setText('pss-cycle-track-note', rows.length
      ? rows.length + ' tracks В· skipped ' + esc(skip.total || 0)
      : 'No track summary');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="8" class="muted">No track summary.</td></tr>';
      return;
    }
    rows.slice(0, 20).forEach(function (row) {
      var tr = document.createElement('tr');
      if ((row.combined_pnl_usd || 0) < 0) tr.className = 'row--warning';
      tr.innerHTML = [
        cell(strong(row.track_id || '-') + '<div class="muted mini">ROI ' + fmt(row.roi_on_initial_pct, 2) + '%</div>'),
        cell(esc(row.side || '-')),
        cell(esc(row.open_positions || 0) + ' open / ' + esc(row.closed_positions || 0) + ' closed'),
        cell(row.win_pct === null || row.win_pct === undefined ? '-' : fmt(row.win_pct, 1) + '%'),
        cell(signedMoney(row.realized_pnl_usd || 0)),
        cell(signedMoney(row.unrealized_pnl_usd || 0)),
        cell(signedMoney(row.combined_pnl_usd || 0)),
        cell(money(row.current_topup_needed_usd || 0) + '<div class="muted mini">peak ' + money(row.peak_topup_needed_usd || 0) + '</div>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderCandidateShadow() {
    var body = $('pss-candidate-body');
    var candidate = ((state.cycle_paper || {}).candidate_shadow || {});
    var tracks = candidate.tracks || [];
    if (!body) return;
    setText('pss-candidate-note', tracks.length
      ? tracks.length + ' candidate tracks В· ready ' + esc(candidate.ready_count || 0) + ' В· no slot impact'
      : 'No candidate tracks');
    body.innerHTML = '';
    if (!tracks.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted">No candidate tracks.</td></tr>';
      return;
    }
    tracks.forEach(function (track) {
      var paper = track.paper || {};
      var topReady = (track.top_ready || []).slice(0, 3).map(function (item) {
        return esc(item.symbol || '-') + ' (' + fmt(item.trigger_pump_pct || item.pump_pct, 0) + '%)';
      }).join('<br>');
      var tr = document.createElement('tr');
      if ((paper.combined_pnl_usd || 0) < 0) tr.className = 'row--warning';
      else if ((track.ready || 0) <= 0 && (track.blocked || 0) > 0) tr.className = 'row--warning';
      tr.innerHTML = [
        cell(strong(track.track_id || '-')),
        cell(esc(track.ready || 0)),
        cell(esc(track.watch || 0)),
        cell(esc(track.blocked || 0)),
        cell(esc(paper.open_positions || 0) + ' open / ' + esc(paper.closed_positions || 0) + ' closed'),
        cell(signedMoney(paper.combined_pnl_usd || 0) + '<div class="muted mini">ROI ' + fmt(paper.roi_on_initial_pct || 0, 2) + '%</div>'),
        cell(money(paper.current_topup_needed_usd || 0) + '<div class="muted mini">peak ' + money(paper.peak_topup_needed_usd || 0) + '</div>'),
        cell(topReady || '<span class="muted">-</span>'),
        cell(esc(track.mode || 'shadow'))
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderCycleEvents() {
    var body = $('pss-cycle-events-body');
    var events = ((state.cycle_paper || {}).events_latest || []);
    if (!body) return;
    setText('pss-cycle-events-note', events.length ? events.length + ' recent events' : 'No cycle paper events');
    body.innerHTML = '';
    if (!events.length) {
      body.innerHTML = '<tr><td colspan="5" class="muted">No cycle paper events.</td></tr>';
      return;
    }
    events.slice().reverse().forEach(function (event) {
      var details = [];
      ['reason', 'state', 'step', 'topup_needed_usd', 'net_pnl_usd'].forEach(function (key) {
        if (event[key] !== undefined && event[key] !== null) details.push(key + '=' + event[key]);
      });
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(formatMs(event.ts_ms)),
        cell(statusBadge(event.event || '-')),
        cell(esc(event.side || '-')),
        cell(strong(event.symbol || '-')),
        cell('<span class="mini">' + esc(details.join(', ') || '-') + '</span>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderLegacyPaper() {
    var body = $('pss-legacy-body');
    var legacy = state.legacy_paper || {};
    var positions = legacy.open || [];
    if (!body) return;
    setText('pss-legacy-note', legacy.note || (positions.length ? positions.length + ' open' : 'No legacy paper positions'));
    body.innerHTML = '';
    if (!positions.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted">No legacy paper positions.</td></tr>';
      return;
    }
    positions.forEach(function (row) {
      var tr = document.createElement('tr');
      if (row.requires_topup) tr.className = 'row--danger';
      else if (row.had_peak_topup) tr.className = 'row--warning';
      tr.innerHTML = [
        cell(strong(row.symbol) + '<div class="muted mini">' + esc(row.entry_strategy || '') + '</div>'),
        cell(statusBadge(row.requires_topup ? 'top-up' : row.status || 'open')),
        cell(stepsCell(row)),
        cell(priceCell(row)),
        cell(price(row.target_price) + '<div class="muted mini">TP ' + fmt(row.tp_pct, 0) + '%</div>'),
        cell(pctCell(row.current_pnl_pct) + '<div class="muted mini">' + money(row.current_unrealized_pnl_usd) + '</div>'),
        cell(money(row.current_topup_needed_usd) + '<div class="muted mini">peak ' + money(row.peak_topup_needed_usd) + '</div>'),
        cell(fmt(row.funding_prev_24h_pct, 3) + '%<div class="muted mini">OI ' + fmt(row.oi_change_24h_pct, 1) + '%</div>'),
        cell(fmt(row.time_in_trade_h, 1) + 'h<div class="muted mini">' + fmt(row.hours_left_h, 1) + 'h left</div>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderAudit() {
    var audit = state.audit || {};
    var rows = audit.latest || [];
    var body = $('pss-audit-body');
    setText('pss-audit-file', audit.file || '-');
    if (!body) return;
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="4" class="muted">No audit rows.</td></tr>';
      return;
    }
    rows.slice().reverse().forEach(function (row) {
      var strategies = row.strategies || [];
      var ready = strategies.map(function (strategy) {
        var counts = (((strategy || {}).signals || {}).counts || {});
        return esc(strategy.strategy_id || '-') + ':' + esc(counts.entry_ready || 0);
      }).join(', ');
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(formatMs(row.ts_ms)),
        cell('<span class="mini">' + esc(row.audit_key || '') + '</span>'),
        cell(esc(((row.shadow || {}).status) || '-')),
        cell('<span class="mini">' + ready + '</span>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function stepsCell(row) {
    var ladder = row.ladder || [];
    var chips = ladder.map(function (step) {
      var cls = step.filled ? 'step-chip step-chip--filled' : 'step-chip';
      return '<span class="' + cls + '">' + esc(step.step) + '</span>';
    }).join('');
    return '<div class="step-strip">' + chips + '</div><div class="muted mini">' + esc(row.filled_steps || 0) + ' / ' + esc(row.planned_steps || 0) + '</div>';
  }

  function stepsCellFromLegs(row) {
    var legs = row.legs || [];
    var chips = legs.map(function (leg) {
      var cls = leg.closed ? 'step-chip step-chip--closed' : leg.filled ? 'step-chip step-chip--filled' : 'step-chip';
      return '<span class="' + cls + '" title="' + esc(price(leg.trigger_price)) + '">' + esc(leg.step) + '</span>';
    }).join('');
    return '<div class="step-strip">' + chips + '</div><div class="muted mini">' + esc(row.filled_steps || 0) + ' / ' + esc(row.planned_steps || legs.length || 0) + '</div>';
  }

  function priceCell(row) {
    return [
      '<div>E ' + price(row.entry_price) + '</div>',
      '<div>C ' + price(row.current_price) + '</div>',
      '<div class="muted mini">A ' + price(row.avg_entry_price) + '</div>'
    ].join('');
  }

  function metric(label, value) {
    return '<div><span>' + esc(label) + '</span><strong>' + value + '</strong></div>';
  }

  function statusClass(status) {
    if (status === 'complete' || status === 'entry_ready' || status === 'open') return 'ready';
    if (status === 'running' || status === 'starting' || status === 'waiting' || String(status).indexOf('waiting') === 0) return 'pending';
    if (status === 'error' || status === 'top-up' || String(status).indexOf('blocked') === 0) return 'error';
    return 'idle';
  }

  function stateClass(status) {
    if (status === 'entry_ready') return 'ready';
    if (String(status || '').indexOf('waiting') === 0) return 'pending';
    if (String(status || '').indexOf('blocked') === 0) return 'error';
    return 'idle';
  }

  function rowClass(status) {
    if (status === 'entry_ready') return '';
    if (String(status || '').indexOf('blocked') === 0) return 'row--danger';
    if (String(status || '').indexOf('waiting') === 0) return 'row--warning';
    return '';
  }

  function statusBadge(status, title) {
    return '<span class="status-pill status-pill--' + statusClass(status) + '" title="' + esc(title || status || '') + '">' + esc(status || '-') + '</span>';
  }

  function pctCell(value) {
    var n = Number(value || 0);
    var cls = n >= 0 ? 'value-positive' : 'value-negative';
    return '<span class="' + cls + '">' + fmt(n, 2) + '%</span>';
  }

  function money(value) {
    var n = Number(value || 0);
    var sign = n < 0 ? '-' : '';
    return sign + '$' + Math.abs(n).toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 2 });
  }

  function signedMoney(value) {
    var n = Number(value || 0);
    var cls = n >= 0 ? 'value-positive' : 'value-negative';
    return '<span class="' + cls + '">' + money(n) + '</span>';
  }

  function price(value) {
    var n = Number(value || 0);
    if (!n) return '-';
    return n.toPrecision(n < 1 ? 6 : 8).replace(/0+$/, '').replace(/\.$/, '');
  }

  function fmt(value, digits) {
    var n = Number(value);
    if (!isFinite(n)) return '-';
    return n.toFixed(digits);
  }

  function shortTime(value) {
    var full = formatMs(value);
    return full ? full.replace(' UTC', '').slice(5, 16) : '-';
  }

  function formatMs(value) {
    var n = Number(value || 0);
    if (!n) return null;
    try {
      return new Date(n).toISOString().replace('T', ' ').replace('.000Z', ' UTC');
    } catch (err) {
      return null;
    }
  }

  function cell(value) {
    return '<td>' + (value === null || value === undefined ? '' : value) + '</td>';
  }

  function strong(value) {
    return '<strong>' + esc(value || '-') + '</strong>';
  }

  function esc(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function setText(id, text) {
    var node = $(id);
    if (node) node.textContent = text === null || text === undefined ? '' : String(text);
  }

  function setDisabled(id, disabled) {
    var node = $(id);
    if (node) node.disabled = !!disabled;
  }

  function setStatus(message, isError) {
    var node = $('pss-control-status');
    if (!node) return;
    node.textContent = message || '';
    node.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function bind() {
    if ($('pss-refresh')) $('pss-refresh').addEventListener('click', refresh);
    if ($('pss-run-scan')) $('pss-run-scan').addEventListener('click', runScan);
    if ($('pss-start-schedule')) $('pss-start-schedule').addEventListener('click', startSchedule);
    if ($('pss-stop-schedule')) $('pss-stop-schedule').addEventListener('click', stopSchedule);
    if ($('pss-live-preflight')) $('pss-live-preflight').addEventListener('click', livePreflight);
    if ($('pss-live-prepare')) $('pss-live-prepare').addEventListener('click', livePrepare);
    if ($('pss-live-arm')) $('pss-live-arm').addEventListener('click', liveArm);
    if ($('pss-live-capital-save')) $('pss-live-capital-save').addEventListener('click', liveSetCapital);
    if ($('pss-live-capital-promote')) $('pss-live-capital-promote').addEventListener('click', livePromoteCapital);
    if ($('pss-live-disarm')) $('pss-live-disarm').addEventListener('click', liveDisarm);
    if ($('pss-live-emergency')) $('pss-live-emergency').addEventListener('click', liveEmergencyClose);
  }

  bind();
  render();
  startPolling();
}());
