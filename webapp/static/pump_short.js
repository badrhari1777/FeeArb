(function () {
  'use strict';

  var rootPath = window.__ROOT_PATH__ || '';
  var state = window.__PUMP_SHORT_DASHBOARD_INITIAL__ || {};
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
    requestJson('GET', '/api/pump-short/dashboard', null, function (err, data) {
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
    pollTimer = window.setInterval(refresh, 3000);
  }

  function controlPayload() {
    var symbols = [];
    var parts = String(($('psd-symbols') && $('psd-symbols').value) || '').split(',');
    parts.forEach(function (part) {
      var clean = part.replace(/\s+/g, '').toUpperCase();
      if (clean) symbols.push(clean);
    });
    return {
      lookback_days: 14,
      sleep_sec: 0.8,
      max_symbols: Number(($('psd-max-symbols') && $('psd-max-symbols').value) || 50),
      symbols: symbols,
      newest_first: true,
      recent_event_hours: Number(($('psd-recent-hours') && $('psd-recent-hours').value) || 168)
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
    payload.interval_sec = Number(($('psd-interval-sec') && $('psd-interval-sec').value) || 3600);
    payload.run_immediately = true;
    setStatus('Starting schedule...', false);
    requestJson('POST', '/api/pump-short/bybit/shadow/schedule/start', payload, function (err) {
      if (err) {
        setStatus(err.message, true);
        return;
      }
      setStatus('Schedule started', false);
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
    renderStatus();
    renderCapital();
    renderStrategy();
    renderOpenPositions();
    renderSignals();
    renderClosed();
  }

  function renderStatus() {
    var shadow = state.shadow || {};
    var schedule = state.schedule || {};
    var capital = state.capital || {};
    var strategy = state.strategy || {};
    var status = shadow.status || 'idle';
    var pill = $('psd-shadow-pill');
    if (pill) {
      pill.textContent = status;
      pill.className = 'status-pill status-pill--' + statusClass(status);
    }
    setText('psd-schedule-status', schedule.status || 'idle');
    setText('psd-open-count', String(capital.active_open_positions || 0) + ' / ' + String(strategy.max_active_coins || capital.max_active_coins || 0));
    setText('psd-capital-head', money(capital.usable_capital_left_usd) + ' left');
    setText('psd-last-update', 'Updated ' + (formatMs(shadow.updated_at_ms) || formatMs(shadow.finished_at_ms) || '-'));

    var scheduleRunning = ['running', 'starting', 'waiting', 'stopping'].indexOf(schedule.status || '') >= 0;
    var scanRunning = ['running', 'starting'].indexOf(status) >= 0;
    setDisabled('psd-run-scan', scanRunning || scheduleRunning);
    setDisabled('psd-start-schedule', scanRunning || scheduleRunning);
    setDisabled('psd-stop-schedule', !scheduleRunning);
  }

  function renderCapital() {
    var capital = state.capital || {};
    var band = $('psd-capital-band');
    if (band) {
      band.className = 'pump-dashboard-band pump-dashboard-band--' + (capital.severity || 'ok');
    }
    setText('psd-initial-capital', money(capital.initial_capital_usd));
    setText('psd-free-capital', money(capital.usable_capital_left_usd));
    setText('psd-used-margin', money(capital.used_margin_usd));
    setText('psd-current-topup', money(capital.current_topup_needed_usd));
    setText('psd-peak-topup', money(capital.peak_topup_needed_usd));
    setText('psd-unrealized', money(capital.current_unrealized_pnl_usd));
  }

  function renderStrategy() {
    var strategy = state.strategy || {};
    var capital = state.capital || {};
    setText('psd-strategy-venue', strategy.venue || 'bybit');
    setText('psd-strategy-entry', strategy.entry || 'pb20');
    setText('psd-strategy-funding', String(strategy.funding_window_h || 24) + 'h > ' + fmt(strategy.funding_min_pct, 1) + '%');
    setText('psd-strategy-exit', 'TP' + fmt(strategy.tp_pct, 0) + ' / ' + String(strategy.max_hold_h || 168) + 'h');
    setText('psd-strategy-steps', String(strategy.ladder_legs || 4) + ' x ' + fmt(strategy.ladder_step_pct, 0) + '%');
    setText('psd-step-notional', money(capital.per_step_notional_usd));
  }

  function renderOpenPositions() {
    var body = $('psd-open-body');
    var positions = ((state.positions || {}).open || []);
    if (!body) return;
    body.innerHTML = '';
    setText('psd-open-note', positions.length ? (positions.length + ' open shadow positions') : 'No open positions');
    if (!positions.length) {
      body.innerHTML = '<tr><td colspan="11" class="muted">No open shadow positions.</td></tr>';
      return;
    }
    positions.forEach(function (row) {
      var tr = document.createElement('tr');
      if (row.requires_topup) tr.className = 'row--danger';
      else if (row.had_peak_topup) tr.className = 'row--warning';
      tr.innerHTML = [
        cell(strong(row.symbol) + '<div class="muted mini">' + esc(row.profile || '') + '</div>'),
        cell(statusBadge(row.requires_topup ? 'top-up' : row.status || 'open')),
        cell(stepsCell(row)),
        cell(priceCell(row)),
        cell(price(row.target_price) + '<div class="muted mini">TP ' + fmt(row.tp_pct, 0) + '%</div>'),
        cell(money(row.used_margin_usd) + '<div class="muted mini">' + money(row.per_step_margin_usd) + '/step</div>'),
        cell(money(row.gross_notional_usd) + '<div class="muted mini">' + money(row.per_step_notional_usd) + '/step</div>'),
        cell(pctCell(row.current_pnl_pct) + '<div class="muted mini">' + money(row.current_unrealized_pnl_usd) + '</div>'),
        cell(money(row.current_topup_needed_usd) + '<div class="muted mini">peak ' + money(row.peak_topup_needed_usd) + '</div>'),
        cell(fmt(row.funding_prev_24h_pct, 3) + '%<div class="muted mini">OI ' + fmt(row.oi_change_24h_pct, 1) + '%</div>'),
        cell(fmt(row.time_in_trade_h, 1) + 'h<div class="muted mini">' + fmt(row.hours_left_h, 1) + 'h left</div>')
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderSignals() {
    var signals = state.signals || {};
    var candidates = signals.entry_candidates || [];
    var watch = signals.watchlist || [];
    var blocked = signals.blocked || [];
    setText('psd-signal-counts', candidates.length + ' candidates, ' + watch.length + ' watch, ' + blocked.length + ' blocked');
    renderSignalTable('psd-candidates-body', candidates, ['symbol', 'trigger_pump_pct', 'pullback_from_high_pct', 'funding_prev_24h_pct', 'reason'], 'No candidates.');
    renderSignalTable('psd-watch-body', watch, ['symbol', 'status', 'pullback_from_high_pct', 'oi_change_24h_pct', 'reason'], 'No watch rows.');
    renderSignalTable('psd-blocked-body', blocked, ['symbol', 'status', 'funding_prev_24h_pct', 'oi_change_24h_pct', 'reason'], 'No blocked rows.');
  }

  function renderSignalTable(id, rows, columns, emptyText) {
    var body = $(id);
    if (!body) return;
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="' + columns.length + '" class="muted">' + esc(emptyText) + '</td></tr>';
      return;
    }
    rows.slice(0, 20).forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = columns.map(function (column) {
        var value = row[column];
        if (column.indexOf('_pct') >= 0 || column === 'trigger_pump_pct') return cell(fmt(value, 2) + '%');
        return cell(value);
      }).join('');
      body.appendChild(tr);
    });
  }

  function renderClosed() {
    var body = $('psd-closed-body');
    var positions = ((state.positions || {}).recent_closed || []);
    if (!body) return;
    body.innerHTML = '';
    setText('psd-closed-count', String((state.positions || {}).closed_count || 0) + ' closed');
    if (!positions.length) {
      body.innerHTML = '<tr><td colspan="6" class="muted">No closed positions.</td></tr>';
      return;
    }
    positions.slice(0, 20).forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.symbol),
        cell(row.exit_reason || '-'),
        cell(pctCell(row.combined_net_pct)),
        cell(money(row.realized_pnl_usd)),
        cell(fmt(row.mfe_pct, 1) + '% / ' + fmt(row.mae_pct, 1) + '%'),
        cell(formatMs(row.closed_at_ms))
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

  function priceCell(row) {
    return [
      '<div>E ' + price(row.entry_price) + '</div>',
      '<div>C ' + price(row.current_price) + '</div>',
      '<div class="muted mini">A ' + price(row.avg_entry_price) + '</div>'
    ].join('');
  }

  function statusClass(status) {
    if (status === 'complete' || status === 'open') return 'ready';
    if (status === 'running' || status === 'starting' || status === 'waiting') return 'pending';
    if (status === 'error' || status === 'top-up') return 'error';
    return 'idle';
  }

  function statusBadge(status) {
    return '<span class="status-pill status-pill--' + statusClass(status) + '">' + esc(status || '-') + '</span>';
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
    var node = $('psd-control-status');
    if (!node) return;
    node.textContent = message || '';
    node.className = isError ? 'settings-status settings-status--error' : 'settings-status';
  }

  function bind() {
    if ($('psd-refresh')) $('psd-refresh').addEventListener('click', refresh);
    if ($('psd-run-scan')) $('psd-run-scan').addEventListener('click', runScan);
    if ($('psd-start-schedule')) $('psd-start-schedule').addEventListener('click', startSchedule);
    if ($('psd-stop-schedule')) $('psd-stop-schedule').addEventListener('click', stopSchedule);
  }

  bind();
  render();
  startPolling();
}());
