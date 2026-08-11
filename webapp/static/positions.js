(function () {
  'use strict';

  var REFRESH_MS = 15000;
  var refreshTimer = null;
  var nextRefreshAt = 0;
  var inFlight = false;

  function $(id) {
    return document.getElementById(id);
  }

  function esc(value) {
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

  function number(value, digits) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(parsed)) {
      return '-';
    }
    return parsed.toFixed(typeof digits === 'number' ? digits : 2);
  }

  function money(value) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(parsed)) {
      return '-';
    }
    return (parsed < 0 ? '-$' : '$') + Math.abs(parsed).toFixed(2);
  }

  function date(value) {
    if (!value) {
      return '-';
    }
    var parsed = new Date(value);
    return isNaN(parsed.getTime()) ? '-' : parsed.toLocaleString();
  }

  function setText(id, value) {
    var node = $(id);
    if (node) {
      node.textContent = value;
    }
  }

  function setTone(id, value, tone) {
    var node = $(id);
    if (!node) {
      return;
    }
    node.textContent = value;
    node.className = tone || '';
  }

  function riskBadge(level) {
    var normalized = String(level || 'unknown').toLowerCase();
    var cls = normalized === 'high'
      ? 'status-pill status-pill--error'
      : (normalized === 'warn' ? 'status-pill status-pill--pending' : 'status-pill status-pill--ready');
    return '<span class="' + cls + '">' + esc(normalized) + '</span>';
  }

  function request(callback) {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', '/api/positions/overview', true);
    xhr.setRequestHeader('Accept', 'application/json');
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) {
        return;
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          callback(null, JSON.parse(xhr.responseText || '{}'));
        } catch (err) {
          callback(err, null);
        }
      } else {
        callback(new Error('Request failed (' + xhr.status + ')'), null);
      }
    };
    xhr.onerror = function () {
      callback(new Error('Network error'), null);
    };
    xhr.send();
  }

  function protectionText(card) {
    var legs = Array.isArray(card.legs) ? card.legs : [];
    if (!legs.length) {
      return '-';
    }
    var stops = 0;
    var takes = 0;
    legs.forEach(function (leg) {
      if (Number(leg.stop_price || 0) > 0) stops += 1;
      if (Number(leg.take_price || 0) > 0) takes += 1;
    });
    return stops + '/' + legs.length + ' stops · ' + takes + '/' + legs.length + ' takes';
  }

  function renderMain(main) {
    var positions = Array.isArray(main.positions) ? main.positions : [];
    var body = $('pp-main-body');
    setText('pp-main-freshness', 'Account snapshot: ' + date(main.account_last_updated || main.last_updated));
    if (!body) {
      return;
    }
    if (!positions.length) {
      body.innerHTML = '<tr><td colspan="10" class="muted">No open positions in the main module.</td></tr>';
      return;
    }
    body.innerHTML = positions.map(function (card) {
      var pnl = Number(card.net_pnl || 0);
      var pnlClass = pnl > 0 ? 'value-positive' : (pnl < 0 ? 'value-negative' : '');
      var summary = card.position_summary || {};
      var autoExit = card.auto_exit || {};
      var exchanges = (card.long_exchange || '-') + ' long / ' + (card.short_exchange || '-') + ' short';
      return '<tr>' +
        '<td><strong>' + esc(card.symbol || '-') + '</strong><span class="cell-note">' + esc(card.pair_label || '-') + '</span></td>' +
        '<td>' + esc(exchanges) + '</td>' +
        '<td class="' + pnlClass + '">' + money(pnl) + '</td>' +
        '<td>' + money(summary.amount_usdt) + '</td>' +
        '<td>' + money(card.expected_funding) + '</td>' +
        '<td>' + (card.live_spread_pct === null || card.live_spread_pct === undefined ? '-' : number(card.live_spread_pct, 3) + '%') + '</td>' +
        '<td>' + (card.liq_distance_pct === null || card.liq_distance_pct === undefined ? '-' : number(card.liq_distance_pct, 2) + '%') + '</td>' +
        '<td>' + esc(protectionText(card)) + '</td>' +
        '<td>' + esc(autoExit.status || (autoExit.spread_enabled ? 'enabled' : 'off')) + '</td>' +
        '<td>' + riskBadge(card.risk_level) + '</td>' +
      '</tr>';
    }).join('');
  }

  function ladderTable(position) {
    var legs = Array.isArray(position.legs) ? position.legs : [];
    if (!legs.length) {
      return '<p class="muted">No ladder rows.</p>';
    }
    return '<div class="table-wrapper"><table class="table table--compact">' +
      '<thead><tr><th>Step</th><th>Status</th><th>Weight</th><th>Trigger</th><th>Margin</th><th>Notional</th><th>Filled qty</th><th>Fill price</th></tr></thead>' +
      '<tbody>' + legs.map(function (leg) {
        return '<tr><td>' + esc(leg.step || '-') + '</td><td>' + esc(leg.status || '-') + '</td>' +
          '<td>' + number(leg.weight, 2) + '</td><td>' + number(leg.trigger_price, 8) + '</td>' +
          '<td>' + money(leg.margin_usd) + '</td><td>' + money(leg.notional_usd) + '</td>' +
          '<td>' + number(leg.filled_qty, 8) + '</td><td>' + number(leg.avg_fill_price, 8) + '</td></tr>';
      }).join('') + '</tbody></table></div>';
  }

  function renderPump(pump) {
    var positions = Array.isArray(pump.positions) ? pump.positions : [];
    var config = pump.config || {};
    var balance = pump.balance || {};
    var notifications = pump.notifications || {};
    var regime = pump.capital_regime || {};
    var marginManager = pump.margin_manager || {};
    var sharedPool = pump.shared_pool || {};
    var autoTransfer = pump.auto_transfer || {};
    var cards = $('pp-pump-cards');
    var totalTopup = positions.reduce(function (sum, row) {
      return sum + Number(row.margin_topup_usd || 0);
    }, 0);
    setText('pp-pump-status', pump.status || '-');
    setText('pp-pump-total', money(balance.total_usd || 0));
    setText('pp-pump-available', money(balance.available_usd || 0));
    setText('pp-pump-reserve', money(config.reserve_usd || 0));
    setText('pp-pump-topup', money(totalTopup));
    setText('pp-pump-temporary', money(balance.temporary_occupied_usd || 0));
    setText('pp-pump-regime', String(regime.mode || '-').toUpperCase());
    setText(
      'pp-pump-margin-manager',
      (marginManager.policy_id || '-') +
        ' · W' + number(marginManager.ladder_watch_distance_pct || 0, 0) +
        ' / A' + number(marginManager.ladder_activation_distance_pct || 0, 0) +
        ' / R' + number(marginManager.ladder_release_distance_pct || 0, 0)
    );
    setText(
      'pp-pump-entry-headroom',
      sharedPool.entry_headroom_usd === null || sharedPool.entry_headroom_usd === undefined
        ? '-'
        : money(sharedPool.entry_headroom_usd)
    );
    setText(
      'pp-pump-stress-headroom',
      sharedPool.stress_headroom_usd === null || sharedPool.stress_headroom_usd === undefined
        ? '-'
        : money(sharedPool.stress_headroom_usd)
    );
    setText(
      'pp-pump-auto-transfer',
      autoTransfer.enabled
        ? ('ON · daily used ' + money(autoTransfer.daily_used_usd || 0))
        : 'OFF'
    );
    setText('pp-pump-notifications', notifications.last_status || (notifications.configured ? 'ready' : 'off'));
    setText('pp-pump-freshness', 'Protective cycle: ' + date(pump.last_cycle_at_ms));
    var arm = $('pp-pump-arm');
    if (arm) {
      arm.textContent = pump.entry_armed ? 'armed' : 'disarmed';
      arm.className = 'status-pill ' + (pump.entry_armed ? 'status-pill--ready' : 'status-pill--idle');
    }
    if (!cards) {
      return;
    }
    if (!positions.length) {
      cards.innerHTML = '<div class="position-detail-card"><strong>No open Pump Live positions.</strong>' +
        '<p class="muted">Status: ' + esc(pump.status || 'unknown') + ' · monitor: ' +
        (pump.monitor_thread_alive ? 'running' : 'idle') + '</p></div>';
      return;
    }
    cards.innerHTML = positions.map(function (row) {
      var pnl = Number(row.unrealized_pnl_usd || 0);
      var pnlClass = pnl > 0 ? 'value-positive' : (pnl < 0 ? 'value-negative' : '');
      var protectionOk = Number(row.tp_price || 0) > 0 && Number(row.stop_price || 0) > 0;
      return '<article class="position-detail-card position-detail-card--' + esc(row.risk_level || 'unknown') + '">' +
        '<div class="panel-heading"><div><h3>' + esc(row.symbol || '-') + ' · SHORT</h3>' +
        '<p class="muted">' + esc(row.strategy_id || '-') + ' · ' + esc(row.account_alias || 'bybit_pump') + '</p></div>' +
        '<div>' + riskBadge(row.risk_level) + ' <span class="status-pill status-pill--idle">' + esc(row.status || '-') + '</span></div></div>' +
        '<div class="position-metric-grid">' +
        '<div><span>Qty</span><strong>' + number(row.qty, 8) + '</strong></div>' +
        '<div><span>Entry / Mark</span><strong>' + number(row.avg_entry_price, 8) + ' / ' + number(row.mark_price, 8) + '</strong></div>' +
        '<div><span>PnL</span><strong class="' + pnlClass + '">' + money(pnl) + '</strong></div>' +
        '<div><span>TP / Catastrophic SL</span><strong>' + number(row.tp_price, 8) + ' / ' + number(row.stop_price, 8) + '</strong></div>' +
        '<div><span>Liq / Buffer</span><strong>' + number(row.liq_price, 8) + ' / ' + number(row.liq_buffer_pct, 2) + '%</strong></div>' +
        '<div><span>Protection</span><strong class="' + (protectionOk ? 'value-positive' : 'value-negative') + '">' +
          (protectionOk ? 'TP + SL present' : 'incomplete') + '</strong></div>' +
        '<div><span>Top-up / Base / Cap</span><strong>' + money(row.margin_topup_usd || 0) + ' / ' + money(row.margin_prefund_floor_usd || 0) + ' / ' + money(row.margin_topup_cap_usd) + '</strong></div>' +
        '<div><span>Ladder gate</span><strong>' + escapeHtml(row.ladder_gate_status || '-') +
          (row.ladder_gate_step ? ' · L' + escapeHtml(row.ladder_gate_step) : '') + '</strong></div>' +
        '<div><span>Ladder proximity</span><strong>' + escapeHtml(row.ladder_proximity_state || '-') +
          (row.ladder_distance_pct === null || row.ladder_distance_pct === undefined
            ? ''
            : ' · ' + number(row.ladder_distance_pct, 1) + '%') + '</strong></div>' +
        '<div><span>Hold left</span><strong>' + number(row.remaining_hold_h, 1) + 'h / ' + number(row.max_hold_h, 1) + 'h</strong></div>' +
        '<div><span>Ladder</span><strong>' + esc(row.legs_filled || 0) + ' filled · ' + esc(row.legs_open || 0) + ' open</strong></div>' +
        '</div><details><summary>Show ladder and exchange order state</summary>' + ladderTable(row) + '</details></article>';
    }).join('');
  }

  function eventDetails(event) {
    var compact = {};
    Object.keys(event || {}).forEach(function (key) {
      if (['event', 'ts_ms', 'symbol'].indexOf(key) === -1) {
        compact[key] = event[key];
      }
    });
    var text = JSON.stringify(compact);
    return text.length > 400 ? text.slice(0, 397) + '...' : text;
  }

  function renderEvents(events) {
    var body = $('pp-events-body');
    var rows = Array.isArray(events) ? events.slice().reverse() : [];
    if (!body) {
      return;
    }
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="4" class="muted">No Pump protective events yet.</td></tr>';
      return;
    }
    body.innerHTML = rows.map(function (event) {
      return '<tr><td>' + esc(date(event.ts_ms)) + '</td><td><strong>' + esc(event.event || '-') +
        '</strong></td><td>' + esc(event.symbol || '-') + '</td><td class="position-event-detail">' +
        esc(eventDetails(event)) + '</td></tr>';
    }).join('');
  }

  function render(payload) {
    var summary = payload.summary || {};
    var main = payload.main || {};
    var pump = payload.pump || {};
    setText('positions-page-generated', date(payload.generated_at_ms));
    setText('pp-main-count', String(summary.main_positions || 0));
    setText('pp-pump-count', String(summary.pump_positions || 0) + ' / ' + String(summary.pump_cap || 0));
    setTone('pp-total-pnl', money(summary.total_unrealized_pnl_usd || 0), Number(summary.total_unrealized_pnl_usd || 0) < 0 ? 'value-negative' : 'value-positive');
    setTone('pp-main-pnl', money(summary.main_unrealized_pnl_usd || 0), Number(summary.main_unrealized_pnl_usd || 0) < 0 ? 'value-negative' : 'value-positive');
    setTone('pp-pump-pnl', money(summary.pump_unrealized_pnl_usd || 0), Number(summary.pump_unrealized_pnl_usd || 0) < 0 ? 'value-negative' : 'value-positive');
    setText('pp-min-liq', summary.min_liq_buffer_pct === null || summary.min_liq_buffer_pct === undefined ? '-' : number(summary.min_liq_buffer_pct, 2) + '%');
    setText('pp-protection', String(summary.protection_issues || 0));
    setText('pp-risk-count', String(summary.high_risk_positions || 0) + ' / ' + String(summary.warning_risk_positions || 0));
    var risky = Number(summary.high_risk_positions || 0) > 0 || Number(summary.protection_issues || 0) > 0;
    var warning = !risky && Number(summary.warning_risk_positions || 0) > 0;
    var riskNode = $('positions-page-risk');
    if (riskNode) {
      riskNode.textContent = risky ? 'ACTION REQUIRED' : (warning ? 'WARNING' : 'PROTECTED');
      riskNode.className = 'status-pill ' + (risky ? 'status-pill--error' : (warning ? 'status-pill--pending' : 'status-pill--ready'));
    }
    var warningText = [];
    if (summary.protection_issues) warningText.push(summary.protection_issues + ' position group(s) have incomplete stop/take visibility');
    if (pump.blocked_reason) warningText.push('Pump blocked: ' + pump.blocked_reason);
    if (pump.last_error) warningText.push('Pump error: ' + pump.last_error);
    var warningNode = $('positions-page-warning');
    if (warningNode) {
      warningNode.textContent = warningText.join(' · ');
      warningNode.className = warningText.length ? 'settings-status settings-status--error' : 'settings-status';
    }
    var status = $('positions-page-status');
    if (status) {
      status.textContent = 'ready';
      status.className = 'status-pill status-pill--ready';
    }
    renderMain(main);
    renderPump(pump);
    renderEvents(pump.recent_events || []);
  }

  function refresh() {
    if (inFlight) {
      return;
    }
    inFlight = true;
    var status = $('positions-page-status');
    if (status) {
      status.textContent = 'refreshing';
      status.className = 'status-pill status-pill--pending';
    }
    request(function (err, payload) {
      inFlight = false;
      nextRefreshAt = Date.now() + REFRESH_MS;
      if (err) {
        if (status) {
          status.textContent = 'error';
          status.className = 'status-pill status-pill--error';
        }
        var warning = $('positions-page-warning');
        if (warning) {
          warning.textContent = err.message || 'Positions overview request failed';
          warning.className = 'settings-status settings-status--error';
        }
        return;
      }
      render(payload || {});
    });
  }

  function updateCountdown() {
    var seconds = nextRefreshAt ? Math.max(0, Math.ceil((nextRefreshAt - Date.now()) / 1000)) : 0;
    setText('positions-page-next-refresh', seconds + 's');
  }

  function init() {
    var refreshButton = $('positions-page-refresh');
    if (refreshButton) {
      refreshButton.addEventListener('click', refresh);
    }
    refresh();
    refreshTimer = window.setInterval(refresh, REFRESH_MS);
    window.setInterval(updateCountdown, 1000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
}());
