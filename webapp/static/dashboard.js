(function () {
  'use strict';

  var rootPath = String(window.__ROOT_PATH__ || '').replace(/\/$/, '');
  var state = window.__INITIAL_DASHBOARD__ || {};
  var settings = clone(state.settings || {});
  var refreshTimer = null;
  var countdownTimer = null;
  var refreshInFlight = false;
  var nextRefreshAt = 0;

  function $(id) { return document.getElementById(id); }

  function clone(value) {
    return JSON.parse(JSON.stringify(value || {}));
  }

  function escapeHtml(value) {
    if (value === null || value === undefined) return '';
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function number(value, digits) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    if (!isFinite(parsed)) return '—';
    return parsed.toLocaleString('ru-RU', {
      minimumFractionDigits: typeof digits === 'number' ? digits : 2,
      maximumFractionDigits: typeof digits === 'number' ? digits : 2
    });
  }

  function price(value) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    if (!isFinite(parsed)) return '—';
    var digits = Math.abs(parsed) >= 100 ? 3 : Math.abs(parsed) >= 1 ? 5 : 8;
    return number(parsed, digits);
  }

  function money(value) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    if (!isFinite(parsed)) return '—';
    return (parsed < 0 ? '−$' : '$') + number(Math.abs(parsed), 2);
  }

  function percent(value, digits) {
    var parsed = typeof value === 'number' ? value : parseFloat(value);
    return isFinite(parsed) ? number(parsed, typeof digits === 'number' ? digits : 2) + '%' : '—';
  }

  function date(value) {
    if (!value) return '—';
    var parsed = new Date(value);
    return isNaN(parsed.getTime()) ? '—' : parsed.toLocaleString('ru-RU');
  }

  function durationMinutes(value) {
    var parsed = Number(value);
    if (!isFinite(parsed)) return '—';
    if (parsed < 0) return 'событие прошло';
    if (parsed < 60) return number(parsed, 0) + ' мин';
    return number(parsed / 60, 1) + ' ч';
  }

  function setText(id, value) {
    var node = $(id);
    if (node) node.textContent = value;
  }

  function setTone(id, value, tone) {
    var node = $(id);
    if (!node) return;
    node.textContent = value;
    node.classList.remove('value-positive', 'value-negative', 'value-warning');
    if (tone) node.classList.add(tone);
  }

  function statusPill(value, label) {
    var normalized = String(value || 'unknown').toLowerCase();
    var cls = 'status-pill--idle';
    if (['ok', 'ready', 'armed', 'protected', 'healthy', 'running'].indexOf(normalized) >= 0) cls = 'status-pill--ready';
    if (['watch', 'warn', 'warning', 'partial', 'pending'].indexOf(normalized) >= 0) cls = 'status-pill--pending';
    if (['high', 'error', 'stress', 'failed', 'blocked', 'unavailable'].indexOf(normalized) >= 0) cls = 'status-pill--error';
    return '<span class="status-pill ' + cls + '">' + escapeHtml(label || normalized) + '</span>';
  }

  function pnlTone(value) {
    var parsed = Number(value || 0);
    return parsed > 0 ? 'value-positive' : parsed < 0 ? 'value-negative' : '';
  }

  function request(method, path, body) {
    return fetch(rootPath + path, {
      method: method,
      headers: {'Accept': 'application/json', 'Content-Type': 'application/json'},
      body: body === undefined ? undefined : JSON.stringify(body)
    }).then(function (response) {
      return response.text().then(function (text) {
        var payload = {};
        try { payload = text ? JSON.parse(text) : {}; } catch (_err) { payload = {}; }
        if (!response.ok) {
          throw new Error(payload.detail || ('HTTP ' + response.status));
        }
        return payload;
      });
    });
  }

  function renderSummary(payload) {
    var accounts = payload.accounts || {};
    var summaries = accounts.balance_summary || {};
    var overall = summaries.overall || {};
    var main = summaries.bybit_main || {};
    var pumpBalance = summaries.bybit_pump || {};
    var positions = payload.positions || {};
    var summary = positions.summary || {};
    var service = payload.service || {};

    setText('metric-total', money(overall.total));
    setText('metric-total-note', 'доступно ' + money(overall.available));
    setText('metric-bybit-main', money(main.total));
    setText('metric-bybit-main-note', 'доступно ' + money(main.available));
    setText('metric-bybit-pump', money(pumpBalance.total));
    setText('metric-bybit-pump-note', 'доступно ' + money(pumpBalance.available));
    setTone('metric-pnl', money(summary.total_unrealized_pnl_usd), pnlTone(summary.total_unrealized_pnl_usd));
    setText('metric-pnl-note', 'Main ' + money(summary.main_unrealized_pnl_usd) + ' · Pump ' + money(summary.pump_unrealized_pnl_usd));
    setText('metric-positions', String(Number(summary.main_positions || 0) + Number(summary.pump_positions || 0)));
    setText('metric-positions-note', 'Main ' + String(summary.main_positions || 0) + ' · Pump ' + String(summary.pump_positions || 0) + '/' + String(summary.pump_cap || 0));
    setText('metric-liq', percent(summary.min_liq_buffer_pct));
    setText('metric-protection', summary.protection_issues ? ('проблем защиты: ' + summary.protection_issues) : 'видимая защита в норме');
    setText('dashboard-updated', date(accounts.last_updated || service.last_updated));
    setText('dashboard-error', service.last_error || 'нет');

    var serviceStatus = $('dashboard-status');
    if (serviceStatus) {
      serviceStatus.textContent = service.status || 'unknown';
      serviceStatus.className = 'status-pill ' + (service.status === 'ready' ? 'status-pill--ready' : service.last_error ? 'status-pill--error' : 'status-pill--pending');
    }
    var high = Number(summary.high_risk_positions || 0);
    var warnings = Number(summary.warning_risk_positions || 0);
    var protection = Number(summary.protection_issues || 0);
    var riskNode = $('dashboard-risk');
    if (riskNode) {
      var critical = high > 0 || protection > 0;
      riskNode.textContent = critical ? 'ТРЕБУЕТ ВНИМАНИЯ' : warnings > 0 ? 'ПРЕДУПРЕЖДЕНИЕ' : 'ЗАЩИЩЕНО';
      riskNode.className = 'status-pill ' + (critical ? 'status-pill--error' : warnings > 0 ? 'status-pill--pending' : 'status-pill--ready');
    }

    var pump = positions.pump || {};
    var alerts = [];
    if (service.last_error) alerts.push('Сервис: ' + service.last_error);
    if (pump.last_error) alerts.push('Pump Live: ' + pump.last_error);
    if (pump.blocked_reason) alerts.push('Новые входы Pump заблокированы: ' + pump.blocked_reason);
    if (protection) alerts.push('Неполная видимость защиты у групп позиций: ' + protection);
    var alertNode = $('dashboard-alert');
    if (alertNode) {
      alertNode.hidden = alerts.length === 0;
      alertNode.textContent = alerts.join(' · ');
    }
  }

  function renderBalances(payload) {
    var accounts = payload.accounts || {};
    var rows = Array.isArray(accounts.balances) ? accounts.balances : [];
    var body = $('balances-body');
    if (!body) return;
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="8" class="muted">Нет свежих данных о балансах.</td></tr>';
      return;
    }
    body.innerHTML = rows.map(function (row) {
      var ratio = Number(row.margin_ratio);
      var ratioTone = isFinite(ratio) && ratio >= 0.8 ? 'value-negative' : isFinite(ratio) && ratio >= 0.6 ? 'value-warning' : '';
      return '<tr>' +
        '<td><strong>' + escapeHtml(String(row.exchange || '—').toUpperCase()) + '</strong></td>' +
        '<td>' + escapeHtml(row.account_label || row.account_alias || 'Main account') + '</td>' +
        '<td>' + money(row.total) + '</td>' +
        '<td>' + money(row.available) + '</td>' +
        '<td>' + money(row.used) + '</td>' +
        '<td class="' + ratioTone + '">' + (isFinite(ratio) ? percent(ratio * 100, 1) : '—') + '</td>' +
        '<td>' + statusPill(row.status || 'unknown') + (row.error ? '<span class="cell-note">' + escapeHtml(row.error) + '</span>' : '') + '</td>' +
        '<td>' + escapeHtml(date(row.updated_at || row.timestamp)) + '</td>' +
      '</tr>';
    }).join('');
    var unavailable = rows.filter(function (row) { return ['error', 'unavailable'].indexOf(String(row.status || '').toLowerCase()) >= 0; }).length;
    var health = $('balance-health');
    if (health) {
      health.textContent = unavailable ? ('недоступно: ' + unavailable) : ('аккаунтов: ' + rows.length);
      health.className = 'status-pill ' + (unavailable ? 'status-pill--error' : 'status-pill--ready');
    }
  }

  function mainLegs(card) {
    var legs = Array.isArray(card.legs) ? card.legs : [];
    if (!legs.length) return '—';
    return legs.map(function (leg) {
      var side = String(leg.side || '').toUpperCase();
      var qty = number(Math.abs(Number(leg.quantity || 0)), 6);
      return '<span class="dashboard-leg dashboard-leg--' + escapeHtml(String(leg.side || '').toLowerCase()) + '">' +
        escapeHtml(String(leg.exchange || '—').toUpperCase()) + ' ' + escapeHtml(side) +
        '<small>' + qty + ' · ' + money(leg.current_notional || leg.amount) + '</small></span>';
    }).join('');
  }

  function protectionText(card) {
    var legs = Array.isArray(card.legs) ? card.legs : [];
    if (!legs.length) return '—';
    var stops = legs.filter(function (leg) { return Number(leg.stop_price || 0) > 0; }).length;
    var takes = legs.filter(function (leg) { return Number(leg.take_price || 0) > 0; }).length;
    return stops + '/' + legs.length + ' стоп · ' + takes + '/' + legs.length + ' тейк';
  }

  function renderMainPositions(payload) {
    var main = ((payload.positions || {}).main || {});
    var rows = Array.isArray(main.positions) ? main.positions : [];
    var body = $('main-positions-body');
    setText('main-freshness', 'Аккаунт-снимок: ' + date(main.account_last_updated || main.last_updated));
    if (!body) return;
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted">На Main account нет открытых позиций.</td></tr>';
      return;
    }
    body.innerHTML = rows.map(function (card) {
      var summary = card.position_summary || {};
      var funding = card.funding || {};
      var pnl = Number(card.net_pnl || 0);
      return '<tr>' +
        '<td><strong>' + escapeHtml(card.symbol || '—') + '</strong><span class="cell-note">' + escapeHtml(card.pair_label || '—') + '</span></td>' +
        '<td><div class="dashboard-legs">' + mainLegs(card) + '</div></td>' +
        '<td><strong>' + money(summary.current_exposure_usdt || summary.amount_usdt) + '</strong><span class="cell-note">gross ' + money(summary.gross_current_exposure_usdt) + '</span></td>' +
        '<td class="' + pnlTone(pnl) + '"><strong>' + money(pnl) + '</strong></td>' +
        '<td>' + money(funding.expected_funding !== undefined ? funding.expected_funding : card.expected_funding) + '</td>' +
        '<td>' + escapeHtml(durationMinutes(funding.minutes_to_next_funding)) + '<span class="cell-note">' + escapeHtml(date(funding.next_funding)) + '</span></td>' +
        '<td>' + percent(card.liq_distance_pct) + '</td>' +
        '<td>' + escapeHtml(protectionText(card)) + '</td>' +
        '<td>' + statusPill(card.risk_level || 'unknown') + '</td>' +
      '</tr>';
    }).join('');
  }

  function nextLadder(position) {
    var legs = Array.isArray(position.legs) ? position.legs : [];
    var pending = legs.filter(function (leg) { return String(leg.status || '').toLowerCase() !== 'filled'; });
    if (!pending.length) return 'лестница заполнена';
    pending.sort(function (a, b) { return Number(a.step || 0) - Number(b.step || 0); });
    var leg = pending[0];
    return 'L' + (leg.step || '—') + ' · ' + price(leg.trigger_price) + ' · ' + String(leg.status || 'planned');
  }

  function renderPump(payload) {
    var pump = ((payload.positions || {}).pump || {});
    var rows = Array.isArray(pump.positions) ? pump.positions : [];
    var config = pump.config || {};
    var balance = pump.balance || {};
    var pool = pump.shared_pool || {};
    var body = $('pump-positions-body');
    setText('pump-status', pump.status || '—');
    setText('pump-available', money(balance.available_usd));
    setText('pump-reserve', money(config.reserve_usd));
    setText('pump-temporary', money(balance.temporary_occupied_usd));
    setText('pump-regime', String((pump.capital_regime || {}).mode || '—').toUpperCase());
    setText('pump-entry-headroom', pool.entry_headroom_usd === null || pool.entry_headroom_usd === undefined ? 'не рассчитан' : money(pool.entry_headroom_usd));
    setText('pump-freshness', 'Защитный цикл: ' + date(pump.last_cycle_at_ms) + ' · monitor ' + (pump.monitor_thread_alive ? 'работает' : 'не работает'));
    var arm = $('pump-arm');
    if (arm) {
      arm.textContent = pump.entry_armed ? 'ARMED' : 'DISARMED';
      arm.className = 'status-pill ' + (pump.entry_armed ? 'status-pill--ready' : 'status-pill--idle');
    }
    if (!body) return;
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="10" class="muted">Нет открытых Pump Live позиций.</td></tr>';
      return;
    }
    body.innerHTML = rows.map(function (row) {
      var pnl = Number(row.unrealized_pnl_usd || 0);
      var protectionOk = Number(row.tp_price || 0) > 0 && Number(row.stop_price || 0) > 0;
      return '<tr>' +
        '<td><strong>' + escapeHtml(row.symbol || '—') + '</strong><span class="cell-note">SHORT · ' + escapeHtml(row.status || '—') + '</span></td>' +
        '<td><strong>' + String(row.legs_filled || 0) + '/' + String((row.legs || []).length || 0) + '</strong><span class="cell-note">открытых ордеров ' + String(row.legs_open || 0) + '</span></td>' +
        '<td>' + price(row.avg_entry_price) + ' / ' + price(row.mark_price) + '</td>' +
        '<td class="' + pnlTone(pnl) + '"><strong>' + money(pnl) + '</strong></td>' +
        '<td>' + price(row.liq_price) + '<span class="cell-note">запас ' + percent(row.liq_buffer_pct) + '</span></td>' +
        '<td class="' + (protectionOk ? 'value-positive' : 'value-negative') + '">' + price(row.tp_price) + ' / ' + price(row.stop_price) + '</td>' +
        '<td>' + money(row.margin_topup_usd) + '<span class="cell-note">base ' + money(row.margin_prefund_floor_usd) + '</span></td>' +
        '<td>' + escapeHtml(nextLadder(row)) + '<span class="cell-note">gate ' + escapeHtml(row.ladder_gate_status || '—') + '</span></td>' +
        '<td>' + (row.remaining_hold_h === null || row.remaining_hold_h === undefined ? '—' : number(row.remaining_hold_h, 1) + ' ч') + '</td>' +
        '<td>' + statusPill(row.risk_level || 'unknown') + '</td>' +
      '</tr>';
    }).join('');
  }

  function renderGrid(payload) {
    var grid = payload.grid || {};
    var rules = Array.isArray(grid.rules) ? grid.rules : [];
    setText('grid-total', String(grid.total_rules || 0));
    setText('grid-enabled', String(grid.enabled_rules || 0));
    setText('grid-active', String(grid.active_rules || 0));
    var node = $('grid-rules');
    if (!node) return;
    if (!rules.length) {
      node.innerHTML = '<p class="muted">Grid-правил пока нет. Модуль доступен, но ничего не запускает сам.</p>';
      return;
    }
    node.innerHTML = rules.slice(0, 8).map(function (rule) {
      var status = rule.status || (rule.enabled ? 'enabled' : 'disabled');
      return '<div class="dashboard-list-row"><div><strong>' + escapeHtml(rule.symbol || rule.rule_id || 'Grid') + '</strong>' +
        '<small>' + escapeHtml(String(rule.long_exchange || '').toUpperCase()) + ' / ' + escapeHtml(String(rule.short_exchange || '').toUpperCase()) + '</small></div>' +
        statusPill(status) + '</div>';
    }).join('');
  }

  function renderRuntime(payload) {
    var modules = payload.runtime_modules || {};
    var labels = {
      account_monitor: 'Балансы и аккаунты',
      positions_market: 'Рынок открытых позиций',
      protective_orders: 'Стопы и тейки',
      manual_execution: 'Ручная торговля',
      auto_arb_grid: 'Grid-бот',
      pump_live: 'Pump Live',
      strategy_lab_observatory: 'Strategy Lab'
    };
    var order = Object.keys(labels);
    var node = $('runtime-modules');
    if (!node) return;
    node.innerHTML = order.map(function (key) {
      var enabled = Boolean(modules[key]);
      return '<div class="module-registry__row"><span>' + escapeHtml(labels[key]) + '</span>' +
        '<strong class="module-registry__state module-registry__state--' + (enabled ? 'on' : 'off') + '">' + (enabled ? 'РАБОТАЕТ' : 'ОТКЛЮЧЕН') + '</strong></div>';
    }).join('');
  }

  function fillSettings(payload) {
    settings = clone(payload.settings || settings || {});
    var protective = settings.protective || {};
    var exchanges = settings.analysis_exchanges || {};
    var exchangeNode = $('analysis-exchanges');
    if (exchangeNode) {
      exchangeNode.innerHTML = Object.keys(exchanges).sort().map(function (name) {
        return '<label class="exchange-check"><input type="checkbox" name="dashboard-analysis-exchange" value="' + escapeHtml(name) + '" ' + (exchanges[name] ? 'checked' : '') + '> ' + escapeHtml(name.toUpperCase()) + '</label>';
      }).join('');
    }
    setInput('setting-table-refresh', settings.table_refresh_seconds);
    setInput('setting-account-refresh', settings.account_refresh_seconds);
    setInput('setting-positions-refresh', settings.positions_market_refresh_seconds);
    setInput('setting-summary-refresh', settings.summary_refresh_seconds);
    setChecked('setting-auto-protect', protective.auto_protect_enabled);
    setChecked('setting-auto-take', protective.auto_take_enabled);
    setChecked('setting-margin-alerts', protective.send_margin_alerts);
    setChecked('setting-stop-alerts', protective.send_missing_stop_alerts);
    setChecked('setting-auto-margin', protective.auto_margin_enabled);
    setChecked('setting-isolated', protective.enforce_isolated_margin);
    setChecked('setting-leverage', protective.enforce_leverage);
    setChecked('setting-kucoin-topup', protective.kucoin_isolated_topup_only);
    setInput('setting-target-leverage', protective.target_leverage);
    setInput('setting-stop-gap', protective.stop_gap_from_liq_pct);
    setInput('setting-stop-requote', protective.stop_requote_threshold_pct);
    setInput('setting-fallback-take', protective.fallback_take_rr_pct);
    setInput('setting-notify-primary', protective.notification_primary_channel || 'ntfy');
    setInput('setting-notify-fallback', protective.notification_fallback_channel || 'telegram');
  }

  function setInput(id, value) {
    var node = $(id);
    if (node && value !== undefined && value !== null) node.value = value;
  }

  function setChecked(id, value) {
    var node = $(id);
    if (node) node.checked = Boolean(value);
  }

  function inputNumber(id, fallback) {
    var node = $(id);
    var parsed = node ? Number(node.value) : NaN;
    return isFinite(parsed) ? parsed : fallback;
  }

  function saveSettings(event) {
    event.preventDefault();
    var payload = clone(settings);
    payload.analysis_exchanges = clone(payload.analysis_exchanges || {});
    Array.prototype.forEach.call(document.querySelectorAll('input[name="dashboard-analysis-exchange"]'), function (node) {
      payload.analysis_exchanges[node.value] = node.checked;
    });
    payload.table_refresh_seconds = inputNumber('setting-table-refresh', payload.table_refresh_seconds);
    payload.account_refresh_seconds = inputNumber('setting-account-refresh', payload.account_refresh_seconds);
    payload.positions_market_refresh_seconds = inputNumber('setting-positions-refresh', payload.positions_market_refresh_seconds);
    payload.summary_refresh_seconds = inputNumber('setting-summary-refresh', payload.summary_refresh_seconds);
    payload.protective = clone(payload.protective || {});
    payload.protective.auto_protect_enabled = $('setting-auto-protect').checked;
    payload.protective.auto_take_enabled = $('setting-auto-take').checked;
    payload.protective.send_margin_alerts = $('setting-margin-alerts').checked;
    payload.protective.send_missing_stop_alerts = $('setting-stop-alerts').checked;
    payload.protective.auto_margin_enabled = $('setting-auto-margin').checked;
    payload.protective.enforce_isolated_margin = $('setting-isolated').checked;
    payload.protective.enforce_leverage = $('setting-leverage').checked;
    payload.protective.kucoin_isolated_topup_only = $('setting-kucoin-topup').checked;
    payload.protective.target_leverage = inputNumber('setting-target-leverage', payload.protective.target_leverage);
    payload.protective.stop_gap_from_liq_pct = inputNumber('setting-stop-gap', payload.protective.stop_gap_from_liq_pct);
    payload.protective.stop_requote_threshold_pct = inputNumber('setting-stop-requote', payload.protective.stop_requote_threshold_pct);
    payload.protective.fallback_take_rr_pct = inputNumber('setting-fallback-take', payload.protective.fallback_take_rr_pct);
    payload.protective.notification_primary_channel = $('setting-notify-primary').value;
    payload.protective.notification_fallback_channel = $('setting-notify-fallback').value;
    var status = $('settings-status');
    if (status) {
      status.textContent = 'Сохранение…';
      status.className = 'settings-status';
    }
    request('POST', '/api/settings', payload).then(function (response) {
      settings = clone(response.settings || payload);
      if (status) {
        status.textContent = 'Настройки сохранены';
        status.className = 'settings-status settings-status--success';
      }
      scheduleRefresh();
    }, function (error) {
      if (status) {
        status.textContent = error.message || 'Не удалось сохранить';
        status.className = 'settings-status settings-status--error';
      }
    });
  }

  function render(payload, refillSettings) {
    state = payload || {};
    renderSummary(state);
    renderBalances(state);
    renderMainPositions(state);
    renderPump(state);
    renderGrid(state);
    renderRuntime(state);
    if (refillSettings) fillSettings(state);
  }

  function refreshIntervalMs() {
    var configured = Number(((state.service || {}).refresh_intervals || {}).dashboard_sec || (settings || {}).table_refresh_seconds || 30);
    return Math.max(15000, Math.min(300000, configured * 1000));
  }

  function scheduleRefresh() {
    if (refreshTimer) window.clearTimeout(refreshTimer);
    var delay = refreshIntervalMs();
    nextRefreshAt = Date.now() + delay;
    refreshTimer = window.setTimeout(refresh, delay);
  }

  function refresh() {
    if (refreshInFlight) return;
    refreshInFlight = true;
    var button = $('dashboard-refresh');
    if (button) button.disabled = true;
    request('GET', '/api/dashboard').then(function (payload) {
      render(payload, false);
      refreshInFlight = false;
      if (button) button.disabled = false;
      scheduleRefresh();
    }, function (error) {
      var alert = $('dashboard-alert');
      if (alert) {
        alert.hidden = false;
        alert.textContent = 'Не удалось обновить главную страницу: ' + (error.message || 'network error');
      }
      refreshInFlight = false;
      if (button) button.disabled = false;
      scheduleRefresh();
    });
  }

  function updateCountdown() {
    var seconds = nextRefreshAt ? Math.max(0, Math.ceil((nextRefreshAt - Date.now()) / 1000)) : 0;
    setText('dashboard-countdown', seconds + ' сек.');
  }

  function init() {
    render(state, true);
    var refreshButton = $('dashboard-refresh');
    if (refreshButton) refreshButton.addEventListener('click', refresh);
    var form = $('dashboard-settings-form');
    if (form) form.addEventListener('submit', saveSettings);
    scheduleRefresh();
    countdownTimer = window.setInterval(updateCountdown, 1000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
}());
