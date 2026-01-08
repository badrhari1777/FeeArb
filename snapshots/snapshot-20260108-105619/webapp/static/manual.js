/**
 * Manual trading controls (dry-run + execute).
 * Uses plain XHR for broad browser compatibility.
 */
(function () {
  'use strict';

  var MAX_LOG_LINES = 200;

  function storageKey(prefix, field) {
    return 'manual_' + prefix + '_' + field;
  }

  function sharedKey(field) {
    return 'manual_shared_' + field;
  }

  function formFields(prefix) {
    var form = document.getElementById('manual-form');
    if (!form) {
      return [];
    }
    var nodes = form.querySelectorAll('input, select, textarea');
    var out = [];
    for (var i = 0; i < nodes.length; i += 1) {
      var el = nodes[i];
      if (!el.id || el.id.indexOf(prefix + '-') !== 0) {
        continue;
      }
      if (el.type === 'button' || el.type === 'submit' || el.type === 'reset') {
        continue;
      }
      out.push(el);
    }
    return out;
  }

  function readFieldValue(el) {
    if (el.type === 'checkbox') {
      return el.checked ? 'true' : 'false';
    }
    return el.value;
  }

  function writeFieldValue(el, value) {
    if (el.type === 'checkbox') {
      el.checked = value === 'true';
      return;
    }
    el.value = value;
  }

  function applyFormPrefs(prefix) {
    var fields = formFields(prefix);
    for (var i = 0; i < fields.length; i += 1) {
      var el = fields[i];
      var field = el.id.slice(prefix.length + 1);
      var stored = null;
      try {
        stored = window.localStorage.getItem(storageKey(prefix, field));
        if (stored === null) {
          stored = window.localStorage.getItem(sharedKey(field));
        }
      } catch (_err) {
        stored = null;
      }
      if (stored === null || stored === undefined) {
        continue;
      }
      writeFieldValue(el, stored);
    }
  }

  function saveFormPrefs(prefix) {
    var fields = formFields(prefix);
    for (var i = 0; i < fields.length; i += 1) {
      var el = fields[i];
      var field = el.id.slice(prefix.length + 1);
      var value = readFieldValue(el);
      try {
        window.localStorage.setItem(storageKey(prefix, field), value);
        window.localStorage.setItem(sharedKey(field), value);
      } catch (_err) {
        // ignore storage errors
      }
    }
  }

  function bindFormPersistence(prefix) {
    var form = document.getElementById('manual-form');
    if (!form) {
      return;
    }
    form.addEventListener('change', function () {
      saveFormPrefs(prefix);
    });
  }

  function applyLivePrefs(prefix) {
    try {
      var toggle = document.getElementById(prefix + '-live-orderbook');
      if (toggle) {
        var stored = window.localStorage.getItem(storageKey(prefix, 'live_orderbook'));
        if (stored !== null) {
          toggle.checked = stored === 'true';
        }
      }
      var depthEl = document.getElementById(prefix + '-live-depth');
      if (depthEl) {
        var storedDepth = window.localStorage.getItem(storageKey(prefix, 'live_depth'));
        if (storedDepth !== null && storedDepth !== '') {
          depthEl.value = storedDepth;
        }
      }
    } catch (_err) {
      // ignore storage errors
    }
  }

  function saveLivePrefs(prefix) {
    try {
      var toggle = document.getElementById(prefix + '-live-orderbook');
      if (toggle) {
        window.localStorage.setItem(storageKey(prefix, 'live_orderbook'), toggle.checked ? 'true' : 'false');
      }
      var depthEl = document.getElementById(prefix + '-live-depth');
      if (depthEl) {
        window.localStorage.setItem(storageKey(prefix, 'live_depth'), depthEl.value || '');
      }
    } catch (_err) {
      // ignore storage errors
    }
  }

  function readManualPrefs() {
    var toggle = document.getElementById('manual-live-orderbook');
    var depth = document.getElementById('manual-live-depth');
    return {
      live_orderbook: toggle ? !!toggle.checked : false,
      live_depth: depth && depth.value ? parseInt(depth.value, 10) : 5
    };
  }

  function persistManualSettings() {
    request('GET', '/api/settings', null, function (err, data) {
      if (err || !data || !data.settings) {
        return;
      }
      var payload = data.settings;
      payload.manual = readManualPrefs();
      request('POST', '/api/settings', payload, function () {});
    });
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

  function formatNumber(value, digits) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(number)) {
      return '-';
    }
    var places = typeof digits === 'number' ? digits : 4;
    return number.toFixed(places);
  }

  function formatPlan(plan) {
    if (!plan) {
      return 'No data.';
    }
    var lines = [];
    if (plan.errors && plan.errors.length) {
      lines.push('Errors:');
      plan.errors.forEach(function (err) {
        lines.push('  - ' + err);
      });
    }
    if (plan.warnings && plan.warnings.length) {
      lines.push('Warnings:');
      plan.warnings.forEach(function (warn) {
        lines.push('  - ' + warn);
      });
    }
    if (plan.suggested_expensive_leg) {
      var suggested = plan.suggested_expensive_leg.suggested_leg;
      var longLabel = plan.action === 'roll' ? 'to' : 'long';
      var shortLabel = plan.action === 'roll' ? 'from' : 'short';
      if (plan.action === 'roll') {
        suggested = suggested === 'long' ? 'to' : (suggested === 'short' ? 'from' : suggested);
      }
      lines.push('Suggested expensive leg: ' + suggested +
        ' (' + plan.suggested_expensive_leg.reason + ')');
      lines.push('Taker fees (bps): ' + longLabel + '=' +
        formatNumber(plan.suggested_expensive_leg.taker_fee_bps.long, 2) +
        ' ' + shortLabel + '=' + formatNumber(plan.suggested_expensive_leg.taker_fee_bps.short, 2));
      lines.push('Top3 liquidity (USD): ' + longLabel + '=' +
        formatNumber(plan.suggested_expensive_leg.top3_liquidity_usd.long, 2) +
        ' ' + shortLabel + '=' + formatNumber(plan.suggested_expensive_leg.top3_liquidity_usd.short, 2));
    }
    if (plan.spread_pct !== undefined && plan.spread_pct !== null) {
      lines.push('Spread (%): ' + formatNumber(plan.spread_pct, 4));
    }
    if (plan.spread_range && (plan.spread_range.min !== null || plan.spread_range.max !== null)) {
      lines.push('Spread range: ' +
        (plan.spread_range.min !== null && plan.spread_range.min !== undefined ? plan.spread_range.min : '-') +
        ' to ' +
        (plan.spread_range.max !== null && plan.spread_range.max !== undefined ? plan.spread_range.max : '-'));
      if (plan.spread_within_range !== undefined && plan.spread_within_range !== null) {
        lines.push('Spread within range: ' + plan.spread_within_range);
      }
    }
    if (plan.recommended_qty) {
      lines.push('Recommended qty (<= max slippage): ' + formatNumber(plan.recommended_qty, 6));
    }
    if (plan.recommended_notional) {
      lines.push('Recommended notional: ' + formatNumber(plan.recommended_notional, 2));
    }
    if (plan.min_chunk_qty) {
      lines.push('Min chunk qty (exchange): ' + formatNumber(plan.min_chunk_qty, 6));
    }
    if (plan.recommended_chunk_qty) {
      lines.push('Recommended chunk qty: ' + formatNumber(plan.recommended_chunk_qty, 6));
    }
    if (plan.suggested_mode) {
      lines.push('Suggested mode: ' + plan.suggested_mode);
    }
    if (plan.legs && plan.legs.length) {
      plan.legs.forEach(function (leg) {
        var exch = leg.exchange || '-';
        var stats = plan.stats && plan.stats[exch] ? plan.stats[exch] : {};
        var slip = plan.slippage && plan.slippage[exch] ? plan.slippage[exch] : {};
        var fund = plan.funding && plan.funding[exch] ? plan.funding[exch] : {};
        lines.push('');
        lines.push(exch.toUpperCase() + ' (' + leg.side + ')');
        lines.push('  best_bid=' + formatNumber(stats.best_bid, 4) +
          ' best_ask=' + formatNumber(stats.best_ask, 4));
        lines.push('  spread=' + formatNumber(stats.spread, 6) +
          ' mid=' + formatNumber(stats.mid, 4));
        lines.push('  top3_liquidity_usd=' + formatNumber(stats.min_liquidity_top3, 2));
        lines.push('  expected_slippage_bps=' + formatNumber(slip.expected_slippage_bps, 2) +
          ' filled=' + formatNumber(slip.filled_qty, 6) +
          ' remaining=' + formatNumber(slip.remaining_qty, 6));
        if (fund && fund.minutes_to_funding !== undefined) {
          lines.push('  funding_rate=' + formatNumber(fund.funding_rate, 6) +
            ' minutes_to_funding=' + formatNumber(fund.minutes_to_funding, 2));
        }
        if (plan.market_constraints && plan.market_constraints[exch]) {
          var constraints = plan.market_constraints[exch] || {};
          lines.push('  min_qty=' + formatNumber(constraints.min_qty, 6) +
            ' min_notional=' + formatNumber(constraints.min_notional, 2));
          lines.push('  amount_step=' + formatNumber(constraints.amount_step, 8) +
            ' price_step=' + formatNumber(constraints.price_step, 8) +
            ' contract_size=' + formatNumber(constraints.contract_size, 6));
          if (constraints.min_qty_required) {
            lines.push('  min_qty_required=' + formatNumber(constraints.min_qty_required, 6));
          }
        }
      });
    }
    if (plan.actions && plan.actions.length) {
      lines.push('');
      lines.push('Actions:');
      plan.actions.forEach(function (action) {
        var line = '  - ' + (action.exchange || '-') + ' ' + (action.status || '-') +
          ' filled=' + formatNumber(action.filled_qty, 6) +
          ' avg=' + formatNumber(action.avg_price, 4);
        if (action.error) {
          line += ' error=' + action.error;
        }
        lines.push(line);
      });
    }
    if (plan.remaining_qty !== undefined && plan.remaining_qty !== null) {
      lines.push('');
      lines.push('Remaining qty: ' + formatNumber(plan.remaining_qty, 6));
    }
    return lines.join('\n');
  }

  function formatConstraintsAll(constraints) {
    if (!constraints) {
      return '';
    }
    var exchanges = Object.keys(constraints || {}).sort();
    if (!exchanges.length) {
      return '';
    }
    var lines = [];
    exchanges.forEach(function (exchange) {
      var entry = constraints[exchange] || {};
      lines.push(exchange.toUpperCase());
      if (entry.error) {
        lines.push('  error=' + entry.error);
        lines.push('');
        return;
      }
      lines.push('  min_qty=' + formatNumber(entry.min_qty, 6) +
        ' min_notional=' + formatNumber(entry.min_notional, 2));
      if (entry.min_notional_override !== undefined && entry.min_notional_override !== null) {
        lines.push('  min_notional_override=' + formatNumber(entry.min_notional_override, 2));
      }
      if (entry.min_notional_effective !== undefined && entry.min_notional_effective !== null) {
        lines.push('  min_notional_effective=' + formatNumber(entry.min_notional_effective, 2));
      }
      if (entry.min_notional_buffer_pct !== undefined && entry.min_notional_buffer_pct !== null) {
        lines.push('  min_notional_buffer_pct=' + formatNumber(entry.min_notional_buffer_pct, 2));
      }
      lines.push('  amount_step=' + formatNumber(entry.amount_step, 8) +
        ' price_step=' + formatNumber(entry.price_step, 8));
      lines.push('  contract_size=' + formatNumber(entry.contract_size, 6) +
        ' min_qty_required=' + formatNumber(entry.min_qty_required, 6));
      if (entry.price_hint !== undefined && entry.price_hint !== null) {
        lines.push('  price_hint=' + formatNumber(entry.price_hint, 6));
      }
      lines.push('');
    });
    return lines.join('\n').trim();
  }

  function formatExecLogs(logs) {
    if (!logs || !logs.length) {
      return '';
    }
    var slice = logs;
    if (logs.length > MAX_LOG_LINES) {
      slice = logs.slice(logs.length - MAX_LOG_LINES);
    }
    return slice.map(function (entry) {
      var ts = entry.ts ? ('[' + entry.ts + '] ') : '';
      var event = entry.event ? (entry.event + ': ') : '';
      var message = entry.message || '';
      var data = entry.data && Object.keys(entry.data).length ? (' ' + JSON.stringify(entry.data)) : '';
      return ts + event + message + data;
    }).join('\n');
  }

  function setStatus(element, message, tone) {
    if (!element) {
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
    element.className = cls;
    element.textContent = message || '';
  }

  function toggleHedgeFields(prefix) {
    var typeEl = document.getElementById(prefix + '-hedge-type');
    if (!typeEl) {
      return;
    }
    var isLimit = typeEl.value === 'limit';
    var suffixes = [
      'hedge-limit-mode',
      'hedge-favorable-bps',
      'hedge-adverse-bps',
      'hedge-reprice-min',
      'hedge-bps',
      'hedge-ticks'
    ];
    suffixes.forEach(function (suffix) {
      var el = document.getElementById(prefix + '-' + suffix);
      if (!el) {
        return;
      }
      el.disabled = !isLimit;
    });
  }

  function currentAction() {
    var actionEl = document.getElementById('manual-action');
    return actionEl ? actionEl.value : 'enter';
  }

  function resolveStrategyMode(action, rawMode) {
    if (action === 'roll') {
      var rollMode = document.getElementById('manual-roll-mode');
      return rollMode ? rollMode.value : 'limit-first-expensive';
    }
    if (rawMode === 'smart') {
      return action === 'exit' ? 'smart-exit' : 'smart-enter';
    }
    if (rawMode === 'fast') {
      return action === 'exit' ? 'fast-exit' : 'fast-enter';
    }
    return rawMode || (action === 'exit' ? 'smart-exit' : 'smart-enter');
  }

  function toggleActionFields() {
    var action = currentAction();
    var showEnterExit = action !== 'roll';
    var showRoll = action === 'roll';
    toggleScope('manual-scope-enter-exit', showEnterExit);
    toggleScope('manual-scope-roll', showRoll);
  }

  function toggleScope(className, show) {
    var nodes = document.querySelectorAll('.' + className);
    for (var i = 0; i < nodes.length; i += 1) {
      var node = nodes[i];
      if (show) {
        node.classList.remove('hidden');
      } else {
        node.classList.add('hidden');
      }
    }
  }

  function readMinNotionalOverrides() {
    var overrides = {};
    var inputs = document.querySelectorAll('[data-min-notional-exchange]');
    for (var i = 0; i < inputs.length; i += 1) {
      var el = inputs[i];
      var exchange = el.getAttribute('data-min-notional-exchange');
      var value = parseFloat(el.value);
      if (exchange && !isNaN(value) && value > 0) {
        overrides[exchange] = value;
      }
    }
    return overrides;
  }

  function applyPlanDefaults(prefix, plan) {
    if (!plan || !plan.auto_limit_defaults) {
      return;
    }
    var defaults = plan.auto_limit_defaults || {};
    setIfEmpty(prefix + '-min-level-notional', defaults.min_level_notional);
    setIfEmpty(prefix + '-min-level-qty', defaults.min_level_qty);
    setIfEmpty(prefix + '-max-limit-dev', defaults.max_limit_deviation_bps);
    var chunkEl = document.getElementById(prefix + '-chunk-qty');
    if (chunkEl) {
      var currentChunk = parseFloat(chunkEl.value);
      var hasChunk = chunkEl.value !== '' && !isNaN(currentChunk);
      if (!hasChunk) {
        if (plan.recommended_chunk_qty) {
          chunkEl.value = plan.recommended_chunk_qty;
        } else if (plan.min_chunk_qty) {
          chunkEl.value = plan.min_chunk_qty;
        }
      } else if (plan.min_chunk_qty && currentChunk < plan.min_chunk_qty) {
        chunkEl.value = plan.min_chunk_qty;
      }
    }
  }

  function pollExecution(execId, prefix, planEl, logEl, statusEl, minimaEl) {
    function tick() {
      request('GET', '/api/manual/exec/' + execId, null, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        if (logEl) {
          logEl.textContent = formatExecLogs(data.logs || []);
        }
        if (data.result) {
          planEl.textContent = formatPlan(data.result);
          applyPlanDefaults(prefix, data.result);
          if (minimaEl) {
            var minimaText = formatConstraintsAll(data.result ? data.result.constraints_all : null);
            if (minimaText) {
              minimaEl.textContent = minimaText;
            }
          }
        }
        if (data.status === 'running') {
          setStatus(statusEl, 'Execution running...', 'info');
          setTimeout(tick, 1000);
          return;
        }
        if (data.status === 'completed_with_errors') {
          setStatus(statusEl, 'Completed with errors', 'error');
          return;
        }
        if (data.status === 'completed') {
          setStatus(statusEl, 'Execution complete', 'success');
          return;
        }
        if (data.status === 'failed') {
          setStatus(statusEl, 'Execution failed: ' + (data.error || 'unknown error'), 'error');
          return;
        }
        setStatus(statusEl, 'Execution finished', 'success');
      });
    }
    tick();
  }

  function setIfEmpty(id, value) {
    var el = document.getElementById(id);
    if (!el || value === null || value === undefined) {
      return;
    }
    if (String(el.value || '').trim() !== '') {
      return;
    }
    el.value = value;
  }

  function payloadCommon(prefix) {
    var get = function (id) { return document.getElementById(prefix + id); };
    function getValue(id) {
      var el = get(id);
      return el ? el.value : '';
    }
    function getChecked(id) {
      var el = get(id);
      return el ? !!el.checked : false;
    }
    function parseOptionalNumber(value) {
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
    return {
      symbol: (getValue('symbol') || '').trim().toUpperCase(),
      qty: parseOptionalNumber(getValue('qty')),
      mode: getValue('mode'),
      max_slippage_bps: parseOptionalNumber(getValue('slippage')) || 0,
      spread_min_pct: parseOptionalNumber(getValue('spread-min')),
      spread_max_pct: parseOptionalNumber(getValue('spread-max')),
      timeout_sec: parseOptionalNumber(getValue('timeout')),
      max_runtime_sec: parseOptionalNumber(getValue('runtime')),
      reprice_sec: parseOptionalNumber(getValue('reprice')),
      chunk_qty: parseOptionalNumber(getValue('chunk-qty')),
      chunk_notional: parseOptionalNumber(getValue('chunk-notional')),
      max_unhedged_sec: parseOptionalNumber(getValue('unhedged-sec')),
      max_unhedged_pct: parseOptionalNumber(getValue('unhedged-pct')),
      hedge_order_type: getValue('hedge-type') || null,
      hedge_offset_bps: parseOptionalNumber(getValue('hedge-bps')),
      hedge_offset_ticks: parseOptionalNumber(getValue('hedge-ticks')),
      hedge_limit_mode: getValue('hedge-limit-mode') || null,
      hedge_favorable_bps: parseOptionalNumber(getValue('hedge-favorable-bps')),
      hedge_adverse_bps: parseOptionalNumber(getValue('hedge-adverse-bps')),
      hedge_reprice_min_sec: parseOptionalNumber(getValue('hedge-reprice-min')),
      limit_offset_bps: parseOptionalNumber(getValue('limit-bps')),
      limit_offset_ticks: parseOptionalNumber(getValue('limit-ticks')),
      min_level_notional: parseOptionalNumber(getValue('min-level-notional')),
      min_level_qty: parseOptionalNumber(getValue('min-level-qty')),
      max_limit_deviation_bps: parseOptionalNumber(getValue('max-limit-dev')),
      use_orderbook_check: getChecked('orderbook-check'),
      fallback_to_market: getChecked('fallback'),
      margin_mode: getValue('margin-mode') || null,
      min_notional_buffer_pct: parseOptionalNumber(getValue('min-notional-buffer')),
      min_notional_overrides: readMinNotionalOverrides()
    };
  }

  function bindManual() {
    var planEl = document.getElementById('manual-plan');
    var statusEl = document.getElementById('manual-status');
    var logEl = document.getElementById('manual-exec-log');
    var minimaEl = document.getElementById('manual-minima');

    function buildPayload() {
      var payload = payloadCommon('manual-');
      var action = currentAction();
      payload.mode = resolveStrategyMode(action, payload.mode);
      if (action === 'roll') {
        payload.from_exchange = document.getElementById('manual-from-exchange').value;
        payload.to_exchange = document.getElementById('manual-to-exchange').value;
        payload.side = document.getElementById('manual-side').value;
        payload.expensive_leg = (document.getElementById('manual-expensive-leg-roll').value || null);
      } else {
        payload.long_exchange = document.getElementById('manual-long-exchange').value;
        payload.short_exchange = document.getElementById('manual-short-exchange').value;
        payload.expensive_leg = (document.getElementById('manual-expensive-leg').value || null);
      }
      return payload;
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

    function submit(dryRun) {
      var action = currentAction();
      var payload = buildPayload();
      payload.dry_run = dryRun;
      payload.async_run = !dryRun;
      setStatus(statusEl, dryRun ? 'Running dry-run...' : 'Submitting orders...', 'info');
      if (logEl && !dryRun) {
        logEl.textContent = '';
      }
      request('POST', actionEndpoint(action), payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        if (!dryRun && data && data.execution_id) {
          setStatus(statusEl, 'Execution started...', 'info');
          pollExecution(data.execution_id, 'manual', planEl, logEl, statusEl, minimaEl);
          return;
        }
        planEl.textContent = formatPlan(data);
        applyPlanDefaults('manual', data);
        if (minimaEl) {
          var minimaText = formatConstraintsAll(data ? data.constraints_all : null);
          minimaEl.textContent = minimaText || 'Order minimums will appear here after dry-run.';
        }
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else if (dryRun) {
          setStatus(statusEl, 'Dry-run complete', 'success');
        } else {
          setStatus(statusEl, 'Execution complete', 'success');
        }
      });
    }

    document.getElementById('manual-dry-run').addEventListener('click', function () { submit(true); });
    document.getElementById('manual-execute').addEventListener('click', function () { submit(false); });
    var hedgeType = document.getElementById('manual-hedge-type');
    if (hedgeType) {
      hedgeType.addEventListener('change', function () { toggleHedgeFields('manual'); });
      toggleHedgeFields('manual');
    }
    var actionEl = document.getElementById('manual-action');
    if (actionEl) {
      actionEl.addEventListener('change', toggleActionFields);
    }
    toggleActionFields();
  }

  function init() {
    applyFormPrefs('manual');
    bindManual();
    bindFormPersistence('manual');
    applyLivePrefs('manual');
    setupLiveFeed('manual', 'manual-long-exchange', 'manual-short-exchange', 'manual-live');
    toggleActionFields();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  function setupLiveFeed(prefix, longId, shortId, liveId) {
    var liveEl = document.getElementById(liveId);
    if (!liveEl) {
      return;
    }
    var bookEl = document.getElementById(prefix + '-live-book');
    var ws = null;
    var reconnectTimer = null;

    applyLivePrefs(prefix);

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/manual';
    }

    function setLive(text, tone) {
      liveEl.textContent = text;
      liveEl.className = 'live-metrics' + (tone ? (' live-metrics--' + tone) : '');
    }

    function currentPayload() {
      var action = currentAction();
      if (action === 'roll') {
        return { disabled: true };
      }
      var payload = payloadCommon(prefix + '-');
      payload.action = 'subscribe';
      payload.long_exchange = document.getElementById(longId).value;
      payload.short_exchange = document.getElementById(shortId).value;
      var liveToggle = document.getElementById(prefix + '-live-orderbook');
      if (liveToggle) {
        payload.include_orderbook = !!liveToggle.checked;
      }
      var depthEl = document.getElementById(prefix + '-live-depth');
      if (depthEl && depthEl.value) {
        payload.orderbook_depth = parseInt(depthEl.value, 10) || 5;
      }
      return payload;
    }

    function subscribe() {
      if (!ws || ws.readyState !== 1) {
        return;
      }
      var payload = currentPayload();
      if (!payload || payload.disabled) {
        setLive('Live spread: -', '');
        if (bookEl) {
          bookEl.textContent = '';
        }
        return;
      }
      if (!payload.symbol || !payload.long_exchange || !payload.short_exchange) {
        setLive('Live spread: -', '');
        if (bookEl) {
          bookEl.textContent = '';
        }
        return;
      }
      ws.send(JSON.stringify(payload));
      setLive('Live spread: connecting...', '');
      if (bookEl && !payload.include_orderbook) {
        bookEl.textContent = 'Live orderbook: off';
      }
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        subscribe();
      };
      ws.onmessage = function (evt) {
        if (currentAction() === 'roll') {
          return;
        }
        var data = null;
        try {
          data = JSON.parse(evt.data);
        } catch (_err) {
          return;
        }
        if (!data) {
          return;
        }
        if (data.type === 'status') {
          if (data.status === 'waiting') {
            var missing = data.missing || [];
            var label = missing.length ? ('waiting for ' + missing.join(', ')) : 'waiting for data';
            setLive('Live spread: ' + label, 'info');
            if (bookEl) {
              bookEl.textContent = 'Live orderbook: waiting for ' + (missing.length ? missing.join(', ') : 'data');
            }
          }
          return;
        }
        if (data.type === 'error') {
          setLive('Live spread: ' + (data.error || 'error'), 'error');
          return;
        }
        if (data.type !== 'spread') {
          return;
        }
        var spread = data.spread_pct;
        if (spread === null || spread === undefined) {
          setLive('Live spread: -', '');
          return;
        }
        var range = data.spread_range || {};
        var within = data.within_range;
        var text = 'Live spread: ' + formatNumber(spread, 4) + '%';
        if (range.min !== null && range.min !== undefined || range.max !== null && range.max !== undefined) {
          text += ' (range ' + (range.min !== null && range.min !== undefined ? range.min : '-') +
            ' to ' + (range.max !== null && range.max !== undefined ? range.max : '-') + ')';
        }
        if (within === true) {
          setLive(text, 'ok');
        } else if (within === false) {
          setLive(text, 'bad');
        } else {
          setLive(text, '');
        }
        if (bookEl) {
          var longBook = data.long || {};
          var shortBook = data.short || {};
          if (longBook.bids && longBook.asks && shortBook.bids && shortBook.asks) {
            bookEl.textContent = formatLiveBooks(
              longBook,
              shortBook,
              data.long_exchange,
              data.short_exchange,
              data.subscriptions || {}
            );
          } else {
            bookEl.textContent = 'Live orderbook: off';
          }
        }
      };
      ws.onclose = function () {
        if (reconnectTimer) {
          return;
        }
        reconnectTimer = window.setTimeout(function () {
          reconnectTimer = null;
          connect();
        }, 1000);
      };
    }

    connect();

    var form = document.getElementById('manual-form');
    if (form) {
      form.addEventListener('change', function (evt) {
        var target = evt && evt.target ? evt.target : null;
        var isLiveField = target && target.id && (
          target.id === (prefix + '-live-orderbook') ||
          target.id === (prefix + '-live-depth')
        );
        if (isLiveField) {
          saveLivePrefs(prefix);
          persistManualSettings();
        }
        subscribe();
      });
    }

    function formatLiveBooks(longBook, shortBook, longExchange, shortExchange, subscriptions) {
      var lines = [];
      lines.push('LONG (' + (longExchange || '-').toUpperCase() + ')');
      if (subscriptions && subscriptions[longExchange]) {
        lines.push('  Sub: ' + subscriptions[longExchange]);
      }
      lines.push('  Asks (top -> mid):');
      lines = lines.concat(formatLevels(longBook.asks, true));
      lines.push('  Mid: ' + formatNumber(longBook.best_bid, 4) + ' / ' + formatNumber(longBook.best_ask, 4));
      lines.push('  Bids (mid -> down):');
      lines = lines.concat(formatLevels(longBook.bids, false));
      lines.push('');
      lines.push('SHORT (' + (shortExchange || '-').toUpperCase() + ')');
      if (subscriptions && subscriptions[shortExchange]) {
        lines.push('  Sub: ' + subscriptions[shortExchange]);
      }
      lines.push('  Asks (top -> mid):');
      lines = lines.concat(formatLevels(shortBook.asks, true));
      lines.push('  Mid: ' + formatNumber(shortBook.best_bid, 4) + ' / ' + formatNumber(shortBook.best_ask, 4));
      lines.push('  Bids (mid -> down):');
      lines = lines.concat(formatLevels(shortBook.bids, false));
      return lines.join('\n');
    }

    function formatLevels(levels, reverse) {
      if (!levels || !levels.length) {
        return ['    -'];
      }
      var ordered = levels.slice();
      if (reverse) {
        ordered.reverse();
      }
      return ordered.map(function (level) {
        return '    ' + formatNumber(level[0], 4) + ' x ' + formatNumber(level[1], 4);
      });
    }
  }
})();
