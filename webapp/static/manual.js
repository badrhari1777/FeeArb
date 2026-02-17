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
          var responseText = (xhr.responseText || '').trim();
          if (responseText.length > 200) {
            responseText = responseText.slice(0, 200) + '...';
          }
          var detail = method + ' ' + url + ' (' + xhr.status + ')';
          if (responseText) {
            detail += ' ' + responseText;
          }
          error = new Error('Request failed ' + detail);
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

  function copyTextToClipboard(text, callback) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () {
        callback(true);
      }).catch(function () {
        fallbackCopy(text, callback);
      });
      return;
    }
    fallbackCopy(text, callback);
  }

  function fallbackCopy(text, callback) {
    var textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.setAttribute('readonly', '');
    textarea.style.position = 'fixed';
    textarea.style.top = '0';
    textarea.style.left = '0';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.select();
    var ok = false;
    try {
      ok = document.execCommand('copy');
    } catch (_err) {
      ok = false;
    }
    document.body.removeChild(textarea);
    callback(ok);
  }

  function flashCopyButton(button, success) {
    var original = button.getAttribute('data-copy-label') || button.textContent;
    button.setAttribute('data-copy-label', original);
    button.textContent = success ? 'Copied' : 'Copy failed';
    button.disabled = true;
    setTimeout(function () {
      button.textContent = original;
      button.disabled = false;
    }, 1200);
  }

  function bindCopyButtons() {
    var buttons = document.querySelectorAll('[data-copy-target]');
    for (var i = 0; i < buttons.length; i += 1) {
      bindCopyButton(buttons[i]);
    }
  }

  function bindCopyButton(button) {
    if (!button) {
      return;
    }
    button.addEventListener('click', function () {
      var targetId = button.getAttribute('data-copy-target');
      var target = targetId ? document.getElementById(targetId) : null;
      var text = target ? target.textContent : '';
      if (!text) {
        flashCopyButton(button, false);
        return;
      }
      copyTextToClipboard(text, function (ok) {
        flashCopyButton(button, ok);
      });
    });
  }

  function normalizeInputSymbol(value) {
    var text = (value || '').trim().toUpperCase();
    if (text.indexOf(':') >= 0) {
      text = text.split(':')[0];
    }
    if (text.indexOf('/') >= 0) {
      text = text.replace('/', '');
    }
    text = text.replace(/[-_]/g, '');
    if (text.endsWith('USDTM')) {
      text = text.slice(0, -1);
    }
    if (text.endsWith('UMCBL') || text.endsWith('DMCBL')) {
      text = text.slice(0, -5);
    }
    if (text.endsWith('SWAP')) {
      text = text.slice(0, -4);
    }
    if (text.endsWith('PERP')) {
      text = text.slice(0, -4);
    }
    if (text.endsWith('USDT') || text.endsWith('USD')) {
      return text;
    }
    return text ? text + 'USDT' : text;
  }

  function normalizeGateSymbol(value) {
    var symbol = normalizeInputSymbol(value);
    if (symbol.endsWith('USDT')) {
      return symbol.slice(0, -4) + '_USDT';
    }
    return symbol;
  }

  function normalizeKucoinSymbol(value) {
    var symbol = normalizeInputSymbol(value);
    if (symbol.endsWith('USDT')) {
      var base = symbol.slice(0, -4);
      if (base === 'BTC') {
        base = 'XBT';
      }
      return base + 'USDTM';
    }
    return symbol;
  }

  function normalizeBitgetSymbol(value) {
    return normalizeInputSymbol(value);
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
        var constraints = plan.market_constraints && plan.market_constraints[exch] ? plan.market_constraints[exch] : {};
        lines.push('');
        lines.push(exch.toUpperCase() + ' (' + leg.side + ')');
        lines.push('  best_bid=' + formatNumber(stats.best_bid, 4) +
          ' best_ask=' + formatNumber(stats.best_ask, 4));
        lines.push('  spread=' + formatNumber(stats.spread, 6) +
          ' mid=' + formatNumber(stats.mid, 4));
        if (constraints.price_step) {
          lines.push('  price_step=' + formatNumber(constraints.price_step, 8));
          if (stats.mid) {
            var tickBps = (constraints.price_step / stats.mid) * 10000.0;
            lines.push('  tick_bps~=' + formatNumber(tickBps, 2));
          }
        }
        lines.push('  top3_liquidity_usd=' + formatNumber(stats.min_liquidity_top3, 2));
        lines.push('  expected_slippage_bps=' + formatNumber(slip.expected_slippage_bps, 2) +
          ' filled=' + formatNumber(slip.filled_qty, 6) +
          ' remaining=' + formatNumber(slip.remaining_qty, 6));
        if (fund && fund.minutes_to_funding !== undefined) {
          lines.push('  funding_rate=' + formatNumber(fund.funding_rate, 6) +
            ' minutes_to_funding=' + formatNumber(fund.minutes_to_funding, 2));
        }
      });
    }
    if (plan.actions && plan.actions.length) {
      lines.push('');
      lines.push('Actions:');
      plan.actions.forEach(function (action) {
        var ts = action.ts || action.timestamp || action.time;
        var line = '  - ' + (ts ? ('[' + ts + '] ') : '') +
          (action.exchange || '-') + ' ' + (action.status || '-') +
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
      var message = entry.message || '';
      if (entry.event === 'payload') {
        var header = ts + 'payload: ' + message;
        var payloadText = '';
        if (entry.data && Object.keys(entry.data).length) {
          try {
            payloadText = JSON.stringify(entry.data, null, 2);
          } catch (err) {
            payloadText = String(entry.data);
          }
        }
        return payloadText ? (header + '\n' + payloadText) : header;
      }
      var data = entry.data && Object.keys(entry.data).length ? (' ' + JSON.stringify(entry.data)) : '';
      if (entry.event === 'story') {
        return ts + message;
      }
      var event = entry.event ? (entry.event + ': ') : '';
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
      var resolved = rollMode ? rollMode.value : '';
      return resolved || 'smart-roll';
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
    var modeEl = document.getElementById('manual-mode');
    var mode = modeEl ? modeEl.value : '';
    var showMarket = showEnterExit && mode === 'fast';
    var showExit = action === 'exit';
    toggleScope('manual-scope-enter-exit', showEnterExit);
    toggleScope('manual-scope-roll', showRoll);
    toggleScope('manual-scope-market', showMarket);
    toggleScope('manual-scope-exit', showExit);
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

  function applyPlanDefaults(prefix, plan) {
    if (!plan || !plan.auto_limit_defaults) {
      return;
    }
    var defaults = plan.auto_limit_defaults || {};
    setIfEmpty(prefix + '-max-limit-dev', defaults.max_limit_deviation_bps);
    var chunkEl = document.getElementById(prefix + '-chunk-qty');
    if (chunkEl) {
      var forceEl = document.getElementById(prefix + '-force-chunk');
      var forceChunk = forceEl ? !!forceEl.checked : false;
      var planChunk = null;
      if (plan.recommended_chunk_qty) {
        planChunk = plan.recommended_chunk_qty;
      } else if (plan.min_chunk_qty) {
        planChunk = plan.min_chunk_qty;
      }
      if (!forceChunk && planChunk !== null && planChunk !== undefined) {
        chunkEl.value = planChunk;
      }
    }
  }

  function pollExecution(execId, prefix, planEl, logEl, statusEl, stopBtn, execIdEl) {
    function tick() {
      request('GET', '/api/manual/exec/' + execId, null, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        if (execIdEl) {
          var execLabel = 'Execution id: ' + execId;
          if (data && data.status) {
            execLabel += ' (' + data.status + ')';
          }
          execIdEl.textContent = execLabel;
        }
        if (logEl) {
          logEl.textContent = formatExecLogs(data.logs || []);
        }
        if (data.stop_requested) {
          setStatus(statusEl, 'Stop requested; waiting for current orders...', 'info');
        }
        if (data.result) {
          planEl.textContent = formatPlan(data.result);
          applyPlanDefaults(prefix, data.result);
        }
        if (data.status === 'running') {
          setStatus(statusEl, 'Execution running...', 'info');
          setTimeout(tick, 1000);
          return;
        }
        if (stopBtn) {
          stopBtn.disabled = true;
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
    function parseOptionalInt(value) {
      if (value === null || value === undefined) {
        return null;
      }
      var text = String(value).trim();
      if (!text) {
        return null;
      }
      var parsed = parseInt(text, 10);
      return isNaN(parsed) ? null : parsed;
    }
    function readWsHealth(exchange) {
      var interval = parseOptionalNumber(getValue('ws-' + exchange + '-heartbeat-interval'));
      var timeout = parseOptionalNumber(getValue('ws-' + exchange + '-heartbeat-timeout'));
      var attempts = parseOptionalInt(getValue('ws-' + exchange + '-reconnect-attempts'));
      var grace = parseOptionalNumber(getValue('ws-' + exchange + '-reconnect-grace'));
      var cfg = {};
      if (interval !== null) {
        cfg.heartbeat_interval = interval;
      }
      if (timeout !== null) {
        cfg.heartbeat_timeout = timeout;
      }
      if (attempts !== null) {
        cfg.reconnect_attempts = attempts;
      }
      if (grace !== null) {
        cfg.reconnect_grace_sec = grace;
      }
      return Object.keys(cfg).length ? cfg : null;
    }
    var wsHealth = {};
    var exchanges = ['bybit', 'binance', 'okx', 'gate', 'bitget', 'kucoin', 'bingx'];
    for (var i = 0; i < exchanges.length; i += 1) {
      var exchange = exchanges[i];
      var cfg = readWsHealth(exchange);
      if (cfg) {
        wsHealth[exchange] = cfg;
      }
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
      force_chunk_qty: getChecked('force-chunk'),
      hedge_order_type: getValue('hedge-type') || null,
      hedge_offset_bps: parseOptionalNumber(getValue('hedge-bps')),
      hedge_offset_ticks: parseOptionalNumber(getValue('hedge-ticks')),
      hedge_limit_mode: getValue('hedge-limit-mode') || null,
      hedge_favorable_bps: parseOptionalNumber(getValue('hedge-favorable-bps')),
      hedge_adverse_bps: parseOptionalNumber(getValue('hedge-adverse-bps')),
      hedge_reprice_min_sec: parseOptionalNumber(getValue('hedge-reprice-min')),
      limit_offset_bps: parseOptionalNumber(getValue('limit-bps')),
      limit_offset_ticks: parseOptionalNumber(getValue('limit-ticks')),
      max_limit_deviation_bps: parseOptionalNumber(getValue('max-limit-dev')),
      market_refill_bps: parseOptionalNumber(getValue('market-refill-bps')),
      market_refill_buffer: parseOptionalNumber(getValue('market-refill-buffer')),
      market_refill_max_wait_sec: parseOptionalNumber(getValue('market-refill-max-wait')),
      use_orderbook_check: getChecked('orderbook-check'),
      fallback_to_market: getChecked('fallback'),
      margin_mode: getValue('margin-mode') || null,
      ws_orders_health: Object.keys(wsHealth).length ? wsHealth : null
    };
  }

  function bindManual() {
    var planEl = document.getElementById('manual-plan');
    var statusEl = document.getElementById('manual-status');
    var logEl = document.getElementById('manual-exec-log');
    var stopBtn = document.getElementById('manual-stop');
    var execIdEl = document.getElementById('manual-exec-id');
    var currentExecId = null;

    function setExecId(execId, status) {
      if (!execIdEl) {
        return;
      }
      if (!execId) {
        execIdEl.textContent = 'Execution id: -';
        return;
      }
      var label = 'Execution id: ' + execId;
      if (status) {
        label += ' (' + status + ')';
      }
      execIdEl.textContent = label;
    }

    function setStopEnabled(enabled) {
      if (!stopBtn) {
        return;
      }
      stopBtn.disabled = !enabled;
    }

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
        if (action === 'exit') {
          var allowFlipEl = document.getElementById('manual-exit-allow-flip');
          payload.exit_allow_flip = allowFlipEl ? !!allowFlipEl.checked : false;
        }
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
      currentExecId = null;
      setStopEnabled(false);
      setExecId(null);
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
          currentExecId = data.execution_id;
          setStopEnabled(true);
          setExecId(currentExecId, 'running');
          setStatus(statusEl, 'Execution started...', 'info');
          pollExecution(data.execution_id, 'manual', planEl, logEl, statusEl, stopBtn, execIdEl);
          return;
        }
        planEl.textContent = formatPlan(data);
        applyPlanDefaults('manual', data);
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
    if (stopBtn) {
      stopBtn.addEventListener('click', function () {
        if (!currentExecId) {
          return;
        }
        setStatus(statusEl, 'Stop requested...', 'info');
        request('POST', '/api/manual/exec/' + currentExecId + '/stop', {}, function () {});
      });
    }
    function loadActiveRuns() {
      request('GET', '/api/manual/exec', null, function (err, data) {
        if (err || !data || !data.runs || currentExecId) {
          if (!currentExecId) {
            setExecId(null);
          }
          return;
        }
        var runs = data.runs || [];
        var running = null;
        for (var i = 0; i < runs.length; i++) {
          if (runs[i].status === 'running') {
            running = runs[i];
            break;
          }
        }
        if (!running) {
          setExecId(null);
          return;
        }
        currentExecId = running.execution_id;
        setExecId(currentExecId, running.status);
        setStopEnabled(true);
        setStatus(statusEl, 'Execution running...', 'info');
        pollExecution(currentExecId, 'manual', planEl, logEl, statusEl, stopBtn, execIdEl);
      });
    }

    loadActiveRuns();

    var hedgeType = document.getElementById('manual-hedge-type');
    if (hedgeType) {
      hedgeType.addEventListener('change', function () { toggleHedgeFields('manual'); });
      toggleHedgeFields('manual');
    }
    var actionEl = document.getElementById('manual-action');
    if (actionEl) {
      actionEl.addEventListener('change', toggleActionFields);
    }
    var modeEl = document.getElementById('manual-mode');
    if (modeEl) {
      modeEl.addEventListener('change', toggleActionFields);
    }
    toggleActionFields();
  }

  function init() {
    applyFormPrefs('manual');
    bindManual();
    bindFormPersistence('manual');
    applyLivePrefs('manual');
    bindCopyButtons();
    setupLiveFeed('manual', 'manual-long-exchange', 'manual-short-exchange', 'manual-live');
    setupWsOrderRawLogs();
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
      var payload = payloadCommon(prefix + '-');
      payload.action = 'subscribe';
      if (action === 'roll') {
        payload.long_exchange = document.getElementById(prefix + '-to-exchange').value;
        payload.short_exchange = document.getElementById(prefix + '-from-exchange').value;
      } else {
        payload.long_exchange = document.getElementById(longId).value;
        payload.short_exchange = document.getElementById(shortId).value;
      }
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

  function setupWsOrderRawLogs() {
    var longLogEl = document.getElementById('manual-ws-long-log');
    var shortLogEl = document.getElementById('manual-ws-short-log');
    var longStatusEl = document.getElementById('manual-ws-long-status');
    var shortStatusEl = document.getElementById('manual-ws-short-status');
    var symbolEl = document.getElementById('manual-symbol');
    if (!longLogEl || !shortLogEl) {
      return;
    }

    var maxLines = 500;
    var longLogger = createRawLogger(longLogEl, maxLines);
    var shortLogger = createRawLogger(shortLogEl, maxLines);

    var longClient = createWsOrderRawClient(longLogger, longStatusEl);
    var shortClient = createWsOrderRawClient(shortLogger, shortStatusEl);

    function resolveExchange(side) {
      var action = currentAction();
      if (action === 'roll') {
        if (side === 'long') {
          return getSelectValue('manual-to-exchange');
        }
        return getSelectValue('manual-from-exchange');
      }
      if (side === 'long') {
        return getSelectValue('manual-long-exchange');
      }
      return getSelectValue('manual-short-exchange');
    }

    function updateClients() {
      var symbol = symbolEl ? symbolEl.value : '';
      longClient.connect(resolveExchange('long'), symbol);
      shortClient.connect(resolveExchange('short'), symbol);
    }

    var form = document.getElementById('manual-form');
    if (form) {
      form.addEventListener('change', updateClients);
    }
    if (symbolEl) {
      symbolEl.addEventListener('input', updateClients);
    }
    updateClients();

    function getSelectValue(id) {
      var el = document.getElementById(id);
      return el ? String(el.value || '').toLowerCase() : '';
    }
  }

  function createRawLogger(el, maxLines) {
    var buffer = [];
    function append(line) {
      if (!el) {
        return;
      }
      buffer.push(line);
      if (buffer.length > maxLines) {
        buffer = buffer.slice(buffer.length - maxLines);
      }
      el.textContent = buffer.join('\n');
      el.scrollTop = el.scrollHeight;
    }
    append.clear = function () {
      buffer = [];
      if (el) {
        el.textContent = '';
      }
    };
    return append;
  }

  function createWsOrderRawClient(logLine, statusEl) {
    var ws = null;
    var currentExchange = null;
    var currentSymbol = '';
    var subscribed = false;

    function connect(exchange, symbol) {
      var nextExchange = String(exchange || '').toLowerCase();
      var nextSymbol = String(symbol || '');
      if (nextExchange === currentExchange && nextSymbol === currentSymbol && ws && ws.readyState === 1) {
        return;
      }
      disconnect();
      currentExchange = nextExchange;
      currentSymbol = nextSymbol;
      if (logLine && logLine.clear) {
        logLine.clear();
      }
      if (!currentExchange) {
        setStatus(statusEl, 'Select exchange', 'info');
        return;
      }
      var config = wsOrderConfig(currentExchange);
      if (!config) {
        setStatus(statusEl, 'WS raw not supported', 'error');
        return;
      }
      subscribed = false;
      ws = new WebSocket(wsUrl(config.endpoint));
      setStatus(statusEl, 'Connecting...', 'info');
      ws.onopen = function () {
        send({ action: 'connect' });
      };
      ws.onmessage = function (evt) {
        var text = String(evt.data || '');
        logLine(text);
        if (!subscribed && shouldSubscribe(currentExchange, text)) {
          var sent = sendSubscriptions(currentExchange, currentSymbol);
          subscribed = true;
          if (sent) {
            setStatus(statusEl, 'Subscribed', 'success');
          } else {
            setStatus(statusEl, 'Connected', 'success');
          }
        }
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'error');
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WS error', 'error');
      };
    }

    function disconnect() {
      if (ws) {
        try {
          send({ action: 'disconnect' });
        } catch (_err) {
          // ignore
        }
        try {
          ws.close();
        } catch (_err2) {
          // ignore
        }
        ws = null;
      }
    }

    function send(message) {
      if (!ws || ws.readyState !== 1) {
        return;
      }
      ws.send(JSON.stringify(message));
    }

    function sendSubscriptions(exchange, symbol) {
      var payloads = buildOrderSubscriptions(exchange, symbol);
      if (!payloads.length) {
        if (requiresSymbol(exchange) && !normalizeInputSymbol(symbol)) {
          setStatus(statusEl, 'Enter symbol', 'info');
        }
        return false;
      }
      payloads.forEach(function (payload) {
        send({ action: 'send', payload: payload });
      });
      return true;
    }

    function wsUrl(path) {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + path;
    }

    function shouldSubscribe(exchange, line) {
      var config = wsOrderConfig(exchange);
      if (!config) {
        return false;
      }
      if (!config.loginRequired) {
        return line.indexOf('[sys] connected') !== -1;
      }
      var payload = parseJsonFromLine(line);
      if (!payload) {
        return false;
      }
      if (exchange === 'bybit') {
        return payload.op === 'auth' && (payload.success === true || payload.retCode === 0);
      }
      if (exchange === 'okx') {
        return payload.event === 'login' && String(payload.code) === '0';
      }
      if (exchange === 'bitget') {
        return payload.event === 'login' && String(payload.code) === '0';
      }
      if (exchange === 'gate') {
        return payload.header &&
          payload.header.channel === 'futures.login' &&
          String(payload.header.status) === '200';
      }
      return false;
    }

    function parseJsonFromLine(line) {
      var idx = line.indexOf('{');
      if (idx < 0) {
        return null;
      }
      var text = line.slice(idx);
      try {
        return JSON.parse(text);
      } catch (_err) {
        return null;
      }
    }

    return { connect: connect, disconnect: disconnect };
  }

  function wsOrderConfig(exchange) {
    var configs = {
      bybit: { endpoint: '/ws/trade-private-raw', loginRequired: true },
      binance: { endpoint: '/ws/trade-binance-raw', loginRequired: false },
      okx: { endpoint: '/ws/trade-okx-raw', loginRequired: true },
      gate: { endpoint: '/ws/trade-gate-raw', loginRequired: true },
      bitget: { endpoint: '/ws/trade-bitget-raw', loginRequired: true },
      kucoin: { endpoint: '/ws/trade-kucoin-raw', loginRequired: false },
      bingx: { endpoint: '/ws/trade-bingx-raw', loginRequired: false }
    };
    return configs[exchange] || null;
  }

  function requiresSymbol(exchange) {
    return exchange === 'gate' || exchange === 'kucoin';
  }

  function buildOrderSubscriptions(exchange, rawSymbol) {
    if (exchange === 'bybit') {
      return [{ op: 'subscribe', args: ['order', 'execution'] }];
    }
    if (exchange === 'binance') {
      return [];
    }
    if (exchange === 'okx') {
      return [{ op: 'subscribe', args: [{ channel: 'orders', instType: 'SWAP' }] }];
    }
    if (exchange === 'gate') {
      var gateSymbol = normalizeGateSymbol(rawSymbol);
      if (!gateSymbol) {
        return [];
      }
      return [
        { channel: 'futures.orders', event: 'subscribe', payload: [gateSymbol] },
        { channel: 'futures.usertrades', event: 'subscribe', payload: [gateSymbol] }
      ];
    }
    if (exchange === 'bitget') {
      var bitgetSymbol = normalizeBitgetSymbol(rawSymbol);
      return [
        {
          op: 'subscribe',
          args: [{ instType: 'USDT-FUTURES', channel: 'orders', instId: bitgetSymbol || 'default' }]
        }
      ];
    }
    if (exchange === 'kucoin') {
      var kucoinSymbol = normalizeKucoinSymbol(rawSymbol);
      var topic = kucoinSymbol ? '/contractMarket/tradeOrders:' + kucoinSymbol : '/contractMarket/tradeOrders';
      return [
        {
          id: String(Date.now()),
          type: 'subscribe',
          topic: topic,
          privateChannel: true,
          response: true
        }
      ];
    }
    return [];
  }
})();
