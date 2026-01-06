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
    var enterToggle = document.getElementById('enter-live-orderbook');
    var exitToggle = document.getElementById('exit-live-orderbook');
    var enterDepth = document.getElementById('enter-live-depth');
    var exitDepth = document.getElementById('exit-live-depth');
    return {
      enter_live_orderbook: enterToggle ? !!enterToggle.checked : false,
      enter_live_depth: enterDepth && enterDepth.value ? parseInt(enterDepth.value, 10) : 5,
      exit_live_orderbook: exitToggle ? !!exitToggle.checked : false,
      exit_live_depth: exitDepth && exitDepth.value ? parseInt(exitDepth.value, 10) : 5
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

  function applyPlanDefaults(prefix, plan) {
    if (!plan || !plan.auto_limit_defaults) {
      return;
    }
    var defaults = plan.auto_limit_defaults || {};
    setIfEmpty(prefix + '-min-level-notional', defaults.min_level_notional);
    setIfEmpty(prefix + '-min-level-qty', defaults.min_level_qty);
    setIfEmpty(prefix + '-max-limit-dev', defaults.max_limit_deviation_bps);
    if (plan.recommended_chunk_qty) {
      setIfEmpty(prefix + '-chunk-qty', plan.recommended_chunk_qty);
    }
  }

  function pollExecution(execId, prefix, planEl, logEl, statusEl) {
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
      limit_offset_bps: parseOptionalNumber(getValue('limit-bps')),
      limit_offset_ticks: parseOptionalNumber(getValue('limit-ticks')),
      auto_limit_price: getChecked('auto-limit'),
      min_level_notional: parseOptionalNumber(getValue('min-level-notional')),
      min_level_qty: parseOptionalNumber(getValue('min-level-qty')),
      max_limit_deviation_bps: parseOptionalNumber(getValue('max-limit-dev')),
      use_orderbook_check: getChecked('orderbook-check'),
      fallback_to_market: getChecked('fallback'),
      expensive_leg: getValue('expensive-leg') || null
    };
  }

  function bindEnter() {
    var planEl = document.getElementById('enter-plan');
    var statusEl = document.getElementById('enter-status');
    var logEl = document.getElementById('enter-exec-log');
    function buildPayload() {
      var payload = payloadCommon('enter-');
      payload.long_exchange = document.getElementById('enter-long-exchange').value;
      payload.short_exchange = document.getElementById('enter-short-exchange').value;
      payload.limit_price_long = parseFloat(document.getElementById('enter-limit-long').value || '0') || null;
      payload.limit_price_short = parseFloat(document.getElementById('enter-limit-short').value || '0') || null;
      return payload;
    }
    function submit(dryRun) {
      var payload = buildPayload();
      payload.dry_run = dryRun;
      payload.async_run = !dryRun;
      setStatus(statusEl, dryRun ? 'Running dry-run...' : 'Submitting orders...', 'info');
      if (logEl && !dryRun) {
        logEl.textContent = '';
      }
      request('POST', '/api/manual/enter', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        if (!dryRun && data && data.execution_id) {
          setStatus(statusEl, 'Execution started...', 'info');
          pollExecution(data.execution_id, 'enter', planEl, logEl, statusEl);
          return;
        }
        planEl.textContent = formatPlan(data);
        applyPlanDefaults('enter', data);
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else if (dryRun) {
          setStatus(statusEl, 'Dry-run complete', 'success');
        } else {
          setStatus(statusEl, 'Execution complete', 'success');
        }
      });
    }
    function analyze() {
      var payload = buildPayload();
      payload.action = 'enter';
      setStatus(statusEl, 'Analyzing...', 'info');
      request('POST', '/api/manual/analyze', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        planEl.textContent = formatPlan(data);
        applyPlanDefaults('enter', data);
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Analysis failed', 'error');
        } else {
          setStatus(statusEl, 'Analysis complete', 'success');
        }
      });
    }
    document.getElementById('enter-dry-run').addEventListener('click', function () { submit(true); });
    document.getElementById('enter-analyze').addEventListener('click', analyze);
    document.getElementById('enter-execute').addEventListener('click', function () { submit(false); });
  }

  function bindExit() {
    var planEl = document.getElementById('exit-plan');
    var statusEl = document.getElementById('exit-status');
    var logEl = document.getElementById('exit-exec-log');
    function buildPayload() {
      var payload = payloadCommon('exit-');
      payload.long_exchange = document.getElementById('exit-long-exchange').value;
      payload.short_exchange = document.getElementById('exit-short-exchange').value;
      payload.limit_price_long = parseFloat(document.getElementById('exit-limit-long').value || '0') || null;
      payload.limit_price_short = parseFloat(document.getElementById('exit-limit-short').value || '0') || null;
      return payload;
    }
    function submit(dryRun) {
      var payload = buildPayload();
      payload.dry_run = dryRun;
      payload.async_run = !dryRun;
      setStatus(statusEl, dryRun ? 'Running dry-run...' : 'Submitting orders...', 'info');
      if (logEl && !dryRun) {
        logEl.textContent = '';
      }
      request('POST', '/api/manual/exit', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        if (!dryRun && data && data.execution_id) {
          setStatus(statusEl, 'Execution started...', 'info');
          pollExecution(data.execution_id, 'exit', planEl, logEl, statusEl);
          return;
        }
        planEl.textContent = formatPlan(data);
        applyPlanDefaults('exit', data);
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else if (dryRun) {
          setStatus(statusEl, 'Dry-run complete', 'success');
        } else {
          setStatus(statusEl, 'Execution complete', 'success');
        }
      });
    }
    function analyze() {
      var payload = buildPayload();
      payload.action = 'exit';
      setStatus(statusEl, 'Analyzing...', 'info');
      request('POST', '/api/manual/analyze', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        planEl.textContent = formatPlan(data);
        applyPlanDefaults('exit', data);
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Analysis failed', 'error');
        } else {
          setStatus(statusEl, 'Analysis complete', 'success');
        }
      });
    }
    document.getElementById('exit-dry-run').addEventListener('click', function () { submit(true); });
    document.getElementById('exit-analyze').addEventListener('click', analyze);
    document.getElementById('exit-execute').addEventListener('click', function () { submit(false); });
  }

  function bindRoll() {
    var planEl = document.getElementById('roll-plan');
    var statusEl = document.getElementById('roll-status');
    var logEl = document.getElementById('roll-exec-log');
    function buildPayload() {
      var payload = payloadCommon('roll-');
      payload.from_exchange = document.getElementById('roll-from-exchange').value;
      payload.to_exchange = document.getElementById('roll-to-exchange').value;
      payload.side = document.getElementById('roll-side').value;
      payload.limit_price_to = parseFloat(document.getElementById('roll-limit-to').value || '0') || null;
      payload.limit_price_from = parseFloat(document.getElementById('roll-limit-from').value || '0') || null;
      return payload;
    }
    function submit(dryRun) {
      var payload = buildPayload();
      payload.dry_run = dryRun;
      payload.async_run = !dryRun;
      setStatus(statusEl, dryRun ? 'Running dry-run...' : 'Submitting orders...', 'info');
      if (logEl && !dryRun) {
        logEl.textContent = '';
      }
      request('POST', '/api/manual/roll', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          return;
        }
        if (!dryRun && data && data.execution_id) {
          setStatus(statusEl, 'Execution started...', 'info');
          pollExecution(data.execution_id, 'roll', planEl, logEl, statusEl);
          return;
        }
        planEl.textContent = formatPlan(data);
        applyPlanDefaults('roll', data);
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else if (dryRun) {
          setStatus(statusEl, 'Dry-run complete', 'success');
        } else {
          setStatus(statusEl, 'Execution complete', 'success');
        }
      });
    }
    document.getElementById('roll-dry-run').addEventListener('click', function () { submit(true); });
    document.getElementById('roll-execute').addEventListener('click', function () { submit(false); });
  }

  function init() {
    bindEnter();
    bindExit();
    bindRoll();
    applyLivePrefs('enter');
    applyLivePrefs('exit');
    setupLiveFeed('enter', 'enter-long-exchange', 'enter-short-exchange', 'enter-live');
    setupLiveFeed('exit', 'exit-long-exchange', 'exit-short-exchange', 'exit-live');
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

    var form = document.getElementById('manual-' + prefix + '-form');
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
