/**
 * Manual exchange test helpers.
 */
(function () {
  'use strict';

  var WS_TRADE_LOG_VERSION = '2026-01-09-rawlog-1';

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

  function getValue(id) {
    var el = document.getElementById(id);
    return el ? el.value : '';
  }

  function getChecked(id) {
    var el = document.getElementById(id);
    return !!(el && el.checked);
  }

  function parseOptionalNumber(value) {
    if (value === null || value === undefined || value === '') {
      return null;
    }
    var parsed = parseFloat(value);
    return isNaN(parsed) ? null : parsed;
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

  function normalizeBingxSymbol(value) {
    var symbol = normalizeInputSymbol(value);
    if (symbol.endsWith('USDT')) {
      return symbol.slice(0, -4) + '-USDT';
    }
    return symbol;
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

  var marginSymbolHints = {
    binance: 'BTCUSDT',
    bingx: 'BTC-USDT',
    gate: 'BTC_USDT',
    kucoin: 'BTCUSDTM',
    okx: 'BTC-USDT-SWAP',
    bybit: 'BTCUSDT',
    bitget: 'BTCUSDT',
    mexc: 'BTCUSDT'
  };

  var fundingSymbolHints = {
    binance: 'BTCUSDT',
    bingx: 'BTC-USDT',
    gate: 'BTC_USDT',
    kucoin: 'BTCUSDTM',
    okx: 'BTC-USDT-SWAP',
    bybit: 'BTCUSDT',
    bitget: 'BTCUSDT',
    mexc: 'BTCUSDT'
  };

  var fundingSymbolExamples = {
    binance: 'BTCUSDT, ETHUSDT',
    bingx: 'BTC-USDT, ETH-USDT',
    gate: 'BTC_USDT, ETH_USDT',
    kucoin: 'BTCUSDTM, ETHUSDTM',
    okx: 'BTC-USDT-SWAP, ETH-USDT-SWAP',
    bybit: 'BTCUSDT, ETHUSDT',
    bitget: 'BTCUSDT, ETHUSDT',
    mexc: 'BTCUSDT, ETHUSDT'
  };

  function setStatus(el, message, level) {
    if (!el) {
      return;
    }
    el.textContent = message || '';
    el.className = 'settings-status';
    if (level) {
      el.className += ' status-' + level;
    }
  }

  function pretty(value) {
    try {
      return JSON.stringify(value, null, 2);
    } catch (_err) {
      return String(value);
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

  function nowIso() {
    return new Date().toISOString();
  }

  function createScriptLogger(logEl) {
    return function (message, data) {
      if (!logEl) {
        return;
      }
      var line = '[' + nowIso() + '] ' + message;
      if (data) {
        line += ' ' + pretty(data);
      }
      logEl.textContent += line + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    };
  }

  function createWsMonitor(opts) {
    var statusEl = opts.statusEl;
    var scriptLog = opts.scriptLog;
    var silenceEl = opts.silenceEl;
    var autoProbeEl = opts.autoProbeEl;
    var probeBtn = opts.probeBtn;
    var sendPing = opts.sendPing;
    var defaultThreshold = opts.defaultThreshold || 30;
    var probeTimeoutSec = opts.probeTimeoutSec || 10;
    var connected = false;
    var lastRx = 0;
    var connectedAt = 0;
    var lastPing = 0;
    var lastPong = 0;
    var probeInFlight = false;
    var probeSentAt = 0;
    var timer = null;

    function readThreshold() {
      var value = silenceEl ? parseOptionalNumber(silenceEl.value) : null;
      return value && value > 0 ? value : defaultThreshold;
    }

    function ageMs(value, now) {
      if (!value) {
        return null;
      }
      return (now - value) / 1000.0;
    }

    function updateStatus() {
      if (!statusEl) {
        return;
      }
      if (!connected) {
        statusEl.textContent = 'Monitor: disconnected';
        return;
      }
      var now = Date.now();
      var rxAge = ageMs(lastRx || connectedAt, now);
      var pingAge = ageMs(lastPing, now);
      var pongAge = ageMs(lastPong, now);
      var threshold = readThreshold();
      var healthy = rxAge !== null && rxAge <= threshold;
      var parts = [];
      parts.push('Last RX: ' + (rxAge !== null ? rxAge.toFixed(1) + 's' : '-'));
      parts.push('Last Ping: ' + (pingAge !== null ? pingAge.toFixed(1) + 's' : '-'));
      parts.push('Last Pong: ' + (pongAge !== null ? pongAge.toFixed(1) + 's' : '-'));
      parts.push('Health: ' + (healthy ? 'LIVE' : 'STALE'));
      statusEl.textContent = parts.join(' | ');
    }

    function markRx() {
      lastRx = Date.now();
      if (probeInFlight) {
        probeInFlight = false;
        if (scriptLog) {
          scriptLog('probe response received');
        }
      }
    }

    function splitLogLine(line) {
      if (!line || line.charAt(0) !== '[') {
        return null;
      }
      var idx = line.indexOf(']');
      if (idx <= 1) {
        return null;
      }
      return {
        tag: line.slice(1, idx),
        payload: line.slice(idx + 1).trim()
      };
    }

    function tryParseJson(payload) {
      if (!payload) {
        return null;
      }
      var first = payload.charAt(0);
      if (first !== '{' && first !== '[') {
        return null;
      }
      try {
        return JSON.parse(payload);
      } catch (_err) {
        return null;
      }
    }

    function recordPing() {
      lastPing = Date.now();
    }

    function recordPong() {
      lastPong = Date.now();
    }

    function detectPingPong(tag, payload, obj) {
      var text = String(payload || '').trim();
      var lower = text.toLowerCase();
      if (lower === 'ping') {
        recordPing();
        return;
      }
      if (lower === 'pong') {
        recordPong();
        return;
      }
      if (obj && typeof obj === 'object') {
        var op = obj.op || obj.type || obj.event || obj.channel;
        if (typeof op === 'string') {
          var opLower = op.toLowerCase();
          if (opLower === 'ping' || opLower.indexOf('ping') >= 0 && opLower.indexOf('pong') < 0) {
            recordPing();
          }
          if (opLower === 'pong' || opLower.indexOf('pong') >= 0) {
            recordPong();
          }
        }
      }
      if (tag === 'tx' && lower.indexOf('ping') >= 0) {
        recordPing();
      }
    }

    function detectAck(obj) {
      if (!obj || typeof obj !== 'object') {
        return;
      }
      var event = obj.event || obj.op || obj.type;
      var channel = obj.channel || (obj.arg && obj.arg.channel) || '';
      if (typeof channel !== 'string') {
        channel = '';
      }
      if (event === 'login' || event === 'auth' || channel.indexOf('login') >= 0) {
        if (scriptLog) {
          scriptLog('login response', { event: event || 'login', channel: channel || null });
        }
      }
      if (event === 'subscribe' || event === 'subscribed' || event === 'ack') {
        if (scriptLog) {
          scriptLog('subscribe ack', { channel: channel || null });
        }
      }
    }

    function onLine(line) {
      var parts = splitLogLine(line);
      if (!parts) {
        return;
      }
      var tag = parts.tag;
      var payload = parts.payload;
      if (tag.indexOf('rx') === 0) {
        markRx();
      }
      var obj = tryParseJson(payload);
      detectPingPong(tag, payload, obj);
      detectAck(obj);
      if (tag === 'err' && scriptLog) {
        scriptLog('remote error', { message: payload });
      }
      if (tag === 'sys' && payload.indexOf('remote ws closed') >= 0 && scriptLog) {
        scriptLog('remote ws closed');
      }
    }

    function sendProbe(manual, silenceSec) {
      if (!sendPing) {
        return;
      }
      var ok = sendPing();
      if (!ok) {
        if (scriptLog) {
          scriptLog('probe ping skipped: not connected');
        }
        return;
      }
      probeInFlight = true;
      probeSentAt = Date.now();
      if (scriptLog) {
        scriptLog(
          manual ? 'manual probe ping sent' : 'probe ping sent',
          { silence_sec: silenceSec !== undefined ? silenceSec : null }
        );
      }
    }

    function tick() {
      if (!connected) {
        updateStatus();
        return;
      }
      var now = Date.now();
      var threshold = readThreshold();
      var rxAge = ageMs(lastRx || connectedAt, now);
      if (autoProbeEl && autoProbeEl.checked && connectedAt && rxAge !== null && rxAge >= threshold) {
        if (!probeInFlight && (!probeSentAt || (now - probeSentAt) / 1000.0 >= threshold)) {
          sendProbe(false, rxAge !== null ? rxAge.toFixed(1) : null);
        }
      }
      if (probeInFlight && (now - probeSentAt) / 1000.0 >= probeTimeoutSec) {
        probeInFlight = false;
        if (scriptLog) {
          scriptLog('probe timeout', { silence_sec: rxAge !== null ? rxAge.toFixed(1) : null });
        }
      }
      updateStatus();
    }

    function startTimer() {
      if (timer) {
        return;
      }
      timer = window.setInterval(tick, 1000);
    }

    function stopTimer() {
      if (!timer) {
        return;
      }
      window.clearInterval(timer);
      timer = null;
    }

    function onOpen() {
      connected = true;
      lastRx = 0;
      connectedAt = Date.now();
      lastPing = 0;
      lastPong = 0;
      probeInFlight = false;
      probeSentAt = 0;
      if (scriptLog) {
        scriptLog('ws connected; waiting for login/subscription');
      }
      startTimer();
      updateStatus();
    }

    function onClose() {
      connected = false;
      connectedAt = 0;
      stopTimer();
      updateStatus();
      if (scriptLog) {
        scriptLog('ws closed');
      }
    }

    if (probeBtn) {
      probeBtn.addEventListener('click', function () {
        sendProbe(true, null);
      });
    }
    if (autoProbeEl) {
      autoProbeEl.addEventListener('change', function () {
        updateStatus();
      });
    }
    if (silenceEl) {
      silenceEl.addEventListener('change', function () {
        updateStatus();
      });
    }

    return {
      onOpen: onOpen,
      onClose: onClose,
      onLine: onLine,
      logAction: function (message, data) {
        if (scriptLog) {
          scriptLog(message, data);
        }
      }
    };
  }

  function buildPayload() {
    return {
      exchange: (getValue('test-exchange') || '').trim(),
      symbol: (getValue('test-symbol') || '').trim().toUpperCase(),
      side: (getValue('test-side') || '').trim().toLowerCase(),
      qty: parseOptionalNumber(getValue('test-qty')),
      price: parseOptionalNumber(getValue('test-price')),
      offset_bps: parseOptionalNumber(getValue('test-offset-bps')),
      offset_ticks: parseOptionalNumber(getValue('test-offset-ticks')),
      margin_mode: getValue('test-margin-mode') || null,
      reduce_only: getChecked('test-reduce-only'),
      position_side: (getValue('test-position-side') || '').trim()
    };
  }

  function bind() {
    var statusEl = document.getElementById('test-status');
    var resultEl = document.getElementById('test-result');
    var orderIdEl = document.getElementById('test-order-id');

    function handleResponse(err, data) {
      if (err) {
        setStatus(statusEl, err.message, 'error');
        resultEl.textContent = err.message;
        return;
      }
      resultEl.textContent = pretty(data);
      if (data && data.errors && data.errors.length) {
        setStatus(statusEl, 'Completed with errors', 'error');
      } else {
        setStatus(statusEl, 'Completed', 'success');
      }
      if (data && data.order_id && orderIdEl && !orderIdEl.value) {
        orderIdEl.value = data.order_id;
      }
    }

    document.getElementById('test-limit').addEventListener('click', function () {
      setStatus(statusEl, 'Submitting limit...', 'info');
      request('POST', '/api/manual/test/limit', buildPayload(), handleResponse);
    });

    document.getElementById('test-market').addEventListener('click', function () {
      setStatus(statusEl, 'Submitting market...', 'info');
      request('POST', '/api/manual/test/market', buildPayload(), handleResponse);
    });

    document.getElementById('test-cancel').addEventListener('click', function () {
      var payload = {
        exchange: (getValue('test-exchange') || '').trim(),
        symbol: (getValue('test-symbol') || '').trim().toUpperCase(),
        order_id: (getValue('test-order-id') || '').trim()
      };
      setStatus(statusEl, 'Canceling order...', 'info');
      request('POST', '/api/manual/test/cancel', payload, handleResponse);
    });
  }

  function bindMarginTests() {
    var cards = document.querySelectorAll('[data-margin-exchange]');
    if (!cards || !cards.length) {
      return;
    }
    Array.prototype.forEach.call(cards, function (card) {
      var exchange = (card.getAttribute('data-margin-exchange') || '').trim();
      var symbolEl = card.querySelector('[data-margin-symbol]');
      var sideEl = card.querySelector('[data-margin-side]');
      var modeEl = card.querySelector('[data-margin-mode]');
      var amountEl = card.querySelector('[data-margin-amount]');
      var leverageEl = card.querySelector('[data-margin-leverage]');
      var statusEl = card.querySelector('[data-margin-status]');
      var resultEl = card.querySelector('[data-margin-result]');
      var viewEl = card.querySelector('[data-margin-view]');
      var logEl = card.querySelector('[data-margin-log]');
      var fetchBtn = card.querySelector('[data-margin-fetch]');
      var addBtn = card.querySelector('[data-margin-add]');
      var reduceBtn = card.querySelector('[data-margin-reduce]');
      var leverageBtn = card.querySelector('[data-margin-leverage-set]');
      var leverageBinanceBtn = card.querySelector('[data-margin-leverage-set-binance]');
      var scriptLog = createScriptLogger(logEl);

      if (symbolEl && marginSymbolHints[exchange]) {
        symbolEl.placeholder = marginSymbolHints[exchange];
      }

      function readSymbol() {
        return (symbolEl ? symbolEl.value : '').trim().toUpperCase();
      }

      function readSide() {
        var side = (sideEl ? sideEl.value : '').trim().toLowerCase();
        if (!side || side === 'auto') {
          return null;
        }
        return side;
      }

      function readAmount() {
        return amountEl ? parseOptionalNumber(amountEl.value) : null;
      }

      function readMarginMode() {
        var mode = (modeEl ? modeEl.value : '').trim().toLowerCase();
        if (!mode) {
          return null;
        }
        return mode;
      }

      function readLeverage() {
        return leverageEl ? parseOptionalNumber(leverageEl.value) : null;
      }

      function setResult(data) {
        if (!resultEl) {
          return;
        }
        resultEl.textContent = pretty(data || {});
      }

      function setView(data) {
        if (!viewEl) {
          return;
        }
        if (!data) {
          viewEl.textContent = '';
          return;
        }
        var viewPayload = {};
        if (data.error) {
          viewPayload.error = data.error;
        }
        if (data.errors) {
          viewPayload.errors = data.errors;
        }
        if (data.position_view) {
          viewPayload.position_view = data.position_view;
        }
        if (data.before || data.after) {
          viewPayload.before = data.before ? data.before.position_view || null : null;
          viewPayload.after = data.after ? data.after.position_view || null : null;
        }
        if (data.margin_mode) {
          viewPayload.margin_mode = data.margin_mode;
        }
        if (data.target_leverage !== undefined) {
          viewPayload.target_leverage = data.target_leverage;
        }
        if (!Object.keys(viewPayload).length) {
          viewEl.textContent = '';
          return;
        }
        viewEl.textContent = pretty(viewPayload);
      }

      function handleResponse(err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          setResult({ error: err.message });
          setView({ error: err.message });
          return;
        }
        setResult(data);
        setView(data);
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else {
          setStatus(statusEl, 'Completed', 'success');
        }
      }

      function sendRequest(label, url, payload) {
        setStatus(statusEl, label + '...', 'info');
        scriptLog(label.toLowerCase() + ' requested', payload);
        request('POST', url, payload, handleResponse);
      }

      function fetchPosition() {
        var symbol = readSymbol();
        if (!symbol) {
          setStatus(statusEl, 'Symbol required', 'error');
          return;
        }
        var payload = {
          exchange: exchange,
          symbol: symbol
        };
        var side = readSide();
        if (side) {
          payload.side = side;
        }
        sendRequest('Fetching position', '/api/manual/test/position', payload);
      }

      function updateMargin(action) {
        var symbol = readSymbol();
        if (!symbol) {
          setStatus(statusEl, 'Symbol required', 'error');
          return;
        }
        var amount = readAmount();
        if (!amount || amount <= 0) {
          setStatus(statusEl, 'Amount must be > 0', 'error');
          return;
        }
        var payload = {
          exchange: exchange,
          symbol: symbol,
          amount: amount
        };
        var side = readSide();
        if (side) {
          payload.side = side;
        }
        sendRequest(
          action === 'add' ? 'Adding margin' : 'Reducing margin',
          action === 'add' ? '/api/manual/test/margin/add' : '/api/manual/test/margin/reduce',
          payload
        );
      }

      function updateLeverage() {
        var symbol = readSymbol();
        if (!symbol) {
          setStatus(statusEl, 'Symbol required', 'error');
          return;
        }
        var leverage = readLeverage();
        if (!leverage || leverage <= 0) {
          setStatus(statusEl, 'Leverage must be > 0', 'error');
          return;
        }
        var payload = {
          exchange: exchange,
          symbol: symbol,
          leverage: leverage
        };
        var side = readSide();
        if (side) {
          payload.side = side;
        }
        var marginMode = readMarginMode();
        if (marginMode) {
          payload.margin_mode = marginMode;
        }
        sendRequest('Setting leverage', '/api/manual/test/leverage', payload);
      }

      function updateLeverageBinance() {
        var symbol = readSymbol();
        if (!symbol) {
          setStatus(statusEl, 'Symbol required', 'error');
          return;
        }
        var leverage = readLeverage();
        if (!leverage || leverage <= 0) {
          setStatus(statusEl, 'Leverage must be > 0', 'error');
          return;
        }
        var payload = {
          exchange: exchange,
          symbol: symbol,
          leverage: leverage
        };
        var marginMode = readMarginMode();
        if (marginMode) {
          payload.margin_mode = marginMode;
        }
        sendRequest('Setting leverage (binance)', '/api/manual/test/leverage/binance', payload);
      }

      if (fetchBtn) {
        fetchBtn.addEventListener('click', function () {
          fetchPosition();
        });
      }
      if (addBtn) {
        addBtn.addEventListener('click', function () {
          updateMargin('add');
        });
      }
      if (reduceBtn) {
        reduceBtn.addEventListener('click', function () {
          updateMargin('reduce');
        });
      }
      if (leverageBtn) {
        leverageBtn.addEventListener('click', function () {
          updateLeverage();
        });
      }
      if (leverageBinanceBtn) {
        leverageBinanceBtn.addEventListener('click', function () {
          updateLeverageBinance();
        });
      }
    });
  }

  function bindFundingTests() {
    var cards = document.querySelectorAll('[data-funding-exchange]');
    if (!cards || !cards.length) {
      return;
    }
    Array.prototype.forEach.call(cards, function (card) {
      var exchange = (card.getAttribute('data-funding-exchange') || '').trim();
      var symbolEl = card.querySelector('[data-funding-symbol]');
      var rawEl = card.querySelector('[data-funding-raw]');
      var examplesEl = card.querySelector('[data-funding-examples]');
      var historyLimitEl = card.querySelector('[data-funding-history-limit]');
      var snapshotStatusEl = card.querySelector('[data-funding-snapshot-status]');
      var snapshotResultEl = card.querySelector('[data-funding-snapshot-result]');
      var snapshotRawEl = card.querySelector('[data-funding-snapshot-raw]');
      var snapshotLogEl = card.querySelector('[data-funding-snapshot-log]');
      var snapshotBtn = card.querySelector('[data-funding-snapshot-fetch]');
      var historyStatusEl = card.querySelector('[data-funding-history-status]');
      var historyResultEl = card.querySelector('[data-funding-history-result]');
      var historyRawEl = card.querySelector('[data-funding-history-raw]');
      var historyLogEl = card.querySelector('[data-funding-history-log]');
      var historyBtn = card.querySelector('[data-funding-history-fetch]');
      var snapshotLog = createScriptLogger(snapshotLogEl);
      var historyLog = createScriptLogger(historyLogEl);

      if (symbolEl && fundingSymbolHints[exchange]) {
        symbolEl.placeholder = fundingSymbolHints[exchange];
      }
      if (examplesEl && fundingSymbolExamples[exchange]) {
        examplesEl.textContent = 'Examples: ' + fundingSymbolExamples[exchange];
      }

      function readSymbol() {
        return normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      }

      function readHistoryLimit() {
        if (!historyLimitEl) {
          return 12;
        }
        var parsed = parseOptionalNumber(historyLimitEl.value);
        if (!parsed || parsed <= 0) {
          return 12;
        }
        var limit = Math.round(parsed);
        if (limit < 1) {
          limit = 1;
        }
        if (limit > 200) {
          limit = 200;
        }
        return limit;
      }

      function buildPayload() {
        return {
          exchange: exchange,
          symbol: readSymbol(),
          include_raw: !!(rawEl && rawEl.checked),
          history_limit: readHistoryLimit()
        };
      }

      function setResult(el, data) {
        if (!el) {
          return;
        }
        el.textContent = pretty(data || {});
      }

      function setRaw(el, data, rawEnabled) {
        if (!el) {
          return;
        }
        if (!rawEnabled) {
          el.textContent = 'Raw disabled (enable "Include raw API response").';
          return;
        }
        el.textContent = pretty(data || {});
      }

      function setStatusForResponse(statusEl, data) {
        if (!statusEl) {
          return;
        }
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else {
          setStatus(statusEl, 'Completed', 'success');
        }
      }

      function buildSnapshotSummary(data) {
        var summary = {
          exchange: data ? data.exchange : null,
          symbol: data ? data.symbol : null,
          exchange_symbol: data ? data.exchange_symbol : null,
          funding_rate: data ? data.funding_rate : null,
          funding_interval_hours: data ? data.funding_interval_hours : null,
          next_funding_time: data ? data.next_funding_time : null,
          seconds_to_next: data ? data.seconds_to_next : null,
          next_funding_eta: data ? data.next_funding_eta : null,
          mark_price: data ? data.mark_price : null,
          sources: data ? data.sources : null,
          attempts: data ? data.attempts : null,
          warnings: data ? data.warnings : null
        };
        if (data && data.errors) {
          summary.errors = data.errors;
        }
        return summary;
      }

      function buildHistorySummary(data) {
        var history = data && data.funding_history ? data.funding_history : [];
        var summary = {
          exchange: data ? data.exchange : null,
          symbol: data ? data.symbol : null,
          exchange_symbol: data ? data.exchange_symbol : null,
          history_limit: data ? data.history_limit : null,
          history_count: history.length,
          funding_history: history
        };
        if (data && data.warnings) {
          summary.warnings = data.warnings;
        }
        if (data && data.errors) {
          summary.errors = data.errors;
        }
        return summary;
      }

      function handleSnapshotResponse(err, data) {
        if (err) {
          setStatus(snapshotStatusEl, err.message, 'error');
          setResult(snapshotResultEl, { error: err.message });
          setRaw(snapshotRawEl, { error: err.message }, true);
          return;
        }
        setResult(snapshotResultEl, buildSnapshotSummary(data));
        setStatusForResponse(snapshotStatusEl, data);
        var rawEnabled = !!(rawEl && rawEl.checked);
        if (rawEnabled) {
          var rawPayload = {
            exchange: data && data.raw ? data.raw.exchange : (data ? data.exchange : null),
            symbol: data && data.raw ? data.raw.symbol : (data ? data.symbol : null),
            exchange_symbol: data && data.raw ? data.raw.exchange_symbol : (data ? data.exchange_symbol : null),
            fetched_at: data && data.raw ? data.raw.fetched_at : null,
            snapshot: data && data.raw ? data.raw.snapshot : null
          };
          if (data && data.raw_error) {
            rawPayload.raw_error = data.raw_error;
          }
          if (data && data.raw && data.raw.errors) {
            rawPayload.errors = data.raw.errors;
          }
          setRaw(snapshotRawEl, rawPayload, true);
        } else {
          setRaw(snapshotRawEl, null, false);
        }
      }

      function handleHistoryResponse(err, data) {
        if (err) {
          setStatus(historyStatusEl, err.message, 'error');
          setResult(historyResultEl, { error: err.message });
          setRaw(historyRawEl, { error: err.message }, true);
          return;
        }
        setResult(historyResultEl, buildHistorySummary(data));
        setStatusForResponse(historyStatusEl, data);
        var rawEnabled = !!(rawEl && rawEl.checked);
        if (rawEnabled) {
          var rawPayload = {
            exchange: data && data.raw ? data.raw.exchange : (data ? data.exchange : null),
            symbol: data && data.raw ? data.raw.symbol : (data ? data.symbol : null),
            exchange_symbol: data && data.raw ? data.raw.exchange_symbol : (data ? data.exchange_symbol : null),
            fetched_at: data && data.raw ? data.raw.fetched_at : null,
            history: data && data.raw ? data.raw.history : null
          };
          if (data && data.raw_error) {
            rawPayload.raw_error = data.raw_error;
          }
          if (data && data.raw && data.raw.errors) {
            rawPayload.errors = data.raw.errors;
          }
          setRaw(historyRawEl, rawPayload, true);
        } else {
          setRaw(historyRawEl, null, false);
        }
      }

      function fetchSnapshot() {
        var symbol = readSymbol();
        if (!symbol) {
          setStatus(snapshotStatusEl, 'Symbol required', 'error');
          return;
        }
        var payload = buildPayload();
        setStatus(snapshotStatusEl, 'Fetching snapshot...', 'info');
        snapshotLog('snapshot requested', payload);
        request('POST', '/api/manual/test/funding', payload, handleSnapshotResponse);
      }

      function fetchHistory() {
        var symbol = readSymbol();
        if (!symbol) {
          setStatus(historyStatusEl, 'Symbol required', 'error');
          return;
        }
        var payload = buildPayload();
        setStatus(historyStatusEl, 'Fetching history...', 'info');
        historyLog('history requested', payload);
        request('POST', '/api/manual/test/funding', payload, handleHistoryResponse);
      }

      if (snapshotBtn) {
        snapshotBtn.addEventListener('click', function () {
          fetchSnapshot();
        });
      }

      if (historyBtn) {
        historyBtn.addEventListener('click', function () {
          fetchHistory();
        });
      }
    });
  }

  function bindCoinAnalysisTests() {
    var symbolEl = document.getElementById('coin-analysis-symbol');
    var windowEl = document.getElementById('coin-analysis-window');
    var fundingPointsEl = document.getElementById('coin-analysis-funding-points');
    var includeSeriesEl = document.getElementById('coin-analysis-include-series');
    var statusEl = document.getElementById('coin-analysis-status');
    var summaryEl = document.getElementById('coin-analysis-summary');
    var rawEl = document.getElementById('coin-analysis-raw');
    var logEl = document.getElementById('coin-analysis-log');
    var fetchBtn = document.getElementById('coin-analysis-fetch');
    if (!fetchBtn) {
      return;
    }

    var log = createScriptLogger(logEl);

    function readSymbol() {
      return normalizeInputSymbol(symbolEl ? symbolEl.value : '');
    }

    function readWindow() {
      var value = parseOptionalNumber(windowEl ? windowEl.value : null);
      var minutes = value ? Math.round(value) : 4320;
      if (minutes < 60) {
        minutes = 60;
      }
      if (minutes > 4320) {
        minutes = 4320;
      }
      return minutes;
    }

    function readFundingPoints() {
      var value = parseOptionalNumber(fundingPointsEl ? fundingPointsEl.value : null);
      var points = value ? Math.round(value) : 120;
      if (points < 24) {
        points = 24;
      }
      if (points > 200) {
        points = 200;
      }
      return points;
    }

    function buildSummary(data) {
      var payload = data || {};
      var analysis = payload.analysis || {};
      var exchanges = analysis.exchanges || [];
      var exchangeRows = exchanges.map(function (row) {
        var quality = row && row.data_quality ? row.data_quality : {};
        return {
          exchange: row ? row.exchange : null,
          status: row ? row.status : null,
          funding_interval_hours: row ? row.funding_interval_hours_resolved : null,
          latest_funding_rate: row ? row.latest_funding_rate : null,
          candles_1m_count: row ? row.candles_1m_count : null,
          candles_coverage_pct: quality ? quality.candles_coverage_pct : null,
          oi_points_received: quality ? quality.oi_points_received : null,
          warnings: row ? row.warnings : null,
          errors: row ? row.errors : null
        };
      });
      return {
        symbol: payload.symbol || analysis.symbol || null,
        window_minutes: payload.window_minutes,
        funding_points: payload.funding_points,
        include_series: payload.include_series,
        summary: payload.summary || null,
        analysis_warnings: analysis.warnings || null,
        exchange_rows: exchangeRows
      };
    }

    function runAnalysis() {
      var symbol = readSymbol();
      if (!symbol) {
        setStatus(statusEl, 'Symbol required', 'error');
        return;
      }
      var payload = {
        symbol: symbol,
        window_minutes: readWindow(),
        funding_points: readFundingPoints(),
        include_series: !!(includeSeriesEl && includeSeriesEl.checked)
      };
      setStatus(statusEl, 'Running analysis...', 'info');
      log('coin analysis requested', payload);
      request('POST', '/api/manual/test/coin-analysis', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          if (summaryEl) {
            summaryEl.textContent = pretty({ error: err.message });
          }
          if (rawEl) {
            rawEl.textContent = pretty({ error: err.message });
          }
          return;
        }
        if (summaryEl) {
          summaryEl.textContent = pretty(buildSummary(data));
        }
        if (rawEl) {
          rawEl.textContent = pretty(data || {});
        }
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else {
          setStatus(statusEl, 'Completed', 'success');
        }
      });
    }

    fetchBtn.addEventListener('click', function () {
      runAnalysis();
    });
  }

  function bindWsTest() {
    var statusEl = document.getElementById('ws-status');
    var liveEl = document.getElementById('ws-live');
    var bookEl = document.getElementById('ws-live-book');
    var ws = null;

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/manual';
    }

    function setLive(text, tone) {
      if (!liveEl) {
        return;
      }
      liveEl.textContent = text;
      liveEl.className = 'live-metrics' + (tone ? (' live-metrics--' + tone) : '');
    }

    function currentPayload() {
      var symbol = (getValue('ws-symbol') || '').trim().toUpperCase();
      var longExchange = (getValue('ws-long-exchange') || '').trim();
      var shortExchange = (getValue('ws-short-exchange') || '').trim();
      var includeOrderbook = getChecked('ws-include-orderbook');
      var depth = parseInt(getValue('ws-live-depth'), 10) || 5;
      return {
        action: 'subscribe',
        symbol: symbol,
        long_exchange: longExchange,
        short_exchange: shortExchange,
        spread_min_pct: parseOptionalNumber(getValue('ws-spread-min')),
        spread_max_pct: parseOptionalNumber(getValue('ws-spread-max')),
        include_orderbook: includeOrderbook,
        orderbook_depth: depth
      };
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
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
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
          setStatus(statusEl, data.error || 'Error', 'error');
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
        setStatus(statusEl, 'Disconnected', 'info');
        setLive('Live spread: -', '');
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        ws.close();
      } catch (_err) {
        // ignore close errors
      }
      ws = null;
    }

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    var connectBtn = document.getElementById('ws-connect');
    var disconnectBtn = document.getElementById('ws-disconnect');
    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }

    var form = document.getElementById('ws-test-form');
    if (form) {
      form.addEventListener('change', function () {
        subscribe();
      });
    }
  }

  function bindWsTradeTest() {
    var statusEl = document.getElementById('ws-trade-status');
    var logEl = document.getElementById('ws-trade-log');
    var logRxEl = document.getElementById('ws-trade-log-rx');
    var lastIdEl = document.getElementById('ws-trade-last-id');
    var versionEl = document.getElementById('ws-trade-log-version');
    var ws = null;
    var lastOrderId = '';

    if (versionEl) {
      versionEl.textContent = WS_TRADE_LOG_VERSION;
    }

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/manual-trade';
    }

    function logLine(el, payload) {
      if (!el) {
        return;
      }
      var text = pretty(payload);
      el.textContent += text + '\n';
      el.scrollTop = el.scrollHeight;
    }

    function currentConfig() {
      return {
        action: 'connect',
        exchange: (getValue('ws-trade-exchange') || '').trim(),
        symbol: (getValue('ws-trade-symbol') || '').trim().toUpperCase()
      };
    }

    function send(payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine(logEl, { type: 'client', error: 'ws_not_connected', payload: payload });
        return;
      }
      logLine(logRxEl, { type: 'tx', ts: new Date().toISOString(), payload: payload });
      ws.send(JSON.stringify(payload));
    }

    function placeOrder(orderType, priceMode) {
      var payload = {
        action: 'order',
        exchange: (getValue('ws-trade-exchange') || '').trim(),
        symbol: (getValue('ws-trade-symbol') || '').trim().toUpperCase(),
        side: (getValue('ws-trade-side') || '').trim().toLowerCase(),
        qty: parseOptionalNumber(getValue('ws-trade-qty')),
        order_type: orderType,
        price: parseOptionalNumber(getValue('ws-trade-price')),
        price_mode: priceMode || '',
        offset_bps: parseOptionalNumber(getValue('ws-trade-offset-bps')) || 0,
        offset_ticks: parseOptionalNumber(getValue('ws-trade-offset-ticks')) || 0,
        reduce_only: getChecked('ws-trade-reduce-only'),
        position_side: (getValue('ws-trade-position-side') || '').trim().toLowerCase()
      };
      send(payload);
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine(logRxEl, { type: 'status', ts: new Date().toISOString(), status: 'ws_open' });
        send(currentConfig());
      };
      ws.onmessage = function (evt) {
        logLine(logRxEl, { ts: new Date().toISOString(), raw: evt.data });
        var data = null;
        try {
          data = JSON.parse(evt.data);
        } catch (_err) {
          return;
        }
        if (!data) {
          return;
        }
        if (data.type === 'order_ack' && data.order_id) {
          lastOrderId = data.order_id;
          var orderIdEl = document.getElementById('ws-trade-order-id');
          if (lastIdEl) {
            lastIdEl.value = data.order_id;
          }
          if (orderIdEl && !orderIdEl.value) {
            orderIdEl.value = data.order_id;
          }
        }
        if (data.type === 'rx' && data.payload && data.payload.op === 'order.create') {
          var ack = data.payload;
          if (ack.data && ack.data.orderId) {
            lastOrderId = ack.data.orderId;
            var rxOrderIdEl = document.getElementById('ws-trade-order-id');
            if (lastIdEl) {
              lastIdEl.value = ack.data.orderId;
            }
            if (rxOrderIdEl && !rxOrderIdEl.value) {
              rxOrderIdEl.value = ack.data.orderId;
            }
          }
        }
        if (data.type === 'error') {
          setStatus(statusEl, data.error || 'Error', 'error');
        }
        if (data.type === 'tx' && data.source) {
          logLine(logEl, data);
        }
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine(logRxEl, { type: 'status', ts: new Date().toISOString(), status: 'ws_closed' });
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine(logRxEl, { type: 'status', ts: new Date().toISOString(), status: 'ws_error' });
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        ws.close();
      } catch (_err) {
        // ignore close errors
      }
      ws = null;
    }

    var connectBtn = document.getElementById('ws-trade-connect');
    var disconnectBtn = document.getElementById('ws-trade-disconnect');
    var limitFillBtn = document.getElementById('ws-trade-limit-fill');
    var limitRestBtn = document.getElementById('ws-trade-limit-rest');
    var marketBtn = document.getElementById('ws-trade-market');
    var cancelBtn = document.getElementById('ws-trade-cancel');
    var fetchBtn = document.getElementById('ws-trade-fetch');
    var useLastBtn = document.getElementById('ws-trade-use-last');
    var copyLastBtn = document.getElementById('ws-trade-copy-last');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (limitFillBtn) {
      limitFillBtn.addEventListener('click', function () {
        placeOrder('limit', 'marketable');
      });
    }
    if (limitRestBtn) {
      limitRestBtn.addEventListener('click', function () {
        placeOrder('limit', 'passive');
      });
    }
    if (marketBtn) {
      marketBtn.addEventListener('click', function () {
        placeOrder('market', '');
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        var orderId = (getValue('ws-trade-order-id') || '').trim() || lastOrderId;
        send({
          action: 'cancel',
          exchange: (getValue('ws-trade-exchange') || '').trim(),
          symbol: (getValue('ws-trade-symbol') || '').trim().toUpperCase(),
          order_id: orderId
        });
      });
    }
    if (fetchBtn) {
      fetchBtn.addEventListener('click', function () {
        var orderId = (getValue('ws-trade-order-id') || '').trim() || lastOrderId;
        send({
          action: 'fetch',
          exchange: (getValue('ws-trade-exchange') || '').trim(),
          symbol: (getValue('ws-trade-symbol') || '').trim().toUpperCase(),
          order_id: orderId
        });
      });
    }
    if (useLastBtn) {
      useLastBtn.addEventListener('click', function () {
        var orderIdEl = document.getElementById('ws-trade-order-id');
        if (orderIdEl && lastOrderId) {
          orderIdEl.value = lastOrderId;
          setStatus(statusEl, 'Order id set', 'success');
        }
      });
    }
    if (copyLastBtn) {
      copyLastBtn.addEventListener('click', function () {
        if (!lastOrderId) {
          return;
        }
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(lastOrderId).then(function () {
            setStatus(statusEl, 'Copied', 'success');
          }).catch(function () {
            setStatus(statusEl, 'Copy failed', 'error');
          });
        } else {
          var temp = document.createElement('textarea');
          temp.value = lastOrderId;
          document.body.appendChild(temp);
          temp.select();
          try {
            document.execCommand('copy');
            setStatus(statusEl, 'Copied', 'success');
          } catch (_err) {
            setStatus(statusEl, 'Copy failed', 'error');
          }
          document.body.removeChild(temp);
        }
      });
    }
  }

  function bindWsTradeRaw() {
    var statusEl = document.getElementById('ws-trade-raw-status');
    var logEl = document.getElementById('ws-trade-raw-log');
    var monitorEl = document.getElementById('ws-trade-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-raw-probe');
    var payloadEl = document.getElementById('ws-trade-raw-payload');
    var symbolEl = document.getElementById('ws-trade-raw-symbol');
    var sideEl = document.getElementById('ws-trade-raw-side');
    var qtyEl = document.getElementById('ws-trade-raw-qty');
    var priceEl = document.getElementById('ws-trade-raw-price');
    var orderIdEl = document.getElementById('ws-trade-raw-order-id');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 45,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('send', { op: 'ping' });
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; auth is handled server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function sendOrder() {
      var symbol = symbolEl ? String(symbolEl.value || '').trim().toUpperCase() : '';
      var side = sideEl ? String(sideEl.value || '').trim().toLowerCase() : '';
      var qty = qtyEl ? parseOptionalNumber(qtyEl.value) : null;
      var price = priceEl ? parseOptionalNumber(priceEl.value) : null;
      if (!symbol || !side || !qty || !price) {
        setStatus(statusEl, 'Symbol/side/qty/price required', 'error');
        return;
      }
      var ts = String(Date.now());
      var reqId = 'req-' + ts;
      var payload = {
        op: 'order.create',
        reqId: reqId,
        header: {
          'X-BAPI-TIMESTAMP': ts,
          'X-BAPI-RECV-WINDOW': '5000'
        },
        args: [
          {
            category: 'linear',
            symbol: symbol,
            side: side === 'buy' ? 'Buy' : 'Sell',
            orderType: 'Limit',
            qty: String(qty),
            price: String(price),
            timeInForce: 'GTC',
            apiTimestamp: ts,
            recvWindow: 5000
          }
        ]
      };
      monitor.logAction('order.create sent', { symbol: symbol, side: side, qty: qty, price: price });
      send('send', payload);
    }

    function sendCancel() {
      var symbol = symbolEl ? String(symbolEl.value || '').trim().toUpperCase() : '';
      var orderId = orderIdEl ? String(orderIdEl.value || '').trim() : '';
      if (!symbol || !orderId) {
        setStatus(statusEl, 'Symbol/orderId required', 'error');
        return;
      }
      var ts = String(Date.now());
      var payload = {
        op: 'order.cancel',
        header: {
          'X-BAPI-TIMESTAMP': ts,
          'X-BAPI-RECV-WINDOW': '5000'
        },
        args: [
          {
            category: 'linear',
            symbol: symbol,
            orderId: orderId,
            apiTimestamp: ts,
            recvWindow: 5000
          }
        ]
      };
      monitor.logAction('order.cancel sent', { symbol: symbol, order_id: orderId });
      send('send', payload);
    }

    var connectBtn = document.getElementById('ws-trade-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-raw-send');
    var orderBtn = document.getElementById('ws-trade-raw-order');
    var cancelBtn = document.getElementById('ws-trade-raw-cancel');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (orderBtn) {
      orderBtn.addEventListener('click', function () {
        sendOrder();
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        sendCancel();
      });
    }
  }

  function bindWsTradePrivateRaw() {
    var statusEl = document.getElementById('ws-trade-private-raw-status');
    var logEl = document.getElementById('ws-trade-private-raw-log');
    var monitorEl = document.getElementById('ws-trade-private-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-private-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-private-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-private-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-private-raw-probe');
    var payloadEl = document.getElementById('ws-trade-private-raw-payload');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 45,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('send', { op: 'ping' });
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-private-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; auth is handled server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function sendSubscribe(topic) {
      var payload = { op: 'subscribe', args: [topic] };
      monitor.logAction('subscribe requested', { topic: topic });
      send('send', payload);
    }

    var connectBtn = document.getElementById('ws-trade-private-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-private-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-private-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-private-raw-send');
    var subOrderBtn = document.getElementById('ws-trade-private-sub-order');
    var subExecFastBtn = document.getElementById('ws-trade-private-sub-exec-fast');
    var subPositionBtn = document.getElementById('ws-trade-private-sub-position');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (subOrderBtn) {
      subOrderBtn.addEventListener('click', function () {
        sendSubscribe('order');
      });
    }
    if (subExecFastBtn) {
      subExecFastBtn.addEventListener('click', function () {
        sendSubscribe('execution.fast');
      });
    }
    if (subPositionBtn) {
      subPositionBtn.addEventListener('click', function () {
        sendSubscribe('position');
      });
    }
  }

  function bindWsTradeOkxRaw() {
    var statusEl = document.getElementById('ws-trade-okx-raw-status');
    var logEl = document.getElementById('ws-trade-okx-raw-log');
    var monitorEl = document.getElementById('ws-trade-okx-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-okx-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-okx-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-okx-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-okx-raw-probe');
    var payloadEl = document.getElementById('ws-trade-okx-raw-payload');
    var instIdEl = document.getElementById('ws-trade-okx-instid');
    var sideEl = document.getElementById('ws-trade-okx-side');
    var qtyEl = document.getElementById('ws-trade-okx-qty');
    var priceEl = document.getElementById('ws-trade-okx-price');
    var orderIdEl = document.getElementById('ws-trade-okx-order-id');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 45,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('send', 'ping');
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-okx-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; auth is handled server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function sendOrder() {
      var instId = instIdEl ? String(instIdEl.value || '').trim() : '';
      var side = sideEl ? String(sideEl.value || '').trim().toLowerCase() : '';
      var qty = qtyEl ? parseOptionalNumber(qtyEl.value) : null;
      var price = priceEl ? parseOptionalNumber(priceEl.value) : null;
      if (!instId || !side || !qty || !price) {
        setStatus(statusEl, 'InstId/side/qty/price required', 'error');
        return;
      }
      var reqId = String(Date.now());
      var payload = {
        id: reqId,
        op: 'order',
        args: [
          {
            instId: instId,
            tdMode: 'isolated',
            side: side,
            ordType: 'limit',
            sz: String(qty),
            px: String(price)
          }
        ]
      };
      monitor.logAction('order request sent', { instId: instId, side: side, qty: qty, price: price });
      send('send', payload);
    }

    function sendCancel() {
      var instId = instIdEl ? String(instIdEl.value || '').trim() : '';
      var orderId = orderIdEl ? String(orderIdEl.value || '').trim() : '';
      if (!instId || !orderId) {
        setStatus(statusEl, 'InstId/orderId required', 'error');
        return;
      }
      var reqId = String(Date.now());
      var payload = {
        id: reqId,
        op: 'cancel-order',
        args: [
          {
            instId: instId,
            ordId: orderId
          }
        ]
      };
      monitor.logAction('cancel request sent', { instId: instId, order_id: orderId });
      send('send', payload);
    }

    function sendSubscribe(channel) {
      var payload = {
        op: 'subscribe',
        args: [
          {
            channel: channel,
            instType: 'SWAP'
          }
        ]
      };
      monitor.logAction('subscribe requested', { channel: channel });
      send('send', payload);
    }

    var connectBtn = document.getElementById('ws-trade-okx-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-okx-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-okx-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-okx-raw-send');
    var orderBtn = document.getElementById('ws-trade-okx-order');
    var cancelBtn = document.getElementById('ws-trade-okx-cancel');
    var subOrdersBtn = document.getElementById('ws-trade-okx-sub-orders');
    var subPositionsBtn = document.getElementById('ws-trade-okx-sub-positions');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (orderBtn) {
      orderBtn.addEventListener('click', function () {
        sendOrder();
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        sendCancel();
      });
    }
    if (subOrdersBtn) {
      subOrdersBtn.addEventListener('click', function () {
        sendSubscribe('orders');
      });
    }
    if (subPositionsBtn) {
      subPositionsBtn.addEventListener('click', function () {
        sendSubscribe('positions');
      });
    }
  }

  function bindWsTradeBinanceRaw() {
    var statusEl = document.getElementById('ws-trade-binance-raw-status');
    var logEl = document.getElementById('ws-trade-binance-raw-log');
    var monitorEl = document.getElementById('ws-trade-binance-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-binance-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-binance-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-binance-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-binance-raw-probe');
    var payloadEl = document.getElementById('ws-trade-binance-raw-payload');
    var symbolEl = document.getElementById('ws-trade-binance-symbol');
    var sideEl = document.getElementById('ws-trade-binance-side');
    var qtyEl = document.getElementById('ws-trade-binance-qty');
    var priceEl = document.getElementById('ws-trade-binance-price');
    var orderIdEl = document.getElementById('ws-trade-binance-order-id');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 45,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('ping');
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-binance-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; listenKey handled server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function extendListenKey() {
      setStatus(statusEl, 'Extending listenKey...', 'info');
      monitor.logAction('listenKey extend requested');
      send('extend_listen_key');
    }

    function closeListenKey() {
      setStatus(statusEl, 'Closing listenKey...', 'info');
      monitor.logAction('listenKey close requested');
      send('close_listen_key');
    }

    function restOrder(orderType) {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var side = sideEl ? String(sideEl.value || '').trim().toLowerCase() : '';
      var qty = qtyEl ? parseOptionalNumber(qtyEl.value) : null;
      var price = priceEl ? parseOptionalNumber(priceEl.value) : null;
      if (!symbol || !side || !qty) {
        setStatus(statusEl, 'Symbol/side/qty required', 'error');
        return;
      }
      if (orderType === 'limit' && (!price || price <= 0)) {
        setStatus(statusEl, 'Price required for limit order', 'error');
        return;
      }
      var payload = {
        exchange: 'binance',
        symbol: symbol,
        side: side,
        qty: qty
      };
      if (orderType === 'limit') {
        payload.price = price;
      }
      setStatus(statusEl, 'Submitting ' + orderType + ' (REST)...', 'info');
      monitor.logAction('rest order requested', { type: orderType, symbol: symbol, side: side, qty: qty });
      request(
        'POST',
        orderType === 'limit' ? '/api/manual/test/limit' : '/api/manual/test/market',
        payload,
        function (err, data) {
          if (err) {
            setStatus(statusEl, err.message, 'error');
            logLine('[rest] ' + err.message);
            return;
          }
          logLine('[rest] ' + pretty(data));
          if (data && data.order_id && orderIdEl && !orderIdEl.value) {
            orderIdEl.value = data.order_id;
          }
          if (data && data.errors && data.errors.length) {
            setStatus(statusEl, 'Completed with errors', 'error');
          } else {
            setStatus(statusEl, 'Completed', 'success');
          }
        }
      );
    }

    function restCancel() {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var orderId = orderIdEl ? String(orderIdEl.value || '').trim() : '';
      if (!symbol || !orderId) {
        setStatus(statusEl, 'Symbol/order id required', 'error');
        return;
      }
      var payload = {
        exchange: 'binance',
        symbol: symbol,
        order_id: orderId
      };
      setStatus(statusEl, 'Canceling order (REST)...', 'info');
      monitor.logAction('rest cancel requested', { symbol: symbol, order_id: orderId });
      request('POST', '/api/manual/test/cancel', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          logLine('[rest] ' + err.message);
          return;
        }
        logLine('[rest] ' + pretty(data));
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else {
          setStatus(statusEl, 'Completed', 'success');
        }
      });
    }

    var connectBtn = document.getElementById('ws-trade-binance-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-binance-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-binance-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-binance-raw-send');
    var orderBtn = document.getElementById('ws-trade-binance-order');
    var marketBtn = document.getElementById('ws-trade-binance-market');
    var cancelBtn = document.getElementById('ws-trade-binance-cancel');
    var extendBtn = document.getElementById('ws-trade-binance-extend');
    var closeBtn = document.getElementById('ws-trade-binance-close');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (extendBtn) {
      extendBtn.addEventListener('click', function () {
        extendListenKey();
      });
    }
    if (closeBtn) {
      closeBtn.addEventListener('click', function () {
        closeListenKey();
      });
    }
    if (orderBtn) {
      orderBtn.addEventListener('click', function () {
        restOrder('limit');
      });
    }
    if (marketBtn) {
      marketBtn.addEventListener('click', function () {
        restOrder('market');
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        restCancel();
      });
    }
  }

  function bindWsTradeBitgetRaw() {
    var statusEl = document.getElementById('ws-trade-bitget-raw-status');
    var logEl = document.getElementById('ws-trade-bitget-raw-log');
    var monitorEl = document.getElementById('ws-trade-bitget-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-bitget-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-bitget-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-bitget-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-bitget-raw-probe');
    var payloadEl = document.getElementById('ws-trade-bitget-raw-payload');
    var symbolEl = document.getElementById('ws-trade-bitget-symbol');
    var sideEl = document.getElementById('ws-trade-bitget-side');
    var qtyEl = document.getElementById('ws-trade-bitget-qty');
    var priceEl = document.getElementById('ws-trade-bitget-price');
    var orderIdEl = document.getElementById('ws-trade-bitget-order-id');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 45,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('send', 'ping');
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-bitget-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; auth is handled server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function sendOrder() {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var side = sideEl ? String(sideEl.value || '').trim().toLowerCase() : '';
      var qty = qtyEl ? parseOptionalNumber(qtyEl.value) : null;
      var price = priceEl ? parseOptionalNumber(priceEl.value) : null;
      if (!symbol || !side || !qty || !price) {
        setStatus(statusEl, 'Symbol/side/qty/price required', 'error');
        return;
      }
      var requestId = 'req-' + String(Date.now());
      var clientOid = 'oid-' + String(Date.now());
      var payload = {
        op: 'trade',
        args: [
          {
            id: requestId,
            instType: 'USDT-FUTURES',
            instId: symbol,
            channel: 'place-order',
            params: {
              orderType: 'limit',
              side: side,
              size: String(qty),
              force: 'gtc',
              price: String(price),
              marginCoin: 'USDT',
              marginMode: 'isolated',
              clientOid: clientOid
            }
          }
        ]
      };
      monitor.logAction('order request sent', { symbol: symbol, side: side, qty: qty, price: price });
      send('send', payload);
    }

    function sendCancel() {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var orderId = orderIdEl ? String(orderIdEl.value || '').trim() : '';
      if (!symbol || !orderId) {
        setStatus(statusEl, 'Symbol/orderId required', 'error');
        return;
      }
      var requestId = 'req-' + String(Date.now());
      var payload = {
        op: 'trade',
        args: [
          {
            id: requestId,
            instType: 'USDT-FUTURES',
            instId: symbol,
            channel: 'cancel-order',
            params: {
              orderId: orderId
            }
          }
        ]
      };
      monitor.logAction('cancel request sent', { symbol: symbol, order_id: orderId });
      send('send', payload);
    }

    function sendSubscribe(channel) {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var args = [];
      if (channel === 'account') {
        args = [{ instType: 'USDT-FUTURES', channel: 'account', coin: 'default' }];
      } else if (channel === 'positions') {
        args = [{ instType: 'USDT-FUTURES', channel: 'positions', instId: 'default' }];
      } else if (channel === 'orders' || channel === 'fill') {
        args = [
          {
            instType: 'USDT-FUTURES',
            channel: channel,
            instId: symbol || 'default'
          }
        ];
      } else {
        args = [{ instType: 'USDT-FUTURES', channel: channel }];
      }
      monitor.logAction('subscribe requested', { channel: channel, symbol: symbol || null });
      send('send', { op: 'subscribe', args: args });
    }

    var connectBtn = document.getElementById('ws-trade-bitget-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-bitget-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-bitget-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-bitget-raw-send');
    var orderBtn = document.getElementById('ws-trade-bitget-order');
    var cancelBtn = document.getElementById('ws-trade-bitget-cancel');
    var subOrdersBtn = document.getElementById('ws-trade-bitget-sub-orders');
    var subPositionsBtn = document.getElementById('ws-trade-bitget-sub-positions');
    var subFillsBtn = document.getElementById('ws-trade-bitget-sub-fills');
    var subAccountBtn = document.getElementById('ws-trade-bitget-sub-account');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (orderBtn) {
      orderBtn.addEventListener('click', function () {
        sendOrder();
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        sendCancel();
      });
    }
    if (subOrdersBtn) {
      subOrdersBtn.addEventListener('click', function () {
        sendSubscribe('orders');
      });
    }
    if (subPositionsBtn) {
      subPositionsBtn.addEventListener('click', function () {
        sendSubscribe('positions');
      });
    }
    if (subFillsBtn) {
      subFillsBtn.addEventListener('click', function () {
        sendSubscribe('fill');
      });
    }
    if (subAccountBtn) {
      subAccountBtn.addEventListener('click', function () {
        sendSubscribe('account');
      });
    }
  }

  function bindWsTradeGateRaw() {
    var statusEl = document.getElementById('ws-trade-gate-raw-status');
    var logEl = document.getElementById('ws-trade-gate-raw-log');
    var monitorEl = document.getElementById('ws-trade-gate-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-gate-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-gate-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-gate-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-gate-raw-probe');
    var payloadEl = document.getElementById('ws-trade-gate-raw-payload');
    var symbolEl = document.getElementById('ws-trade-gate-symbol');
    var sideEl = document.getElementById('ws-trade-gate-side');
    var qtyEl = document.getElementById('ws-trade-gate-qty');
    var priceEl = document.getElementById('ws-trade-gate-price');
    var orderIdEl = document.getElementById('ws-trade-gate-order-id');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 60,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('send', { time: Math.floor(Date.now() / 1000), channel: 'futures.ping' });
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-gate-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; login is handled server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function reconnect() {
      setStatus(statusEl, 'Reconnecting...', 'info');
      monitor.logAction('reconnect requested');
      disconnect();
      window.setTimeout(function () {
        connect();
      }, 200);
    }

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function sendOrder() {
      var symbol = normalizeGateSymbol(symbolEl ? symbolEl.value : '');
      var side = sideEl ? String(sideEl.value || '').trim().toLowerCase() : '';
      var qty = qtyEl ? parseOptionalNumber(qtyEl.value) : null;
      var price = priceEl ? parseOptionalNumber(priceEl.value) : null;
      if (!symbol || !side || !qty || !price) {
        setStatus(statusEl, 'Symbol/side/qty/price required', 'error');
        return;
      }
      var size = side === 'sell' ? -Math.abs(qty) : Math.abs(qty);
      var payload = {
        channel: 'futures.order_place',
        event: 'api',
        payload: {
          req_id: String(Date.now()),
          req_param: {
            contract: symbol,
            size: String(size),
            iceberg: '0',
            price: String(price),
            tif: 'gtc'
          }
        }
      };
      monitor.logAction('order request sent', { symbol: symbol, side: side, qty: qty, price: price });
      send('send', payload);
    }

    function sendCancel() {
      var orderId = orderIdEl ? String(orderIdEl.value || '').trim() : '';
      if (!orderId) {
        setStatus(statusEl, 'Order id required', 'error');
        return;
      }
      var payload = {
        channel: 'futures.order_cancel',
        event: 'api',
        payload: {
          req_id: String(Date.now()),
          req_param: {
            order_id: orderId
          }
        }
      };
      monitor.logAction('cancel request sent', { order_id: orderId });
      send('send', payload);
    }

    function sendSubscribe(channel) {
      var symbol = normalizeGateSymbol(symbolEl ? symbolEl.value : '');
      var payload = {
        channel: channel,
        event: 'subscribe',
        payload: symbol ? [symbol] : []
      };
      monitor.logAction('subscribe requested', { channel: channel, symbol: symbol || null });
      send('send', payload);
    }

    var connectBtn = document.getElementById('ws-trade-gate-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-gate-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-gate-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-gate-raw-send');
    var orderBtn = document.getElementById('ws-trade-gate-order');
    var cancelBtn = document.getElementById('ws-trade-gate-cancel');
    var subOrdersBtn = document.getElementById('ws-trade-gate-sub-orders');
    var subPositionsBtn = document.getElementById('ws-trade-gate-sub-positions');
    var subFillsBtn = document.getElementById('ws-trade-gate-sub-fills');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (orderBtn) {
      orderBtn.addEventListener('click', function () {
        sendOrder();
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        sendCancel();
      });
    }
    if (subOrdersBtn) {
      subOrdersBtn.addEventListener('click', function () {
        sendSubscribe('futures.orders');
      });
    }
    if (subPositionsBtn) {
      subPositionsBtn.addEventListener('click', function () {
        sendSubscribe('futures.positions');
      });
    }
    if (subFillsBtn) {
      subFillsBtn.addEventListener('click', function () {
        sendSubscribe('futures.usertrades');
      });
    }
  }

  function bindWsTradeKucoinRaw() {
    var statusEl = document.getElementById('ws-trade-kucoin-raw-status');
    var logEl = document.getElementById('ws-trade-kucoin-raw-log');
    var monitorEl = document.getElementById('ws-trade-kucoin-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-kucoin-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-kucoin-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-kucoin-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-kucoin-raw-probe');
    var payloadEl = document.getElementById('ws-trade-kucoin-raw-payload');
    var symbolEl = document.getElementById('ws-trade-kucoin-symbol');
    var sideEl = document.getElementById('ws-trade-kucoin-side');
    var qtyEl = document.getElementById('ws-trade-kucoin-qty');
    var priceEl = document.getElementById('ws-trade-kucoin-price');
    var orderIdEl = document.getElementById('ws-trade-kucoin-order-id');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 45,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('send', { id: 'ping-' + String(Date.now()), type: 'ping' });
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-kucoin-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; bullet-private token fetched server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function extendListenKey() {
      setStatus(statusEl, 'Extending listenKey...', 'info');
      send('extend_listen_key');
    }

    function closeListenKey() {
      setStatus(statusEl, 'Closing listenKey...', 'info');
      send('close_listen_key');
    }

    function restOrder(orderType) {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var side = sideEl ? String(sideEl.value || '').trim().toLowerCase() : '';
      var qty = qtyEl ? parseOptionalNumber(qtyEl.value) : null;
      var price = priceEl ? parseOptionalNumber(priceEl.value) : null;
      if (!symbol || !side || !qty) {
        setStatus(statusEl, 'Symbol/side/qty required', 'error');
        return;
      }
      if (orderType === 'limit' && (!price || price <= 0)) {
        setStatus(statusEl, 'Price required for limit order', 'error');
        return;
      }
      var payload = {
        exchange: 'kucoin',
        symbol: symbol,
        side: side,
        qty: qty
      };
      if (orderType === 'limit') {
        payload.price = price;
      }
      setStatus(statusEl, 'Submitting ' + orderType + ' (REST)...', 'info');
      monitor.logAction('rest order requested', { type: orderType, symbol: symbol, side: side, qty: qty });
      request(
        'POST',
        orderType === 'limit' ? '/api/manual/test/limit' : '/api/manual/test/market',
        payload,
        function (err, data) {
          if (err) {
            setStatus(statusEl, err.message, 'error');
            logLine('[rest] ' + err.message);
            return;
          }
          logLine('[rest] ' + pretty(data));
          if (data && data.order_id && orderIdEl && !orderIdEl.value) {
            orderIdEl.value = data.order_id;
          }
          if (data && data.errors && data.errors.length) {
            setStatus(statusEl, 'Completed with errors', 'error');
          } else {
            setStatus(statusEl, 'Completed', 'success');
          }
        }
      );
    }

    function restCancel() {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var orderId = orderIdEl ? String(orderIdEl.value || '').trim() : '';
      if (!symbol || !orderId) {
        setStatus(statusEl, 'Symbol/order id required', 'error');
        return;
      }
      var payload = {
        exchange: 'kucoin',
        symbol: symbol,
        order_id: orderId
      };
      setStatus(statusEl, 'Canceling order (REST)...', 'info');
      monitor.logAction('rest cancel requested', { symbol: symbol, order_id: orderId });
      request('POST', '/api/manual/test/cancel', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          logLine('[rest] ' + err.message);
          return;
        }
        logLine('[rest] ' + pretty(data));
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else {
          setStatus(statusEl, 'Completed', 'success');
        }
      });
    }

    function subscribeOrders() {
      var symbol = normalizeKucoinSymbol(symbolEl ? symbolEl.value : '');
      var topic = symbol ? '/contractMarket/tradeOrders:' + symbol : '/contractMarket/tradeOrders';
      var payload = {
        id: String(Date.now()),
        type: 'subscribe',
        topic: topic,
        privateChannel: true,
        response: true
      };
      monitor.logAction('subscribe requested', { topic: topic });
      send('send', payload);
    }

    function subscribePositions() {
      var symbol = normalizeKucoinSymbol(symbolEl ? symbolEl.value : '');
      var topic = symbol ? '/contract/position:' + symbol : '/contract/positionAll';
      var payload = {
        id: String(Date.now()),
        type: 'subscribe',
        topic: topic,
        privateChannel: true,
        response: true
      };
      monitor.logAction('subscribe requested', { topic: topic });
      send('send', payload);
    }

    function subscribeFills() {
      var payload = {
        id: String(Date.now()),
        type: 'subscribe',
        topic: '/contractAccount/wallet',
        privateChannel: true,
        response: true
      };
      monitor.logAction('subscribe requested', { topic: '/contractAccount/wallet' });
      send('send', payload);
    }

    var connectBtn = document.getElementById('ws-trade-kucoin-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-kucoin-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-kucoin-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-kucoin-raw-send');
    var orderBtn = document.getElementById('ws-trade-kucoin-order');
    var marketBtn = document.getElementById('ws-trade-kucoin-market');
    var cancelBtn = document.getElementById('ws-trade-kucoin-cancel');
    var subOrdersBtn = document.getElementById('ws-trade-kucoin-sub-orders');
    var subPositionsBtn = document.getElementById('ws-trade-kucoin-sub-positions');
    var subFillsBtn = document.getElementById('ws-trade-kucoin-sub-fills');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (orderBtn) {
      orderBtn.addEventListener('click', function () {
        restOrder('limit');
      });
    }
    if (marketBtn) {
      marketBtn.addEventListener('click', function () {
        restOrder('market');
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        restCancel();
      });
    }
    if (subOrdersBtn) {
      subOrdersBtn.addEventListener('click', function () {
        subscribeOrders();
      });
    }
    if (subPositionsBtn) {
      subPositionsBtn.addEventListener('click', function () {
        subscribePositions();
      });
    }
    if (subFillsBtn) {
      subFillsBtn.addEventListener('click', function () {
        subscribeFills();
      });
    }
  }

  function bindWsTradeBingxRaw() {
    var statusEl = document.getElementById('ws-trade-bingx-raw-status');
    var logEl = document.getElementById('ws-trade-bingx-raw-log');
    var monitorEl = document.getElementById('ws-trade-bingx-raw-monitor');
    var scriptEl = document.getElementById('ws-trade-bingx-raw-script-log');
    var silenceEl = document.getElementById('ws-trade-bingx-raw-silence');
    var autoProbeEl = document.getElementById('ws-trade-bingx-raw-auto-probe');
    var probeBtn = document.getElementById('ws-trade-bingx-raw-probe');
    var payloadEl = document.getElementById('ws-trade-bingx-raw-payload');
    var symbolEl = document.getElementById('ws-trade-bingx-symbol');
    var sideEl = document.getElementById('ws-trade-bingx-side');
    var qtyEl = document.getElementById('ws-trade-bingx-qty');
    var priceEl = document.getElementById('ws-trade-bingx-price');
    var orderIdEl = document.getElementById('ws-trade-bingx-order-id');
    var ws = null;
    var scriptLog = createScriptLogger(scriptEl);
    var monitor = createWsMonitor({
      statusEl: monitorEl,
      scriptLog: scriptLog,
      silenceEl: silenceEl,
      autoProbeEl: autoProbeEl,
      probeBtn: probeBtn,
      defaultThreshold: 90,
      sendPing: function () {
        if (!ws || ws.readyState !== 1) {
          return false;
        }
        send('send', 'Ping');
        return true;
      }
    });

    function wsUrl() {
      var proto = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
      return proto + window.location.host + '/ws/trade-bingx-raw';
    }

    function logLine(text) {
      if (!logEl) {
        return;
      }
      logEl.textContent += text + '\n';
      logEl.scrollTop = logEl.scrollHeight;
    }

    function send(action, payload) {
      if (!ws || ws.readyState !== 1) {
        setStatus(statusEl, 'Not connected', 'error');
        logLine('[client] not connected');
        return;
      }
      var message = { action: action };
      if (payload) {
        if (typeof payload === 'string') {
          message.raw = payload;
        } else {
          message.payload = payload;
        }
      }
      ws.send(JSON.stringify(message));
    }

    function connect() {
      if (ws && ws.readyState === 1) {
        return;
      }
      ws = new WebSocket(wsUrl());
      ws.onopen = function () {
        setStatus(statusEl, 'Connected', 'success');
        logLine('[client] ws_open');
        monitor.onOpen();
        monitor.logAction('connect requested; listenKey handled server-side');
        send('connect');
      };
      ws.onmessage = function (evt) {
        monitor.onLine(evt.data);
        logLine(evt.data);
      };
      ws.onclose = function () {
        setStatus(statusEl, 'Disconnected', 'info');
        logLine('[client] ws_closed');
        monitor.onClose();
      };
      ws.onerror = function () {
        setStatus(statusEl, 'WebSocket error', 'error');
        logLine('[client] ws_error');
      };
    }

    function disconnect() {
      if (!ws) {
        return;
      }
      try {
        send('close');
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

    function sendRaw() {
      var raw = payloadEl ? String(payloadEl.value || '').trim() : '';
      if (!raw) {
        setStatus(statusEl, 'Raw payload required', 'error');
        return;
      }
      monitor.logAction('raw payload sent');
      send('send', raw);
    }

    function extendListenKey() {
      setStatus(statusEl, 'Extending listenKey...', 'info');
      monitor.logAction('listenKey extend requested');
      send('extend_listen_key');
    }

    function closeListenKey() {
      setStatus(statusEl, 'Closing listenKey...', 'info');
      monitor.logAction('listenKey close requested');
      send('close_listen_key');
    }

    function restOrder(orderType) {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var side = sideEl ? String(sideEl.value || '').trim().toLowerCase() : '';
      var qty = qtyEl ? parseOptionalNumber(qtyEl.value) : null;
      var price = priceEl ? parseOptionalNumber(priceEl.value) : null;
      if (!symbol || !side || !qty) {
        setStatus(statusEl, 'Symbol/side/qty required', 'error');
        return;
      }
      if (orderType === 'limit' && (!price || price <= 0)) {
        setStatus(statusEl, 'Price required for limit order', 'error');
        return;
      }
      var payload = {
        exchange: 'bingx',
        symbol: symbol,
        side: side,
        qty: qty
      };
      if (orderType === 'limit') {
        payload.price = price;
      }
      setStatus(statusEl, 'Submitting ' + orderType + ' (REST)...', 'info');
      monitor.logAction('rest order requested', { type: orderType, symbol: symbol, side: side, qty: qty });
      request(
        'POST',
        orderType === 'limit' ? '/api/manual/test/limit' : '/api/manual/test/market',
        payload,
        function (err, data) {
          if (err) {
            setStatus(statusEl, err.message, 'error');
            logLine('[rest] ' + err.message);
            return;
          }
          logLine('[rest] ' + pretty(data));
          if (data && data.order_id && orderIdEl && !orderIdEl.value) {
            orderIdEl.value = data.order_id;
          }
          if (data && data.errors && data.errors.length) {
            setStatus(statusEl, 'Completed with errors', 'error');
          } else {
            setStatus(statusEl, 'Completed', 'success');
          }
        }
      );
    }

    function restCancel() {
      var symbol = normalizeInputSymbol(symbolEl ? symbolEl.value : '');
      var orderId = orderIdEl ? String(orderIdEl.value || '').trim() : '';
      if (!symbol || !orderId) {
        setStatus(statusEl, 'Symbol/order id required', 'error');
        return;
      }
      var payload = {
        exchange: 'bingx',
        symbol: symbol,
        order_id: orderId
      };
      setStatus(statusEl, 'Canceling order (REST)...', 'info');
      monitor.logAction('rest cancel requested', { symbol: symbol, order_id: orderId });
      request('POST', '/api/manual/test/cancel', payload, function (err, data) {
        if (err) {
          setStatus(statusEl, err.message, 'error');
          logLine('[rest] ' + err.message);
          return;
        }
        logLine('[rest] ' + pretty(data));
        if (data && data.errors && data.errors.length) {
          setStatus(statusEl, 'Completed with errors', 'error');
        } else {
          setStatus(statusEl, 'Completed', 'success');
        }
      });
    }

    var connectBtn = document.getElementById('ws-trade-bingx-raw-connect');
    var disconnectBtn = document.getElementById('ws-trade-bingx-raw-disconnect');
    var reconnectBtn = document.getElementById('ws-trade-bingx-raw-reconnect');
    var sendBtn = document.getElementById('ws-trade-bingx-raw-send');
    var orderBtn = document.getElementById('ws-trade-bingx-order');
    var marketBtn = document.getElementById('ws-trade-bingx-market');
    var cancelBtn = document.getElementById('ws-trade-bingx-cancel');
    var extendBtn = document.getElementById('ws-trade-bingx-extend');
    var closeBtn = document.getElementById('ws-trade-bingx-close');

    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        monitor.logAction('connect button pressed');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
        monitor.logAction('disconnect requested');
        disconnect();
      });
    }
    if (reconnectBtn) {
      reconnectBtn.addEventListener('click', function () {
        reconnect();
      });
    }
    if (sendBtn) {
      sendBtn.addEventListener('click', function () {
        sendRaw();
      });
    }
    if (extendBtn) {
      extendBtn.addEventListener('click', function () {
        extendListenKey();
      });
    }
    if (closeBtn) {
      closeBtn.addEventListener('click', function () {
        closeListenKey();
      });
    }
    if (orderBtn) {
      orderBtn.addEventListener('click', function () {
        restOrder('limit');
      });
    }
    if (marketBtn) {
      marketBtn.addEventListener('click', function () {
        restOrder('market');
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        restCancel();
      });
    }
  }

  document.addEventListener('DOMContentLoaded', function () {
    bind();
    bindMarginTests();
    bindFundingTests();
    bindCoinAnalysisTests();
    bindWsTest();
    bindWsTradeRaw();
    bindWsTradePrivateRaw();
    bindWsTradeOkxRaw();
    bindWsTradeBinanceRaw();
    bindWsTradeBitgetRaw();
    bindWsTradeGateRaw();
    bindWsTradeKucoinRaw();
    bindWsTradeBingxRaw();
  });
})();
