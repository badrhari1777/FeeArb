/**
 * Manual exchange test helpers.
 */
(function () {
  'use strict';

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

    var connectBtn = document.getElementById('ws-connect');
    var disconnectBtn = document.getElementById('ws-disconnect');
    if (connectBtn) {
      connectBtn.addEventListener('click', function () {
        setStatus(statusEl, 'Connecting...', 'info');
        connect();
      });
    }
    if (disconnectBtn) {
      disconnectBtn.addEventListener('click', function () {
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

  document.addEventListener('DOMContentLoaded', function () {
    bind();
    bindWsTest();
  });
})();
