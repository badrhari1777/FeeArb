(function () {
  'use strict';

  var MAX_PAIRS = 3;
  var ACTIVE_KEY = 'spread_monitor.active_count';

  function formatNumber(value, digits) {
    var number = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(number)) {
      return '-';
    }
    var places = typeof digits === 'number' ? digits : 4;
    return number.toFixed(places);
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

  function storageKey(index, field) {
    return 'spread_monitor.' + index + '.' + field;
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

  function SpreadPair(index) {
    var prefix = 'spread-' + index;
    var card = document.getElementById(prefix + '-card');
    var symbolEl = document.getElementById(prefix + '-symbol');
    var longEl = document.getElementById(prefix + '-long');
    var shortEl = document.getElementById(prefix + '-short');
    var minEl = document.getElementById(prefix + '-spread-min');
    var maxEl = document.getElementById(prefix + '-spread-max');
    var depthEl = document.getElementById(prefix + '-depth');
    var showBookEl = document.getElementById(prefix + '-show-book');
    var liveEl = document.getElementById(prefix + '-live');
    var bookEl = document.getElementById(prefix + '-book');

    var ws = null;
    var reconnectTimer = null;
    var visible = true;

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
      return {
        action: 'subscribe',
        symbol: symbolEl ? symbolEl.value.trim() : '',
        long_exchange: longEl ? longEl.value : '',
        short_exchange: shortEl ? shortEl.value : '',
        spread_min_pct: parseOptionalNumber(minEl ? minEl.value : null),
        spread_max_pct: parseOptionalNumber(maxEl ? maxEl.value : null),
        include_orderbook: showBookEl ? !!showBookEl.checked : true,
        orderbook_depth: parseInt(depthEl ? depthEl.value : 5, 10) || 5
      };
    }

    function subscribe() {
      if (!visible) {
        return;
      }
      if (!ws || ws.readyState !== 1) {
        return;
      }
      var payload = currentPayload();
      if (!payload.symbol || !payload.long_exchange || !payload.short_exchange) {
        setLive('Live spread: -', '');
        if (bookEl) {
          bookEl.textContent = 'Live orderbook: waiting for data';
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
      if (!visible) {
        return;
      }
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
          var showBook = showBookEl ? !!showBookEl.checked : true;
          if (showBook && longBook.bids && longBook.asks && shortBook.bids && shortBook.asks) {
            bookEl.textContent = formatLiveBooks(
              longBook,
              shortBook,
              data.long_exchange,
              data.short_exchange,
              data.subscriptions || {}
            );
          } else {
            bookEl.textContent = showBook ? 'Live orderbook: waiting for data' : 'Live orderbook: off';
          }
        }
      };
      ws.onclose = function () {
        if (!visible) {
          return;
        }
        if (reconnectTimer) {
          return;
        }
        reconnectTimer = window.setTimeout(function () {
          reconnectTimer = null;
          connect();
        }, 1000);
      };
    }

    function close() {
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (ws) {
        try {
          ws.close();
        } catch (_err) {
        }
        ws = null;
      }
    }

    function persist() {
      if (symbolEl) {
        localStorage.setItem(storageKey(index, 'symbol'), symbolEl.value || '');
      }
      if (longEl) {
        localStorage.setItem(storageKey(index, 'long'), longEl.value || '');
      }
      if (shortEl) {
        localStorage.setItem(storageKey(index, 'short'), shortEl.value || '');
      }
      if (minEl) {
        localStorage.setItem(storageKey(index, 'min'), minEl.value || '');
      }
      if (maxEl) {
        localStorage.setItem(storageKey(index, 'max'), maxEl.value || '');
      }
      if (depthEl) {
        localStorage.setItem(storageKey(index, 'depth'), depthEl.value || '');
      }
      if (showBookEl) {
        localStorage.setItem(storageKey(index, 'show_book'), showBookEl.checked ? 'true' : 'false');
      }
    }

    function hydrate() {
      if (symbolEl) {
        var saved = localStorage.getItem(storageKey(index, 'symbol'));
        if (saved !== null) {
          symbolEl.value = saved;
        }
      }
      if (longEl) {
        var savedLong = localStorage.getItem(storageKey(index, 'long'));
        if (savedLong !== null) {
          longEl.value = savedLong;
        }
      }
      if (shortEl) {
        var savedShort = localStorage.getItem(storageKey(index, 'short'));
        if (savedShort !== null) {
          shortEl.value = savedShort;
        }
      }
      if (minEl) {
        var savedMin = localStorage.getItem(storageKey(index, 'min'));
        if (savedMin !== null) {
          minEl.value = savedMin;
        }
      }
      if (maxEl) {
        var savedMax = localStorage.getItem(storageKey(index, 'max'));
        if (savedMax !== null) {
          maxEl.value = savedMax;
        }
      }
      if (depthEl) {
        var savedDepth = localStorage.getItem(storageKey(index, 'depth'));
        if (savedDepth !== null) {
          depthEl.value = savedDepth;
        }
      }
      if (showBookEl) {
        var savedShow = localStorage.getItem(storageKey(index, 'show_book'));
        if (savedShow !== null) {
          showBookEl.checked = savedShow === 'true';
        }
      }
    }

    function setVisible(nextVisible) {
      visible = !!nextVisible;
      if (card) {
        card.style.display = visible ? '' : 'none';
      }
      if (!visible) {
        close();
      } else {
        connect();
        subscribe();
      }
    }

    function bind() {
      var inputs = [symbolEl, longEl, shortEl, minEl, maxEl, depthEl, showBookEl];
      inputs.forEach(function (el) {
        if (!el) {
          return;
        }
        el.addEventListener('change', function () {
          persist();
          subscribe();
        });
        el.addEventListener('keyup', function () {
          persist();
          subscribe();
        });
      });
    }

    hydrate();
    bind();
    connect();

    return {
      setVisible: setVisible,
      subscribe: subscribe
    };
  }

  function getActiveCount() {
    var stored = parseInt(localStorage.getItem(ACTIVE_KEY), 10);
    if (isNaN(stored) || stored < 1) {
      return 1;
    }
    return Math.min(MAX_PAIRS, stored);
  }

  function setActiveCount(value) {
    var clamped = Math.max(1, Math.min(MAX_PAIRS, value));
    localStorage.setItem(ACTIVE_KEY, String(clamped));
    return clamped;
  }

  var pairs = [];
  for (var i = 1; i <= MAX_PAIRS; i += 1) {
    pairs.push(new SpreadPair(i));
  }

  function refreshVisibility() {
    var count = getActiveCount();
    pairs.forEach(function (pair, idx) {
      pair.setVisible(idx + 1 <= count);
    });
  }

  var addBtn = document.getElementById('spread-add');
  if (addBtn) {
    addBtn.addEventListener('click', function () {
      var next = setActiveCount(getActiveCount() + 1);
      refreshVisibility();
      if (next >= MAX_PAIRS) {
        addBtn.disabled = true;
      }
    });
  }

  var removeButtons = document.querySelectorAll('.spread-remove');
  removeButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      var index = parseInt(btn.getAttribute('data-index'), 10);
      if (isNaN(index)) {
        return;
      }
      var next = setActiveCount(index - 1);
      refreshVisibility();
      if (addBtn) {
        addBtn.disabled = next >= MAX_PAIRS;
      }
    });
  });

  refreshVisibility();
  if (addBtn) {
    addBtn.disabled = getActiveCount() >= MAX_PAIRS;
  }
})();
