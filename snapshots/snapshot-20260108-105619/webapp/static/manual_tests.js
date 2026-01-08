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

  document.addEventListener('DOMContentLoaded', bind);
})();
