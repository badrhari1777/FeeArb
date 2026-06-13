(function () {
  'use strict';

  var root = window.__ROOT_PATH__ || '';
  var initial = window.__STRATEGIES_INITIAL__ || {};
  var state = initial.payload || {};
  var exchanges = initial.exchanges || [];
  var form = document.getElementById('strategy-form');
  var stepsEl = document.getElementById('strategy-steps');
  var statusEl = document.getElementById('strategy-status');

  function esc(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }

  function request(method, path, body) {
    return fetch(root + path, {
      method: method,
      headers: body ? {'Content-Type': 'application/json'} : {},
      body: body ? JSON.stringify(body) : undefined
    }).then(function (response) {
      return response.json().then(function (data) {
        if (!response.ok) throw new Error(data.detail || 'HTTP ' + response.status);
        return data;
      });
    });
  }

  function fillExchanges() {
    ['strategy-long', 'strategy-short'].forEach(function (id, index) {
      var select = document.getElementById(id);
      select.innerHTML = exchanges.map(function (name) {
        return '<option value="' + esc(name) + '">' + esc(name.toUpperCase()) + '</option>';
      }).join('');
      if (select.options.length > index) select.selectedIndex = index;
    });
  }

  function addStep(values) {
    values = values || {};
    var index = stepsEl.children.length + 1;
    var row = document.createElement('div');
    row.className = 'strategy-step';
    row.innerHTML =
      '<div class="strategy-step-title"><strong>Ступень ' + index + '</strong>' +
      (index > 1 ? '<button type="button" class="button button--danger strategy-remove">Удалить</button>' : '') + '</div>' +
      '<label>Объём, USDT<input class="step-notional" type="number" min="0" step="any" value="' + esc(values.notional_usd || 100) + '" required></label>' +
      '<label>Spread, %<input class="step-spread" type="number" step="any" value="' + esc(values.spread_target_pct !== undefined ? values.spread_target_pct : -2) + '" required></label>' +
      '<label>Funding diff не хуже, %<input class="step-funding" type="number" step="any" placeholder="любое" value="' + esc(values.funding_min_pct) + '"></label>' +
      '<details class="strategy-advanced"><summary>Дополнительно</summary>' +
      '<label>Количество монет<input class="step-qty" type="number" min="0" step="any"></label>' +
      '<label>Процент позиции<input class="step-percent" type="number" min="0" max="100" step="any"></label>' +
      '<label>Лимит чанка, USDT<input class="step-chunk" type="number" min="0" step="any" placeholder="авто"></label>' +
      '<label>Runtime, сек<input class="step-runtime" type="number" min="1" step="1" value="120"></label>' +
      '</details>';
    stepsEl.appendChild(row);
    var remove = row.querySelector('.strategy-remove');
    if (remove) remove.addEventListener('click', function () { row.remove(); renumberSteps(); });
    syncStepMode();
  }

  function renumberSteps() {
    Array.prototype.forEach.call(stepsEl.children, function (row, index) {
      row.querySelector('strong').textContent = 'Ступень ' + (index + 1);
    });
  }

  function syncStepMode() {
    var isExit = document.getElementById('strategy-type').value === 'exit_ladder';
    Array.prototype.forEach.call(stepsEl.querySelectorAll('.step-percent'), function (input) {
      input.closest('label').hidden = !isExit;
    });
  }

  function numberValue(row, selector) {
    var raw = row.querySelector(selector).value.trim();
    return raw === '' ? null : Number(raw);
  }

  function formPayload() {
    return {
      type: document.getElementById('strategy-type').value,
      symbol: document.getElementById('strategy-symbol').value.trim().toUpperCase(),
      long_exchange: document.getElementById('strategy-long').value,
      short_exchange: document.getElementById('strategy-short').value,
      enabled: true,
      steps: Array.prototype.map.call(stepsEl.children, function (row) {
        var qty = numberValue(row, '.step-qty');
        var percent = numberValue(row, '.step-percent');
        return {
          notional_usd: qty || percent ? null : numberValue(row, '.step-notional'),
          qty: qty,
          percent: percent,
          spread_target_pct: numberValue(row, '.step-spread'),
          funding_min_pct: numberValue(row, '.step-funding'),
          chunk_notional_usd: numberValue(row, '.step-chunk'),
          max_runtime_sec: numberValue(row, '.step-runtime')
        };
      })
    };
  }

  function fmt(value, digits) {
    return value === null || value === undefined ? '-' : Number(value).toFixed(digits === undefined ? 4 : digits);
  }

  function render() {
    var strategies = state.strategies || [];
    document.getElementById('strategy-count').textContent = strategies.filter(function (item) { return item.enabled; }).length;
    var running = state.running;
    document.getElementById('strategy-worker').textContent = running ? 'занят' : 'свободен';
    document.getElementById('strategy-running').innerHTML = running
      ? '<strong>' + esc(running.action || '-') + '</strong> exec=' + esc(running.execution_id) +
        ' strategy=' + esc(running.strategy_id || 'legacy') + ' stage=' + esc(running.stage || '-') +
        '<br>' + esc(running.message || '')
      : 'Worker свободен.';

    document.getElementById('strategy-list').innerHTML = strategies.length ? strategies.map(function (strategy) {
      var steps = strategy.steps || [];
      var active = steps.filter(function (step) {
        return step.status !== 'completed' && step.status !== 'completed_with_dust';
      })[0];
      var rows = steps.map(function (step) {
        return '<tr class="' + (active && active.id === step.id ? 'strategy-current-step' : '') + '">' +
          '<td>' + (Number(step.index) + 1) + '</td><td>' + esc(step.status) + '</td>' +
          '<td>' + fmt(step.spread_target_pct) + '%</td><td>' + fmt(step.funding_min_pct) + '</td>' +
          '<td>' + fmt(step.target_qty, 6) + '</td><td>' + fmt(step.filled_qty, 6) + '</td>' +
          '<td>' + fmt(step.remaining_qty, 6) + '</td></tr>';
      }).join('');
      return '<article class="strategy-card"><div class="panel-heading"><div><h3>' + esc(strategy.name) + '</h3>' +
        '<p class="muted">' + esc(strategy.symbol) + ' · ' + esc(strategy.long_exchange.toUpperCase()) +
        ' long / ' + esc(strategy.short_exchange.toUpperCase()) + ' short · ' + esc(strategy.status) + '</p></div>' +
        '<div class="actions"><button class="button button--ghost strategy-toggle" data-id="' + esc(strategy.id) +
        '" data-enabled="' + (strategy.enabled ? '1' : '0') + '">' + (strategy.enabled ? 'Пауза' : 'Продолжить') + '</button>' +
        '<button class="button button--danger strategy-delete" data-id="' + esc(strategy.id) + '">Удалить</button></div></div>' +
        '<div class="table-wrapper"><table class="table"><thead><tr><th>#</th><th>Статус</th><th>Spread</th><th>Funding</th>' +
        '<th>Цель qty</th><th>Исполнено</th><th>Осталось</th></tr></thead><tbody>' + rows + '</tbody></table></div></article>';
    }).join('') : '<p class="muted">Стратегий пока нет.</p>';

    var queue = state.queue || [];
    document.getElementById('strategy-queue').innerHTML = queue.length ? queue.map(function (item, index) {
      return '<tr><td>' + (index + 1) + '</td><td>' + esc(item.strategy_id) + '</td><td>' + esc(item.step_id) +
        '</td><td>' + esc(item.action) + '</td><td>' + esc(item.priority) + '</td><td>' + fmt(item.edge) + '</td></tr>';
    }).join('') : '<tr><td colspan="6" class="muted">Очередь пуста.</td></tr>';

    Array.prototype.forEach.call(document.querySelectorAll('.strategy-toggle'), function (button) {
      button.onclick = function () {
        request('POST', '/api/strategies/' + encodeURIComponent(button.dataset.id) +
          (button.dataset.enabled === '1' ? '/pause' : '/resume')).then(update).catch(showError);
      };
    });
    Array.prototype.forEach.call(document.querySelectorAll('.strategy-delete'), function (button) {
      button.onclick = function () {
        if (!window.confirm('Удалить стратегию?')) return;
        request('DELETE', '/api/strategies/' + encodeURIComponent(button.dataset.id)).then(update).catch(showError);
      };
    });
  }

  function update(data) { state = data || {}; render(); }
  function showError(error) { statusEl.textContent = error.message || String(error); }
  function refresh() { request('GET', '/api/strategies').then(update).catch(showError); }

  function renderPreflight(data) {
    var rows = data && data.steps ? data.steps : [];
    document.getElementById('strategy-preflight-result').innerHTML = rows.map(function (item) {
      var plan = item.plan || {};
      var errors = Array.isArray(plan.errors) ? plan.errors : [];
      return '<div><strong>Ступень ' + esc(item.step) + ':</strong> ' +
        (errors.length ? '<span class="value-neg">' + esc(errors.join('; ')) + '</span>' :
          'qty=' + fmt(plan.recommended_qty || item.requested_qty, 8) +
          ', chunk=' + fmt(plan.recommended_chunk_qty || plan.min_chunk_qty, 8) +
          ', spread=' + fmt(plan.spread_pct, 4) + '%') + '</div>';
    }).join('') || 'Нет результатов.';
  }

  fillExchanges();
  addStep();
  render();
  document.getElementById('strategy-add-step').addEventListener('click', function () { addStep(); });
  document.getElementById('strategy-preflight').addEventListener('click', function () {
    statusEl.textContent = 'Выполняется Preflight...';
    request('POST', '/api/strategies/preflight', formPayload()).then(function (data) {
      statusEl.textContent = 'Preflight завершен без размещения ордеров.';
      renderPreflight(data);
    }).catch(showError);
  });
  document.getElementById('strategy-type').addEventListener('change', syncStepMode);
  form.addEventListener('submit', function (event) {
    event.preventDefault();
    statusEl.textContent = 'Сохранение...';
    request('POST', '/api/strategies', formPayload()).then(function (data) {
      statusEl.textContent = 'Live стратегия включена.';
      update(data);
    }).catch(showError);
  });
  window.setInterval(refresh, 2000);
}());
