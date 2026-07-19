(function () {
  'use strict';

  var rootPath = window.__ROOT_PATH__ || '';
  var initial = window.__AUTO_ARB_INITIAL__ || {};
  var lastAnalysis = null;

  var el = {
    form: document.getElementById('aa-form'),
    symbol: document.getElementById('aa-symbol'),
    longExchange: document.getElementById('aa-long'),
    shortExchange: document.getElementById('aa-short'),
    setupMode: document.getElementById('aa-setup-mode'),
    budgetMode: document.getElementById('aa-budget-mode'),
    maxQty: document.getElementById('aa-max-qty'),
    maxNotional: document.getElementById('aa-max-notional'),
    qtyWrap: document.getElementById('aa-qty-wrap'),
    notionalWrap: document.getElementById('aa-notional-wrap'),
    rangeStart: document.getElementById('aa-range-start'),
    rangeEnd: document.getElementById('aa-range-end'),
    rangeStartLabel: document.getElementById('aa-range-start-label'),
    rangeEndLabel: document.getElementById('aa-range-end-label'),
    rangeHint: document.getElementById('aa-range-hint'),
    levelCount: document.getElementById('aa-level-count'),
    exitGap: document.getElementById('aa-exit-gap'),
    slippage: document.getElementById('aa-slippage'),
    confirmSamples: document.getElementById('aa-confirm-samples'),
    save: document.getElementById('aa-save'),
    status: document.getElementById('aa-status'),
    summary: document.getElementById('aa-summary'),
    levels: document.getElementById('aa-levels'),
    warnings: document.getElementById('aa-warnings'),
    rules: document.getElementById('aa-rules'),
    ruleCount: document.getElementById('aa-rule-count'),
    updated: document.getElementById('aa-updated')
  };

  function escapeHtml(value) {
    return String(value === undefined || value === null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function numberValue(input) {
    if (!input || input.value === '') {
      return null;
    }
    var value = Number(input.value);
    return isFinite(value) ? value : null;
  }

  function fmt(value, digits) {
    if (value === null || value === undefined || !isFinite(Number(value))) {
      return '-';
    }
    return Number(value).toFixed(digits === undefined ? 3 : digits);
  }

  function request(method, path, body) {
    var options = { method: method, headers: {} };
    if (body !== undefined && body !== null) {
      options.headers['Content-Type'] = 'application/json';
      options.body = JSON.stringify(body);
    }
    return fetch(rootPath + path, options).then(function (response) {
      return response.json().catch(function () { return {}; }).then(function (data) {
        if (!response.ok) {
          throw new Error(data.detail || ('HTTP ' + response.status));
        }
        return data;
      });
    });
  }

  function formPayload() {
    var setupMode = el.setupMode ? el.setupMode.value : 'entry_range';
    var rangeStart = numberValue(el.rangeStart);
    var rangeEnd = numberValue(el.rangeEnd);
    var payload = {
      symbol: el.symbol.value.trim().toUpperCase(),
      long_exchange: el.longExchange.value,
      short_exchange: el.shortExchange.value,
      setup_mode: setupMode,
      budget_mode: el.budgetMode.value,
      range_start_pct: rangeStart,
      range_end_pct: rangeEnd,
      max_slippage_bps: numberValue(el.slippage),
      confirm_samples: numberValue(el.confirmSamples),
      liquidity_safety_factor: 0.70,
      enabled: true,
      live: true
    };
    if (setupMode === 'existing_position_exit_range' || setupMode === 'adopt_existing_full_grid') {
      payload.exit_range_start_pct = rangeStart;
      payload.exit_range_end_pct = rangeEnd;
    }
    if (setupMode === 'existing_position_exit_range') {
      payload.budget_mode = 'qty';
    } else {
      if (payload.budget_mode === 'notional') {
        payload.max_notional = numberValue(el.maxNotional);
      } else {
        payload.max_qty = numberValue(el.maxQty);
      }
    }
    var count = numberValue(el.levelCount);
    if (count !== null) {
      payload.level_count = count;
    }
    return payload;
  }

  function updateAutoGapPreview() {
    var start = numberValue(el.rangeStart);
    var end = numberValue(el.rangeEnd);
    var count = numberValue(el.levelCount);
    if (start === null || end === null || count === null || count < 2) {
      el.exitGap.value = '';
      return;
    }
    el.exitGap.value = fmt(Math.abs(start - end) / (count - 1), 6);
  }

  function updateSetupMode() {
    var setupMode = el.setupMode ? el.setupMode.value : 'entry_range';
    var useExitRange = setupMode === 'existing_position_exit_range' || setupMode === 'adopt_existing_full_grid';
    var useCurrentAsMax = setupMode === 'existing_position_exit_range';
    var budgetLabel = el.budgetMode && el.budgetMode.closest ? el.budgetMode.closest('label') : null;
    if (budgetLabel) {
      budgetLabel.hidden = useCurrentAsMax;
    }
    el.qtyWrap.hidden = useCurrentAsMax || el.budgetMode.value === 'notional';
    el.notionalWrap.hidden = useCurrentAsMax || el.budgetMode.value !== 'notional';
    if (el.rangeStartLabel) {
      el.rangeStartLabel.textContent = useExitRange ? 'Первый выход, %' : 'Первый вход, %';
    }
    if (el.rangeEndLabel) {
      el.rangeEndLabel.textContent = useExitRange ? 'Последний выход, %' : 'Последний вход, %';
    }
    if (el.rangeHint) {
      if (setupMode === 'existing_position_exit_range') {
        el.rangeHint.textContent = 'Берётся текущая hedged-позиция; Grid продаёт на росте spread и докупает обратно ниже.';
      } else if (setupMode === 'adopt_existing_full_grid') {
        el.rangeHint.textContent = 'Текущая hedged-позиция принимается внутрь полной сетки; максимум монет остается лимитом стратегии.';
      } else {
        el.rangeHint.textContent = 'Entry spread must move lower to add position.';
      }
    }
    updateAutoGapPreview();
  }

  function setBusy(message) {
    el.status.textContent = message || '';
  }

  function renderAnalysis(data) {
    lastAnalysis = data;
    var config = data.config || {};
    var levels = config.levels || [];
    var fit = config.existing_position_fit || null;
    var fitSummary = '';
    if (fit && fit.existing_qty) {
      fitSummary = ' | real position: <strong>' + fmt(fit.existing_qty, 6) + '</strong>' +
        ' -> ' + (fit.adoption_partial ? 'partial ' : '') +
        'level <strong>' + escapeHtml(fit.level || 0) + '</strong>';
      if (fit.adoption_partial) {
        fitSummary += ' / sized from real qty';
      } else {
        fitSummary += ' / diff ' + fmt(fit.diff_qty, 6) +
          ' <= tol ' + fmt(fit.tolerance_qty, 6);
      }
    }
    el.save.disabled = !levels.length;
    el.levelCount.value = config.level_count || '';
    el.exitGap.value = config.exit_gap_pct || '';
    el.summary.className = 'auto-arb-summary';
    el.summary.innerHTML =
      '<strong>' + escapeHtml(config.symbol || '') + '</strong> ' +
      escapeHtml(config.long_exchange || '') + ' long / ' +
      escapeHtml(config.short_exchange || '') + ' short' +
      ' | уровней: <strong>' + escapeHtml(config.level_count || '-') + '</strong>' +
      ' | чанк: <strong>' + fmt(config.chunk_qty, 6) + '</strong>' +
      ' | safe chunk dry run: ' + fmt(data.safe_chunk_qty, 6) +
      ' | шаг: ' + fmt(data.grid_step_pct, 4) + '%' +
      ' | exit gap auto: <strong>' + fmt(config.exit_gap_pct, 4) + '%</strong>' +
      fitSummary;
    el.levels.innerHTML = levels.map(function (level) {
      var currentEntry = data.live_spreads && data.live_spreads.entry_spread_pct;
      var status = currentEntry !== null && currentEntry !== undefined &&
        Number(currentEntry) <= Number(level.entry_spread_pct)
        ? 'Условие входа достигнуто'
        : 'Ожидает вход';
      return '<tr>' +
        '<td>' + escapeHtml(level.level) + '</td>' +
        '<td>' + escapeHtml(level.entry_condition || '') + '</td>' +
        '<td>' + escapeHtml(level.entry_action || '') + '</td>' +
        '<td>' + escapeHtml(level.exit_condition || '') + '</td>' +
        '<td>' + escapeHtml(level.exit_action || '') + '</td>' +
        '<td>' + fmt(level.qty, 8) + '</td>' +
        '<td>' + fmt(level.chunk_notional_estimate, 2) + '</td>' +
        '<td>' + fmt(level.cumulative_qty, 8) + '</td>' +
        '<td>' + fmt(level.cumulative_notional_estimate, 2) + '</td>' +
        '<td>' + escapeHtml(status) + '</td>' +
        '</tr>';
    }).join('');
    var warnings = data.warnings || [];
    el.warnings.innerHTML = warnings.map(function (warning) {
      return '<p class="cell-note value-negative">' + escapeHtml(warning) + '</p>';
    }).join('');
  }

  function renderRules(payload) {
    var rules = payload.rules || [];
    el.ruleCount.textContent = String(rules.length);
    el.updated.textContent = payload.generated_at || '-';
    if (!rules.length) {
      el.rules.innerHTML = '<p class="muted">Стратегий пока нет.</p>';
      return;
    }
    el.rules.innerHTML = rules.map(function (rule) {
      var enabled = !!rule.enabled;
      var mode = rule.mode || 'shadow';
      var currentLevel = mode === 'live' ? (rule.live_level || 0) : (rule.shadow_level || 0);
      var currentQty = mode === 'live' ? (rule.actual_hedged_qty || 0) : (rule.shadow_qty || 0);
      var transition = rule.pending_transition || {};
      var modeAction = mode === 'live'
        ? ''
        : '<button class="button" data-action="arm-live" data-id="' + escapeHtml(rule.id) + '">Включить Live</button>';
      var toggleAction = enabled ? 'pause' : 'arm-live';
      var toggleLabel = enabled ? 'Пауза' : 'Проверить и включить';
      return '<article class="auto-arb-rule-card">' +
        '<div class="auto-arb-rule-head">' +
          '<div><strong>' + escapeHtml(rule.symbol) + '</strong>' +
          '<div class="cell-note">' + escapeHtml(rule.long_exchange) + ' long / ' +
          escapeHtml(rule.short_exchange) + ' short</div></div>' +
          '<span class="status-pill status-pill--' + (enabled ? 'ready' : 'idle') + '">' +
          escapeHtml(mode.toUpperCase() + ' · ' + (rule.status || (enabled ? 'active' : 'paused'))) + '</span>' +
        '</div>' +
        '<div class="auto-arb-metrics">' +
          '<div><span>Уровень</span><strong>' + escapeHtml(currentLevel) + ' / ' +
          escapeHtml(rule.level_count || 0) + '</strong></div>' +
          '<div><span>Фактический qty</span><strong>' + fmt(currentQty, 8) + '</strong></div>' +
          '<div><span>Принято при старте</span><strong>ур. ' +
          escapeHtml(rule.adopted_level || 0) + ' · ' + fmt(rule.adopted_qty || 0, 8) + '</strong></div>' +
          '<div><span>Текущая ступень</span><strong>' +
          escapeHtml(transition.action || '-') + ' · остаток ' +
          fmt(transition.remaining_qty, 8) + '</strong></div>' +
          '<div><span>Entry spread</span><strong>' + fmt(rule.live_entry_spread_pct, 4) + '%</strong></div>' +
          '<div><span>Exit spread</span><strong>' + fmt(rule.live_exit_spread_pct, 4) + '%</strong></div>' +
          '<div><span>Диапазон</span><strong>' + fmt(rule.range_start_pct, 2) + '% ... ' +
          fmt(rule.range_end_pct, 2) + '%</strong></div>' +
          '<div><span>Exit gap auto</span><strong>' + fmt(rule.exit_gap_pct, 4) + '%</strong></div>' +
          '<div><span>Pending</span><strong>' + escapeHtml(rule.pending_action || '-') + ' ' +
          escapeHtml(rule.pending_samples || 0) + '/' + escapeHtml(rule.confirm_samples || 1) + '</strong></div>' +
          '<div><span>Active execution</span><strong>' + escapeHtml(rule.active_execution_id || '-') + '</strong></div>' +
          '<div><span>Блокировка</span><strong>' + escapeHtml(rule.blocked_reason || '-') + '</strong></div>' +
        '</div>' +
        '<div class="auto-arb-actions">' +
          modeAction +
          '<button class="button button--ghost" data-action="' + toggleAction +
          '" data-id="' + escapeHtml(rule.id) + '">' + toggleLabel + '</button>' +
          '<button class="button button--danger" data-action="delete" data-id="' +
          escapeHtml(rule.id) + '">Удалить</button>' +
        '</div>' +
      '</article>';
    }).join('');
  }

  function refreshRules() {
    request('GET', '/api/auto-arb').then(renderRules).catch(function (error) {
      setBusy(error.message);
    });
  }

  function populateExchanges() {
    var exchanges = initial.exchanges || [];
    var options = exchanges.map(function (name) {
      return '<option value="' + escapeHtml(name) + '">' + escapeHtml(name) + '</option>';
    }).join('');
    el.longExchange.innerHTML = options;
    el.shortExchange.innerHTML = options;
    if (exchanges.indexOf('bybit') >= 0) {
      el.longExchange.value = 'bybit';
    }
    if (exchanges.indexOf('kucoin') >= 0) {
      el.shortExchange.value = 'kucoin';
    } else if (exchanges.length > 1) {
      el.shortExchange.selectedIndex = 1;
    }
  }

  el.budgetMode.addEventListener('change', function () {
    updateSetupMode();
  });
  if (el.setupMode) {
    el.setupMode.addEventListener('change', updateSetupMode);
  }
  [el.rangeStart, el.rangeEnd, el.levelCount].forEach(function (input) {
    input.addEventListener('input', updateAutoGapPreview);
    input.addEventListener('change', updateAutoGapPreview);
  });

  el.form.addEventListener('submit', function (event) {
    event.preventDefault();
    lastAnalysis = null;
    el.save.disabled = true;
    setBusy('Выполняется Dry Run...');
    request('POST', '/api/auto-arb/analyze', formPayload()).then(function (data) {
      renderAnalysis(data);
      setBusy('Анализ готов. Проверьте уровни перед запуском Live.');
    }).catch(function (error) {
      setBusy(error.message);
    });
  });

  el.save.addEventListener('click', function () {
    if (!lastAnalysis) {
      return;
    }
    el.save.disabled = true;
    setBusy('Сохраняется и проверяется Live-стратегия...');
    request('POST', '/api/auto-arb/rules', formPayload()).then(function (data) {
      var rule = data && data.rule ? data.rule : {};
      if (!rule.id) {
        throw new Error('Rule was saved but id is missing.');
      }
      return request(
        'POST',
        '/api/auto-arb/rules/' + encodeURIComponent(rule.id) + '/arm-live',
        { confirmation: 'LIVE ' + rule.id }
      );
    }).then(function () {
      setBusy('Live-сетка включена.');
      refreshRules();
    }).catch(function (error) {
      setBusy(error.message);
      el.save.disabled = false;
    });
  });

  el.rules.addEventListener('click', function (event) {
    var button = event.target.closest('button[data-action]');
    if (!button) {
      return;
    }
    var id = button.getAttribute('data-id');
    var action = button.getAttribute('data-action');
    if (action === 'delete' && !window.confirm('Удалить Grid-стратегию?')) {
      return;
    }
    if (action === 'arm-live') {
      var confirmation = window.prompt(
        'Live Grid размещает реальные ордера. Для подтверждения введите: LIVE ' + id
      );
      if (confirmation === null) {
        return;
      }
      request(
        'POST',
        '/api/auto-arb/rules/' + encodeURIComponent(id) + '/arm-live',
        { confirmation: confirmation }
      ).then(function () {
        setBusy('Live Grid включён. Сетка ожидает следующий подтверждённый уровень.');
        refreshRules();
      }).catch(function (error) {
        setBusy(error.message);
      });
      return;
    }
    if (action === 'shadow' && !window.confirm(
      'Перевести стратегию в Shadow и поставить на паузу? Реальная позиция останется открытой.'
    )) {
      return;
    }
    var method = action === 'delete' ? 'DELETE' : 'POST';
    var path = action === 'delete'
      ? '/api/auto-arb/rules/' + encodeURIComponent(id)
      : '/api/auto-arb/rules/' + encodeURIComponent(id) + '/' + action;
    request(method, path).then(refreshRules).catch(function (error) {
      setBusy(error.message);
    });
  });

  populateExchanges();
  updateSetupMode();
  updateAutoGapPreview();
  renderRules({ rules: initial.rules || [], generated_at: '-' });
  refreshRules();
  window.setInterval(refreshRules, 3000);
})();
