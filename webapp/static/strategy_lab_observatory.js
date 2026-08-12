(function () {
  'use strict';

  var rootPath = window.__ROOT_PATH__ || '';
  var state = window.__STRATEGY_LAB_OBSERVATORY__ || {};

  function byId(id) { return document.getElementById(id); }

  function escapeHtml(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function formatDate(value) {
    if (!value) return '-';
    var date = new Date(value);
    return isNaN(date.getTime()) ? '-' : date.toLocaleString();
  }

  function formatPercent(value) {
    var number = Number(value);
    return isNaN(number) ? '-' : (number * 100).toFixed(4) + '%';
  }

  function formatCoverage(value) {
    var number = Number(value);
    return isNaN(number) ? '-' : number.toFixed(1) + '%';
  }

  function request(path, method, payload, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open(method || 'GET', rootPath + path, true);
    xhr.setRequestHeader('Accept', 'application/json');
    if (payload !== null && payload !== undefined) xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      var data = {};
      try { data = xhr.responseText ? JSON.parse(xhr.responseText) : {}; }
      catch (error) { data = { detail: xhr.responseText || String(error) }; }
      if (xhr.status < 200 || xhr.status >= 300) {
        callback(new Error(data.detail || ('HTTP ' + xhr.status)), data);
        return;
      }
      callback(null, data);
    };
    xhr.send(payload !== null && payload !== undefined ? JSON.stringify(payload) : null);
  }

  function setAction(message, error) {
    var element = byId('sl-action-status');
    if (!element) return;
    element.className = 'settings-status' + (error ? ' settings-status--error' : ' settings-status--success');
    element.textContent = message || '';
  }

  function renderSources() {
    var body = byId('sl-source-body');
    if (!body) return;
    var sources = state.sources || {};
    var names = ['coinglass', 'arbitragescanner'];
    var html = '';
    for (var i = 0; i < names.length; i += 1) {
      var row = sources[names[i]] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(names[i]) + '</td>' +
        '<td>' + escapeHtml(row.status || 'never_run') + '</td>' +
        '<td>' + escapeHtml(row.raw_count || 0) + '</td>' +
        '<td>' + escapeHtml(row.eligible_count || 0) + '</td>' +
        '<td>' + escapeHtml(row.quarantined_count || 0) + '</td>' +
        '<td>' + escapeHtml(formatDate(row.last_success_at)) + '</td>' +
        '<td>' + (row.last_good_used ? 'yes' : 'no') + '</td>' +
        '<td>' + escapeHtml(row.error || '-') + '</td>' +
      '</tr>';
    }
    body.innerHTML = html;
  }

  function renderCandidates() {
    var body = byId('sl-candidate-body');
    var empty = byId('sl-candidate-empty');
    if (!body) return;
    var rows = Array.isArray(state.candidates) ? state.candidates : [];
    var verificationRows = (((state.registry || {}).verification) || []);
    var verification = {};
    for (var v = 0; v < verificationRows.length; v += 1) {
      var checked = verificationRows[v] || {};
      var symbol = checked.canonical_symbol || '';
      if (!verification[symbol]) verification[symbol] = { venues: [], eligible: false, vetoes: [] };
      verification[symbol].eligible = verification[symbol].eligible || !!checked.eligible_for_observation;
      var venues = checked.verified_venues || [];
      for (var x = 0; x < venues.length; x += 1) {
        if (verification[symbol].venues.indexOf(venues[x]) < 0) verification[symbol].venues.push(venues[x]);
      }
      verification[symbol].vetoes = verification[symbol].vetoes.concat(checked.vetoes || []);
    }
    var feedRows = (((state.feed_probe || {}).report || {}).observations) || [];
    var feedCounts = {};
    for (var f = 0; f < feedRows.length; f += 1) {
      var feedSymbol = feedRows[f].canonical_symbol || '';
      feedCounts[feedSymbol] = (feedCounts[feedSymbol] || 0) + 1;
    }
    var html = '';
    for (var i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      var registryCheck = verification[row.canonical_symbol] || {};
      var registryLabel = registryCheck.venues && registryCheck.venues.length
        ? registryCheck.venues.join(', ') + (registryCheck.eligible ? '' : ' (veto)') : '-';
      html += '<tr>' +
        '<td>' + (i + 1) + '</td>' +
        '<td>' + escapeHtml(row.canonical_symbol) + '</td>' +
        '<td>' + escapeHtml(row.monitoring_priority) + '</td>' +
        '<td>' + escapeHtml((row.source_tags || []).join(', ')) + '</td>' +
        '<td>' + (row.source_overlap ? 'yes' : 'no') + '</td>' +
        '<td>' + escapeHtml(row.coinglass_rank === null ? '-' : row.coinglass_rank) + '</td>' +
        '<td>' + escapeHtml(formatPercent(row.funding_dispersion)) + '</td>' +
        '<td>' + escapeHtml(row.long_exchange || '-') + '</td>' +
        '<td>' + escapeHtml(row.short_exchange || '-') + '</td>' +
        '<td>' + escapeHtml(registryLabel) + '</td>' +
        '<td>' + escapeHtml(feedCounts[row.canonical_symbol] || 0) + ' venues</td>' +
        '<td>NO (research)</td>' +
      '</tr>';
    }
    body.innerHTML = html;
    if (empty) empty.style.display = rows.length ? 'none' : '';
  }

  function renderRegistry() {
    var registry = state.registry || {};
    var snapshot = registry.snapshot || {};
    var statuses = snapshot.source_status || {};
    var counts = snapshot.contract_count || {};
    var names = ['binance', 'bybit', 'okx', 'kucoin', 'gate'];
    var body = byId('sl-registry-body');
    var html = '';
    for (var i = 0; i < names.length; i += 1) {
      var row = statuses[names[i]] || {};
      html += '<tr>' +
        '<td>' + escapeHtml(names[i]) + '</td>' +
        '<td>' + escapeHtml(row.status || 'not checked') + '</td>' +
        '<td>' + escapeHtml(counts[names[i]] === undefined ? '-' : counts[names[i]]) + '</td>' +
        '<td>' + escapeHtml(row.mode || '-') + '</td>' +
        '<td>' + escapeHtml(row.error || '-') + '</td>' +
      '</tr>';
    }
    if (body) body.innerHTML = html;
    var summary = byId('sl-registry-summary');
    if (summary) {
      summary.textContent = 'Статус: ' + (registry.status || 'never_run') +
        '; кандидатов подтверждено: ' + (registry.eligible_candidate_count || 0) +
        '; последний успех: ' + formatDate(registry.last_success_at) +
        (registry.last_good_used ? '; показан last-good' : '') +
        (registry.error ? '; ошибка: ' + registry.error : '');
    }
  }

  function renderFeed() {
    var feed = state.feed_probe || {};
    var report = feed.report || {};
    var quality = feed.quality || {};
    var coverage = report.venue_coverage || {};
    var statuses = report.venue_status || {};
    var names = ['binance', 'bybit', 'okx', 'kucoin', 'gate'];
    var body = byId('sl-feed-body');
    var html = '';
    for (var i = 0; i < names.length; i += 1) {
      var venue = statuses[names[i]] || {};
      var venueCoverage = coverage[names[i]] || {};
      var errors = Number(venue.parse_errors || 0) + Number(venue.subscription_errors || 0) + Number(venue.rest_errors || 0);
      html += '<tr>' +
        '<td>' + escapeHtml(names[i]) + '</td>' +
        '<td>' + escapeHtml(venue.status || 'not checked') + '</td>' +
        '<td>' + escapeHtml((venueCoverage.observed || 0) + '/' + (venueCoverage.expected || 0)) + '</td>' +
        '<td>' + escapeHtml(formatCoverage(venueCoverage.coverage_pct)) + '</td>' +
        '<td>' + escapeHtml(venue.connections || 0) + '</td>' +
        '<td>' + escapeHtml(venue.updates || 0) + '</td>' +
        '<td>' + escapeHtml(errors + (venue.error ? ': ' + venue.error : '')) + '</td>' +
      '</tr>';
    }
    if (body) body.innerHTML = html;
    var summary = byId('sl-feed-summary');
    if (summary) {
      summary.textContent = 'Статус: ' + (feed.status || 'never_run') +
        '; QA: ' + (quality.ready_for_bounded_research ? 'bounded-ready' : 'not ready') +
        '; pairs: ' + (report.observation_count || 0) + '/' + (((report.plan || {}).expected_pairs) || 0) +
        ' (' + formatCoverage(report.pair_coverage_pct) + ')' +
        '; symbols on 2+ venues: ' + (report.symbols_with_two_venues || 0) +
        '; max freshness: ' + (((report.freshness_ms || {}).max) === null || ((report.freshness_ms || {}).max) === undefined ? '-' : report.freshness_ms.max + ' ms') +
        (feed.last_good_used ? '; показан last-good' : '') +
        (feed.error ? '; ошибка: ' + feed.error : '');
    }
    if (byId('sl-feed-missing')) byId('sl-feed-missing').textContent = (report.missing_pairs || []).join(', ') || '-';
  }

  function render() {
    var running = !!state.running;
    var status = byId('sl-status');
    if (status) {
      status.textContent = running ? 'running' : 'idle';
      status.className = 'status-pill status-pill--' + (running ? 'pending' : 'ready');
    }
    if (byId('sl-operation')) byId('sl-operation').textContent = state.running_operation || '-';
    if (byId('sl-mode')) byId('sl-mode').textContent = state.mode || 'research_only_no_trading';
    if (byId('sl-candidate-count')) byId('sl-candidate-count').textContent = state.candidate_count || 0;
    if (byId('sl-updated-at')) byId('sl-updated-at').textContent = formatDate(state.updated_at);
    var buttons = document.querySelectorAll('#sl-refresh-all, #sl-refresh-coinglass, #sl-refresh-arbitragescanner, #sl-registry-refresh, #sl-feed-probe');
    for (var i = 0; i < buttons.length; i += 1) buttons[i].disabled = running;
    if (byId('sl-registry-refresh')) byId('sl-registry-refresh').disabled = running || !(state.candidate_count > 0);
    if (byId('sl-feed-probe')) byId('sl-feed-probe').disabled = running || (state.registry || {}).status !== 'fresh';
    renderSources();
    renderRegistry();
    renderFeed();
    renderCandidates();
  }

  function refreshStatus() {
    request('/api/strategy-lab/observatory', 'GET', null, function (error, data) {
      if (error) { setAction(error.message, true); return; }
      state = data || {};
      render();
    });
  }

  function runRefresh(sources) {
    state.running = true;
    render();
    setAction('Сбор запущен. Coinglass может занять до минуты.', false);
    request('/api/strategy-lab/observatory/refresh', 'POST', { sources: sources }, function (error, data) {
      if (error) {
        state.running = false;
        render();
        setAction(error.message, true);
        return;
      }
      state = data || {};
      render();
      setAction('Bounded refresh завершён. Проверьте статусы источников и карантин.', false);
    });
  }

  function runRegistryRefresh() {
    state.running = true;
    state.running_operation = 'registry_refresh';
    render();
    setAction('Instrument Registry refresh запущен.', false);
    request('/api/strategy-lab/observatory/registry/refresh', 'POST', null, function (error, data) {
      if (error) {
        state.running = false;
        state.running_operation = null;
        render();
        setAction(error.message, true);
        return;
      }
      state = data || {};
      render();
      setAction('Registry refresh завершён. Проверьте source status и verification.', false);
    });
  }

  function runFeedProbe() {
    var duration = Number((byId('sl-feed-duration') || {}).value || 12);
    var maxSymbols = Number((byId('sl-feed-symbols') || {}).value || 5);
    state.running = true;
    state.running_operation = 'feed_probe';
    render();
    setAction('Bounded own-feed probe запущен; торговые действия недоступны.', false);
    request('/api/strategy-lab/observatory/feed/probe', 'POST', {
      duration_sec: duration,
      max_symbols: maxSymbols
    }, function (error, data) {
      if (error) {
        state.running = false;
        state.running_operation = null;
        render();
        setAction(error.message, true);
        return;
      }
      state = data || {};
      render();
      setAction('Feed probe завершён. Проверьте coverage, freshness и missing pairs.', false);
    });
  }

  function init() {
    render();
    if (byId('sl-refresh-all')) byId('sl-refresh-all').onclick = function () { runRefresh(null); };
    if (byId('sl-refresh-coinglass')) byId('sl-refresh-coinglass').onclick = function () { runRefresh(['coinglass']); };
    if (byId('sl-refresh-arbitragescanner')) byId('sl-refresh-arbitragescanner').onclick = function () { runRefresh(['arbitragescanner']); };
    if (byId('sl-registry-refresh')) byId('sl-registry-refresh').onclick = runRegistryRefresh;
    if (byId('sl-feed-probe')) byId('sl-feed-probe').onclick = runFeedProbe;
    if (byId('sl-status-refresh')) byId('sl-status-refresh').onclick = refreshStatus;
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
}());
