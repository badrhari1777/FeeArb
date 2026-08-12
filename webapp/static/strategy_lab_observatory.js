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
    var html = '';
    for (var i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
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
        '<td>NO (research)</td>' +
      '</tr>';
    }
    body.innerHTML = html;
    if (empty) empty.style.display = rows.length ? 'none' : '';
  }

  function render() {
    var running = !!state.running;
    var status = byId('sl-status');
    if (status) {
      status.textContent = running ? 'running' : 'idle';
      status.className = 'status-pill status-pill--' + (running ? 'pending' : 'ready');
    }
    if (byId('sl-mode')) byId('sl-mode').textContent = state.mode || 'research_only_no_trading';
    if (byId('sl-candidate-count')) byId('sl-candidate-count').textContent = state.candidate_count || 0;
    if (byId('sl-updated-at')) byId('sl-updated-at').textContent = formatDate(state.updated_at);
    var buttons = document.querySelectorAll('#sl-refresh-all, #sl-refresh-coinglass, #sl-refresh-arbitragescanner');
    for (var i = 0; i < buttons.length; i += 1) buttons[i].disabled = running;
    renderSources();
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

  function init() {
    render();
    if (byId('sl-refresh-all')) byId('sl-refresh-all').onclick = function () { runRefresh(null); };
    if (byId('sl-refresh-coinglass')) byId('sl-refresh-coinglass').onclick = function () { runRefresh(['coinglass']); };
    if (byId('sl-refresh-arbitragescanner')) byId('sl-refresh-arbitragescanner').onclick = function () { runRefresh(['arbitragescanner']); };
    if (byId('sl-status-refresh')) byId('sl-status-refresh').onclick = refreshStatus;
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
}());
