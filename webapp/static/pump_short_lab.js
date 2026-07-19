(function () {
  'use strict';

  var rootPath = window.__ROOT_PATH__ || '';
  var state = window.__PUMP_SHORT_INITIAL__ || {};
  var pollTimer = null;

  function $(id) {
    return document.getElementById(id);
  }

  function api(path, method, payload, callback) {
    var xhr = new XMLHttpRequest();
    xhr.open(method || 'GET', rootPath + path, true);
    xhr.setRequestHeader('Accept', 'application/json');
    if (payload !== null && payload !== undefined) {
      xhr.setRequestHeader('Content-Type', 'application/json');
    }
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      var data = null;
      try {
        data = xhr.responseText ? JSON.parse(xhr.responseText) : {};
      } catch (err) {
        data = { detail: xhr.responseText || String(err) };
      }
      if (xhr.status < 200 || xhr.status >= 300) {
        callback(new Error(data.detail || ('HTTP ' + xhr.status)), data);
        return;
      }
      callback(null, data);
    };
    xhr.send(payload !== null && payload !== undefined ? JSON.stringify(payload) : null);
  }

  function startPolling() {
    if (pollTimer) window.clearInterval(pollTimer);
    pollTimer = window.setInterval(refreshStatus, 2000);
  }

  function refreshStatus() {
    api('/api/pump-short/bybit/status', 'GET', null, function (err, data) {
      if (err) {
        setFormStatus(err.message, true);
        return;
      }
      state = data || {};
      render();
    });
  }

  function startRun() {
    var maxRaw = $('ps-max-symbols').value;
    var symbolsRaw = $('ps-symbols').value || '';
    var symbols = [];
    var parts = symbolsRaw.split(',');
    for (var i = 0; i < parts.length; i += 1) {
      var symbol = parts[i].replace(/\s+/g, '').toUpperCase();
      if (symbol) symbols.push(symbol);
    }
    var payload = {
      lookback_days: Number($('ps-lookback').value || 30),
      sleep_sec: Number($('ps-sleep').value || 0.8),
      max_symbols: maxRaw ? Number(maxRaw) : null,
      symbols: symbols,
      newest_first: $('ps-newest-first').checked,
      resume: $('ps-resume').checked
    };
    setFormStatus('Starting...', false);
    api('/api/pump-short/bybit/start', 'POST', payload, function (err, data) {
      if (err) {
        setFormStatus(err.message, true);
        return;
      }
      state = data || {};
      setFormStatus('Started', false);
      render();
      startPolling();
    });
  }

  function stopRun() {
    setFormStatus('Stop requested...', false);
    api('/api/pump-short/bybit/stop', 'POST', {}, function (err, data) {
      if (err) {
        setFormStatus(err.message, true);
        return;
      }
      state = data || {};
      setFormStatus('Stop requested', false);
      render();
    });
  }

  function collectShadowPayload() {
    var maxRaw = $('ps-shadow-max-symbols').value;
    var symbolsRaw = $('ps-shadow-symbols').value || '';
    var symbols = [];
    var parts = symbolsRaw.split(',');
    for (var i = 0; i < parts.length; i += 1) {
      var symbol = parts[i].replace(/\s+/g, '').toUpperCase();
      if (symbol) symbols.push(symbol);
    }
    return {
      lookback_days: Number($('ps-shadow-lookback').value || 14),
      recent_event_hours: Number($('ps-shadow-recent-hours').value || 168),
      sleep_sec: Number($('ps-shadow-sleep').value || 0.8),
      max_symbols: maxRaw ? Number(maxRaw) : 50,
      symbols: symbols,
      newest_first: true
    };
  }

  function startShadowScan() {
    var payload = collectShadowPayload();
    setShadowStatus('Starting shadow scan...', false);
    api('/api/pump-short/bybit/shadow/start', 'POST', payload, function (err, data) {
      if (err) {
        setShadowStatus(err.message, true);
        return;
      }
      state.shadow = data || {};
      setShadowStatus('Shadow scan started', false);
      render();
      startPolling();
    });
  }

  function startShadowSchedule() {
    var payload = collectShadowPayload();
    payload.interval_sec = Number($('ps-shadow-interval').value || 3600);
    payload.run_immediately = $('ps-shadow-run-now').checked;
    setShadowStatus('Starting shadow schedule...', false);
    api('/api/pump-short/bybit/shadow/schedule/start', 'POST', payload, function (err, data) {
      if (err) {
        setShadowStatus(err.message, true);
        return;
      }
      state.shadow_schedule = data || {};
      setShadowStatus('Shadow schedule started', false);
      render();
      startPolling();
    });
  }

  function stopShadowSchedule() {
    setShadowStatus('Stopping shadow schedule...', false);
    api('/api/pump-short/bybit/shadow/schedule/stop', 'POST', {}, function (err, data) {
      if (err) {
        setShadowStatus(err.message, true);
        return;
      }
      state.shadow_schedule = data || {};
      setShadowStatus('Shadow schedule stop requested', false);
      render();
      startPolling();
    });
  }

  function refreshShadowStatus() {
    api('/api/pump-short/bybit/status', 'GET', null, function (err, data) {
      if (err) {
        setShadowStatus(err.message, true);
        return;
      }
      state = data || {};
      render();
    });
  }

  function render() {
    var status = state.status || 'idle';
    var total = Number(state.total_symbols || 0);
    var currentIndex = Number(state.current_index || 0);
    var collected = Number(state.collected || 0);
    var skipped = Number(state.skipped || 0);
    var failed = Number(state.failed || 0);
    var done = collected + skipped + failed;
    var pct = total > 0 ? Math.min(100, Math.max(0, (done / total) * 100)) : 0;

    var pill = $('ps-status-pill');
    pill.textContent = status;
    pill.className = 'status-pill status-pill--' + statusClass(status);

    $('ps-progress-head').textContent = done + ' / ' + total;
    $('ps-current-head').textContent = state.current_symbol || '-';
    $('ps-requests-head').textContent = String(state.requests_made || 0);
    $('ps-event-head').textContent = state.last_event || '-';
    $('ps-collected').textContent = String(collected);
    $('ps-skipped').textContent = String(skipped);
    $('ps-failed').textContent = String(failed);
    $('ps-total').textContent = String(total);
    $('ps-progress-bar').style.width = pct.toFixed(1) + '%';

    $('ps-run-meta').textContent = JSON.stringify({
      started_at: formatMs(state.started_at_ms),
      updated_at: formatMs(state.updated_at_ms),
      finished_at: formatMs(state.finished_at_ms),
      current_index: currentIndex,
      current_symbol: state.current_symbol || null,
      requests_made: state.requests_made || 0,
      output_dir: state.config && state.config.output_dir,
      last_error: state.last_error || null
    }, null, 2);

    renderSummaries(state.latest_summaries || []);
    renderFiles(state.files || {});
    renderErrors(state.latest_errors || []);
    renderAnalysis(state.analysis || {});
    renderShadow(state.shadow || {});
    renderShadowSchedule(state.shadow_schedule || {});

    var running = status === 'running' || status === 'starting';
    $('ps-start').disabled = running;
    $('ps-stop').disabled = !running;
  }

  function statusClass(status) {
    if (status === 'complete') return 'ready';
    if (status === 'running' || status === 'starting') return 'pending';
    if (status === 'error') return 'error';
    if (status === 'stopped') return 'idle';
    return 'idle';
  }

  function renderSummaries(rows) {
    var body = $('ps-summary-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="12" class="muted">No summaries yet.</td></tr>';
      return;
    }
    rows.slice().reverse().forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.symbol),
        cell(fmt(row.age_days, 2)),
        cell(fmt(row.price_history_hours, 1)),
        cell(fmt(row.last_close, 8)),
        cell(fmt(row.return_24h_pct, 2)),
        cell(fmt(row.return_3d_pct, 2)),
        cell(fmt(row.return_7d_pct, 2)),
        cell(fmt(row.funding_sum_24h_pct, 4)),
        cell(fmt(row.oi_change_24h_pct, 2)),
        cell(fmt(row.pump_score, 1)),
        cell(fmt(row.continuation_risk_score, 1)),
        cell(row.candidate_tier)
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderFiles(files) {
    var names = Object.keys(files);
    if (!names.length) {
      $('ps-files').textContent = 'No files yet.';
      return;
    }
    var lines = [];
    names.sort().forEach(function (name) {
      var file = files[name] || {};
      lines.push(name + ': ' + (file.exists ? (formatBytes(file.size) + ', ' + formatMs(file.updated_at)) : 'missing'));
    });
    $('ps-files').textContent = lines.join('\n');
  }

  function renderErrors(rows) {
    if (!rows.length) {
      $('ps-errors').textContent = 'No errors.';
      return;
    }
    var lines = rows.slice().reverse().map(function (row) {
      return (formatMs(row.ts_ms) || '-') + ' ' + (row.symbol || '-') + ': ' + (row.error || '');
    });
    $('ps-errors').textContent = lines.join('\n');
  }

  function renderAnalysis(analysis) {
    var meta = analysis.metadata || {};
    $('ps-analysis-meta').textContent = Object.keys(meta).length ? JSON.stringify({
      schema: meta.schema || null,
      symbols_seen: meta.symbols_seen || 0,
      events: meta.events || 0,
      episodes: meta.episodes || 0,
      outcomes: meta.outcomes || 0,
      exit_outcomes: meta.exit_outcomes || 0,
      best_rules: meta.best_rules || 0,
      candidate_rule_profiles: meta.candidate_rule_profiles || 0,
      anti_overfit_report: meta.anti_overfit_report || 0,
      worst_tail_events: meta.worst_tail_events || 0,
      funding_regime_summary: meta.funding_regime_summary || 0,
      oi_regime_summary: meta.oi_regime_summary || 0,
      behavior_regime_recommendations: meta.behavior_regime_recommendations || 0,
      elapsed_sec: meta.elapsed_sec || null
    }, null, 2) : 'No analysis report yet.';
    renderAnalysisFiles(analysis.files || {});
    renderBehaviorRecommendations(analysis.behavior_regime_recommendations || []);
    renderCandidateProfiles(analysis.candidate_profiles || []);
    renderAntiOverfit(analysis.anti_overfit || []);
    renderBestRules(analysis.best_rules || []);
    renderWorstTails(analysis.worst_tail_events || []);
  }

  function renderShadow(shadow) {
    var meta = shadow.metadata || shadow.metadata === null ? (shadow.metadata || {}) : {};
    var running = shadow.status === 'running' || shadow.status === 'starting';
    var schedule = state.shadow_schedule || {};
    var scheduleRunning = schedule.status === 'running' || schedule.status === 'starting' || schedule.status === 'waiting' || schedule.status === 'stopping';
    $('ps-shadow-start').disabled = running || scheduleRunning;
    $('ps-shadow-meta').textContent = JSON.stringify({
      status: shadow.status || 'idle',
      started_at: formatMs(shadow.started_at_ms),
      updated_at: formatMs(shadow.updated_at_ms),
      finished_at: formatMs(shadow.finished_at_ms),
      symbols_seen: meta.symbols_seen || 0,
      rows: meta.rows || 0,
      entry_candidates: meta.entry_candidates || 0,
      watchlist: meta.watchlist || 0,
      blocked: meta.blocked || 0,
      errors: meta.errors || 0,
      paper_positions: meta.paper_positions || 0,
      paper_open_positions: meta.paper_open_positions || 0,
      paper_closed_positions: meta.paper_closed_positions || 0,
      paper_events: meta.paper_events || 0,
      requests_made: meta.requests_made || 0,
      last_error: shadow.last_error || null
    }, null, 2);
    renderShadowFiles(shadow.files || {});
    renderShadowRows(shadow.latest_rows || []);
    renderPaperPositions((shadow.paper && shadow.paper.positions) || []);
  }

  function renderShadowSchedule(schedule) {
    var status = schedule.status || 'idle';
    var running = status === 'running' || status === 'starting' || status === 'waiting' || status === 'stopping';
    $('ps-shadow-schedule-start').disabled = running || (state.shadow && (state.shadow.status === 'running' || state.shadow.status === 'starting'));
    $('ps-shadow-schedule-stop').disabled = !running;
    $('ps-shadow-schedule-meta').textContent = JSON.stringify({
      status: status,
      started_at: formatMs(schedule.started_at_ms),
      updated_at: formatMs(schedule.updated_at_ms),
      finished_at: formatMs(schedule.finished_at_ms),
      last_run_started_at: formatMs(schedule.last_run_started_at_ms),
      last_run_finished_at: formatMs(schedule.last_run_finished_at_ms),
      next_run_at: formatMs(schedule.next_run_at_ms),
      runs_started: schedule.runs_started || 0,
      runs_completed: schedule.runs_completed || 0,
      runs_failed: schedule.runs_failed || 0,
      interval_sec: schedule.config && schedule.config.interval_sec,
      max_symbols: schedule.config && schedule.config.max_symbols,
      lookback_days: schedule.config && schedule.config.lookback_days,
      recent_event_hours: schedule.config && schedule.config.recent_event_hours,
      last_event: schedule.last_event || null,
      last_error: schedule.last_error || null
    }, null, 2);
  }

  function renderShadowFiles(files) {
    var names = Object.keys(files);
    if (!names.length) {
      $('ps-shadow-files').textContent = 'No shadow files yet.';
      return;
    }
    var lines = [];
    names.sort().forEach(function (name) {
      var file = files[name] || {};
      lines.push(name + ': ' + (file.exists ? (formatBytes(file.size) + ', ' + formatMs(file.updated_at)) : 'missing'));
    });
    $('ps-shadow-files').textContent = lines.join('\n');
  }

  function renderShadowRows(rows) {
    var body = $('ps-shadow-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="11" class="muted">No shadow scan rows yet.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.status),
        cell(row.symbol),
        cell(row.matched_profile),
        cell(row.matched_entry_strategy),
        cell(row.matched_exit_strategy),
        cell(fmt(row.trigger_pump_pct, 1)),
        cell(fmt(row.pullback_from_high_pct, 1)),
        cell(fmt(row.oi_change_24h_pct, 1)),
        cell(fmt(row.long_ratio, 3)),
        cell(fmt(row.funding_prev_24h_pct, 3)),
        cell(row.reason)
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderPaperPositions(rows) {
    var body = $('ps-paper-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="13" class="muted">No paper positions yet.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.status),
        cell(row.symbol),
        cell(row.profile),
        cell(row.entry_strategy),
        cell(row.exit_strategy),
        cell(fmt(row.entry_price, 8)),
        cell(fmt(row.current_price, 8)),
        cell(fmt(row.remaining_weight, 2)),
        cell(fmt(row.realized_net_pct, 2)),
        cell(fmt(row.unrealized_net_pct, 2)),
        cell(fmt(row.combined_net_pct, 2)),
        cell(fmt(row.mfe_pct, 1) + ' / ' + fmt(row.mae_pct, 1)),
        cell(row.exit_reason)
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderAnalysisFiles(files) {
    var names = Object.keys(files);
    if (!names.length) {
      $('ps-analysis-files').textContent = 'No analysis files yet.';
      return;
    }
    var lines = [];
    names.sort().forEach(function (name) {
      var file = files[name] || {};
      lines.push(name + ': ' + (file.exists ? (formatBytes(file.size) + ', ' + formatMs(file.updated_at)) : 'missing'));
    });
    $('ps-analysis-files').textContent = lines.join('\n');
  }

  function renderBehaviorRecommendations(rows) {
    var body = $('ps-behavior-recommendation-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="12" class="muted">No behavior-regime recommendations yet.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.funding_regime),
        cell(row.oi_regime),
        cell(row.rank),
        cell(row.entry_strategy),
        cell(row.exit_strategy),
        cell(row.n),
        cell(fmt(row.regime_rule_score, 2)),
        cell(fmt(row.win_pct, 1)),
        cell(fmt(row.avg_net_pct, 1)),
        cell(fmt(row.median_net_pct, 1)),
        cell(fmt(row.p90_mae_pct, 1)),
        cell(row.regime_note)
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderCandidateProfiles(rows) {
    var body = $('ps-candidate-profile-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="12" class="muted">No candidate profiles yet.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.profile),
        cell(row.profile_rank),
        cell(row.entry_strategy),
        cell(row.exit_strategy),
        cell(row.n),
        cell(fmt(row.profile_score, 2)),
        cell(fmt(row.win_pct, 1)),
        cell(fmt(row.avg_net_pct, 1)),
        cell(fmt(row.median_net_pct, 1)),
        cell(fmt(row.p90_mae_pct, 1)),
        cell(row.anti_overfit_status),
        cell(row.selection_note)
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderAntiOverfit(rows) {
    var body = $('ps-anti-overfit-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="11" class="muted">No anti-overfit report yet.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.anti_overfit_status),
        cell(row.entry_strategy),
        cell(row.exit_strategy),
        cell(fmt(row.anti_overfit_score, 2)),
        cell(fmt(row.time_test_avg_net_pct, 1)),
        cell(fmt(row.time_test_median_net_pct, 1)),
        cell(fmt(row.time_test_p90_mae_pct, 1)),
        cell(fmt(row.symbol_holdout_avg_net_pct, 1)),
        cell(fmt(row.symbol_holdout_p90_mae_pct, 1)),
        cell(fmt(row.top_positive_symbol_share_pct, 1)),
        cell((row.best_symbol || '-') + ' / ' + (row.worst_symbol || '-'))
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderBestRules(rows) {
    var body = $('ps-best-rules-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="10" class="muted">No best-rules report yet.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.rank),
        cell(row.entry_strategy),
        cell(row.exit_strategy),
        cell(row.n),
        cell(fmt(row.rule_score, 2)),
        cell(fmt(row.win_pct, 1)),
        cell(fmt(row.avg_net_pct, 1)),
        cell(fmt(row.median_net_pct, 1)),
        cell(fmt(row.p90_mae_pct, 1)),
        cell(row.risk_note)
      ].join('');
      body.appendChild(tr);
    });
  }

  function renderWorstTails(rows) {
    var body = $('ps-worst-tail-body');
    body.innerHTML = '';
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="9" class="muted">No worst-tail report yet.</td></tr>';
      return;
    }
    rows.forEach(function (row) {
      var tr = document.createElement('tr');
      tr.innerHTML = [
        cell(row.symbol),
        cell(row.entry_strategy),
        cell(row.exit_strategy),
        cell(fmt(row.net_exit_pct, 1)),
        cell(fmt(row.mae_pct, 1)),
        cell(fmt(row.pump_pct, 1)),
        cell(fmt(row.oi_change_24h_pct, 1)),
        cell(fmt(row.long_ratio, 3)),
        cell(row.exit_reason)
      ].join('');
      body.appendChild(tr);
    });
  }

  function cell(value) {
    var text = value === null || value === undefined || value === '' ? '-' : String(value);
    return '<td>' + escapeHtml(text) + '</td>';
  }

  function fmt(value, digits) {
    if (value === null || value === undefined || value === '') return '-';
    var num = Number(value);
    if (!isFinite(num)) return String(value);
    return num.toFixed(digits);
  }

  function formatMs(value) {
    if (!value) return null;
    var date = new Date(Number(value));
    if (isNaN(date.getTime())) return null;
    return date.toISOString().replace('T', ' ').replace('.000Z', ' UTC');
  }

  function formatBytes(value) {
    var size = Number(value || 0);
    if (size < 1024) return size + ' B';
    if (size < 1024 * 1024) return (size / 1024).toFixed(1) + ' KB';
    return (size / 1024 / 1024).toFixed(2) + ' MB';
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function setFormStatus(text, isError) {
    var node = $('ps-form-status');
    node.textContent = text || '';
    node.className = 'settings-status' + (isError ? ' settings-status--error' : '');
  }

  function setShadowStatus(text, isError) {
    var node = $('ps-shadow-form-status');
    node.textContent = text || '';
    node.className = 'settings-status' + (isError ? ' settings-status--error' : '');
  }

  document.addEventListener('DOMContentLoaded', function () {
    $('ps-start').addEventListener('click', startRun);
    $('ps-refresh').addEventListener('click', refreshStatus);
    $('ps-stop').addEventListener('click', stopRun);
    $('ps-shadow-start').addEventListener('click', startShadowScan);
    $('ps-shadow-refresh').addEventListener('click', refreshShadowStatus);
    $('ps-shadow-schedule-start').addEventListener('click', startShadowSchedule);
    $('ps-shadow-schedule-stop').addEventListener('click', stopShadowSchedule);
    render();
    startPolling();
  });
})();
