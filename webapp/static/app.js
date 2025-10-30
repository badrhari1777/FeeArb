(() => {
  const defaultState = {
    status: "idle",
    refresh_interval: 300,
    last_error: null,
    last_updated: null,
    snapshot: null,
    refresh_in_progress: false,
    events: [],
    exchange_status: [],
  };

  const initialState = {
    ...defaultState,
    ...(window.__INITIAL_STATE__ || {}),
  };

  let currentState = initialState;
  let pollIntervalSeconds = Math.max(
    Number(initialState.refresh_interval) || 300,
    30,
  );
  let pollingTimer = null;
  let pollingInFlight = false;

  const elements = {
    generatedAt: document.getElementById("generated-at"),
    lastUpdated: document.getElementById("last-updated"),
    screenerSource: document.getElementById("screener-source"),
    coinglassSource: document.getElementById("coinglass-source"),
    opportunityCount: document.getElementById("opportunity-count"),
    statusPill: document.getElementById("status-pill"),
    lastError: document.getElementById("last-error"),
    lastProgress: document.getElementById("last-progress"),
    exchangeSummary: document.getElementById("exchange-summary"),
    screenerTable: document.getElementById("screener-table")?.querySelector("tbody"),
    coinglassTable: document.getElementById("coinglass-table")?.querySelector("tbody"),
    universeTable: document.getElementById("universe-table-body"),
    opportunityTable: document.getElementById("opportunity-table-body"),
    messagesPanel: document.getElementById("messages"),
    refreshButton: document.getElementById("refresh-button"),
    hint: document.querySelector(".hint"),
    emptyState: document.getElementById("empty-state"),
    exchangeTable: document.getElementById("exchange-status-body"),
    eventLog: document.getElementById("event-log"),
    eventEmpty: document.getElementById("event-empty"),
  };

  const escapeHtml = (value) => {
    if (value === null || value === undefined) {
      return "";
    }
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  };

  const formatPercent = (value, digits = 2) => {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return "-";
    }
    return (value * 100).toFixed(digits);
  };

  const formatNumber = (value, digits = 4) => {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return "-";
    }
    if (Math.abs(value) >= 1000) {
      return value.toLocaleString(undefined, { maximumFractionDigits: digits });
    }
    return Number(value).toFixed(digits);
  };

  const formatTime = (value) => {
    if (!value) {
      return "-";
    }
    try {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) {
        return value;
      }
      return date.toISOString().replace("T", " ").replace("Z", " UTC");
    } catch {
      return value;
    }
  };

  const truncate = (value, limit = 96) => {
    if (!value) {
      return "";
    }
    const text = String(value);
    if (text.length <= limit) {
      return text;
    }
    return `${text.slice(0, limit - 1)}…`;
  };

  const updateStatusPill = (statusText) => {
    if (!elements.statusPill) {
      return;
    }
    const text = statusText || "idle";
    elements.statusPill.textContent = text;
    const slug = text.toString().toLowerCase().replace(/[^a-z0-9]+/g, "-");
    elements.statusPill.className = `status-pill status-pill--${slug}`;
  };

  const toggleEmptyState = (show) => {
    if (!elements.emptyState) {
      return;
    }
    elements.emptyState.style.display = show ? "" : "none";
  };

  const renderScreener = (rows = []) => {
    if (!elements.screenerTable) {
      return;
    }
    const limited = rows.slice(0, 10);
    elements.screenerTable.innerHTML = limited
      .map(
        (row) => `
          <tr>
            <td>${escapeHtml(row.symbol)}</td>
            <td>${formatPercent(row.spread ?? 0, 4)}</td>
            <td>${escapeHtml(row.long_exchange)}</td>
            <td>${formatPercent(row.long_fee ?? 0, 4)}</td>
            <td>${escapeHtml(row.short_exchange)}</td>
            <td>${formatPercent(row.short_fee ?? 0, 4)}</td>
          </tr>
        `,
      )
      .join("");
  };

  const renderCoinglass = (rows = []) => {
    if (!elements.coinglassTable) {
      return;
    }
    const limited = rows.slice(0, 10);
    elements.coinglassTable.innerHTML = limited
      .map(
        (row) => `
          <tr>
            <td>${escapeHtml(row.ranking)}</td>
            <td>${escapeHtml(row.symbol)}</td>
            <td>${escapeHtml(row.pair)}</td>
            <td>${escapeHtml(row.long_exchange)}</td>
            <td>${escapeHtml(row.short_exchange)}</td>
            <td>${escapeHtml(
              Number(row.net_funding_rate_percent ?? 0).toFixed(3),
            )}</td>
            <td>${escapeHtml(Number(row.apr_percent ?? 0).toFixed(2))}</td>
            <td>${escapeHtml(
              Number(row.spread_rate_percent ?? 0).toFixed(3),
            )}</td>
          </tr>
        `,
      )
      .join("");
  };

  const renderUniverse = (rows = []) => {
    if (!elements.universeTable) {
      return;
    }
    elements.universeTable.innerHTML = rows
      .map(
        (row) => `
          <tr>
            <td>${escapeHtml(row.symbol)}</td>
            <td>${escapeHtml(row.sources)}</td>
          </tr>
        `,
      )
      .join("");
  };

  const renderOpportunities = (rows = []) => {
    if (!elements.opportunityTable) {
      return;
    }
    elements.opportunityTable.innerHTML = rows
      .map(
        (row) => `
          <tr>
            <td>${escapeHtml(row.symbol)}</td>
            <td>${escapeHtml(row.long_exchange)}</td>
            <td>${formatPercent(row.long_rate, 3)}</td>
            <td>${formatNumber(row.long_bid, 4)}</td>
            <td>${formatNumber(row.long_ask, 4)}</td>
            <td>${escapeHtml(formatTime(row.long_next_funding))}</td>
            <td>${escapeHtml(row.short_exchange)}</td>
            <td>${formatPercent(row.short_rate, 3)}</td>
            <td>${formatNumber(row.short_bid, 4)}</td>
            <td>${formatNumber(row.short_ask, 4)}</td>
            <td>${escapeHtml(formatTime(row.short_next_funding))}</td>
            <td>${formatPercent(row.spread, 3)}</td>
            <td>${formatPercent(row.price_diff_pct, 3)}</td>
            <td>${formatPercent(row.effective_spread, 3)}</td>
            <td>${escapeHtml(row.participants ?? "-")}</td>
          </tr>
        `,
      )
      .join("");

    if (elements.opportunityCount) {
      elements.opportunityCount.textContent = rows.length;
    }
  };

  const renderExchangeStatus = (rows = []) => {
    if (!elements.exchangeTable) {
      return;
    }
    if (!rows || rows.length === 0) {
      elements.exchangeTable.innerHTML =
        '<tr><td colspan="4" class="muted">No exchange data yet.</td></tr>';
      return;
    }
    elements.exchangeTable.innerHTML = rows
      .map((row) => {
        const exchange = escapeHtml(row.exchange ?? row.name ?? "-");
        const statusValue = (row.status ?? "unknown").toString();
        const slug = statusValue.toLowerCase().replace(/[^a-z0-9]+/g, "-");
        const count = row.count ?? "-";
        const message = row.message || row.error || "";
        return `
          <tr>
            <td>${exchange}</td>
            <td><span class="status-chip status-chip--${slug}">${escapeHtml(statusValue)}</span></td>
            <td>${escapeHtml(count)}</td>
            <td>${escapeHtml(message || "-")}</td>
          </tr>
        `;
      })
      .join("");
  };

  const renderEvents = (events = []) => {
    if (!elements.eventLog) {
      return;
    }
    if (!events || events.length === 0) {
      elements.eventLog.innerHTML = "";
      if (elements.eventEmpty) {
        elements.eventEmpty.style.display = "";
      }
      return;
    }
    const items = events.slice(-50);
    elements.eventLog.innerHTML = items
      .map((event) => {
        const timestamp = formatTime(event.timestamp);
        const message = event.payload?.message || event.event;
        return `
          <li class="event-log__item">
            <span class="event-log__time">${escapeHtml(timestamp)}</span>
            <span class="event-log__message">${escapeHtml(message)}</span>
          </li>
        `;
      })
      .join("");
    if (elements.eventEmpty) {
      elements.eventEmpty.style.display = "none";
    }
  };

  const ensureMessagesPanel = () => {
    if (elements.messagesPanel) {
      return elements.messagesPanel;
    }
    const panel = document.createElement("section");
    panel.className = "panel panel--alert";
    panel.id = "messages";
    panel.innerHTML = "<h2>Status Messages</h2><ul></ul>";
    document.querySelector(".content")?.prepend(panel);
    elements.messagesPanel = panel;
    return panel;
  };

  const renderMessages = (messages = []) => {
    if (!messages || messages.length === 0) {
      if (elements.messagesPanel) {
        elements.messagesPanel.style.display = "none";
      }
      return;
    }
    const panel = ensureMessagesPanel();
    panel.style.display = "";
    const list = panel.querySelector("ul");
    if (list) {
      list.innerHTML = messages
        .map((message) => `<li>${escapeHtml(message)}</li>`)
        .join("");
    }
  };

  const renderSnapshotData = (snapshot) => {
    if (!snapshot) {
      renderScreener([]);
      renderCoinglass([]);
      renderUniverse([]);
      renderOpportunities([]);
      return;
    }
    renderScreener(snapshot.screener_rows ?? []);
    renderCoinglass(snapshot.coinglass_rows ?? []);
    renderUniverse(snapshot.universe ?? []);
    renderOpportunities(snapshot.opportunities ?? []);
  };

  const buildMessages = (state) => {
    const messages = [];
    if (!state.snapshot && state.status === "pending") {
      messages.push(
        "Initial data is being collected. This may take a couple of minutes.",
      );
    }
    if (state.last_error) {
      messages.push(`Last refresh error: ${state.last_error}`);
    }
    if (state.snapshot?.messages?.length) {
      messages.push(...state.snapshot.messages);
    }
    return messages;
  };

  const updateMetadata = (state) => {
    const snapshot = state.snapshot;
    const generated = formatTime(snapshot?.generated_at);
    if (elements.generatedAt) {
      elements.generatedAt.textContent = generated;
    }

    const lastUpdated =
      formatTime(state.last_updated) || generated || "-";
    if (elements.lastUpdated) {
      elements.lastUpdated.textContent = lastUpdated;
    }

    if (elements.screenerSource) {
      const value = snapshot?.screener_from_cache;
      elements.screenerSource.textContent =
        value === undefined || value === null ? "-" : value ? "cache" : "fresh";
    }

    if (elements.coinglassSource) {
      const value = snapshot?.coinglass_from_cache;
      elements.coinglassSource.textContent =
        value === undefined || value === null ? "-" : value ? "cache" : "fresh";
    }

    if (elements.lastError) {
      const lastError = state.last_error;
      elements.lastError.textContent = lastError ? truncate(lastError) : "None";
      if (lastError) {
        elements.lastError.setAttribute("title", lastError);
      } else {
        elements.lastError.removeAttribute("title");
      }
    }

    if (elements.lastProgress) {
      const events = Array.isArray(state.events) ? state.events : [];
      const lastEvent = events.length ? events[events.length - 1] : null;
      const message = lastEvent?.payload?.message || lastEvent?.event || "-";
      elements.lastProgress.textContent = message || "-";
      if (lastEvent?.payload?.message) {
        elements.lastProgress.setAttribute("title", lastEvent.payload.message);
      } else {
        elements.lastProgress.removeAttribute("title");
      }
    }

    if (elements.exchangeSummary) {
      const entries =
        (state.snapshot?.exchange_status &&
        Array.isArray(state.snapshot.exchange_status)
          ? state.snapshot.exchange_status
          : null) ??
        (Array.isArray(state.exchange_status) ? state.exchange_status : []);
      if (!entries || entries.length === 0) {
        elements.exchangeSummary.textContent = "-";
      } else {
        const counts = entries.reduce((acc, entry) => {
          const key = (entry.status || "unknown").toString().toLowerCase();
          acc[key] = (acc[key] || 0) + 1;
          return acc;
        }, {});
        const parts = [];
        if (counts.ok) parts.push(`${counts.ok} ok`);
        if (counts.pending) parts.push(`${counts.pending} pending`);
        if (counts.failed) parts.push(`${counts.failed} failed`);
        if (counts.missing) parts.push(`${counts.missing} missing`);
        if (counts.error) parts.push(`${counts.error} error`);
        elements.exchangeSummary.textContent = parts.length
          ? parts.join(", ")
          : `${entries.length} exchanges`;
      }
    }

    if (elements.hint) {
      elements.hint.textContent = `Auto refresh every ${pollIntervalSeconds} seconds`;
    }
  };

  const updateRefreshButton = () => {
    if (!elements.refreshButton) {
      return;
    }
    const busy = Boolean(currentState.refresh_in_progress);
    elements.refreshButton.disabled = busy;
    elements.refreshButton.textContent = busy
      ? "Refreshing..."
      : "Manual refresh";
  };

  const mergeState = (next = {}) => {
    const mergedSnapshot =
      next.snapshot !== undefined ? next.snapshot : currentState.snapshot;
    const mergedEvents = Array.isArray(next.events)
      ? next.events
      : currentState.events ?? [];
    const mergedExchangeStatus = Array.isArray(next.exchange_status)
      ? next.exchange_status
      : currentState.exchange_status ?? [];
    const snapshotExchangeStatus =
      mergedSnapshot && Array.isArray(mergedSnapshot.exchange_status)
        ? mergedSnapshot.exchange_status
        : null;

    currentState = {
      ...currentState,
      ...next,
      snapshot: mergedSnapshot,
      events: mergedEvents,
      exchange_status: snapshotExchangeStatus ?? mergedExchangeStatus,
      refresh_interval:
        next.refresh_interval ?? currentState.refresh_interval,
      refresh_in_progress:
        next.refresh_in_progress ?? currentState.refresh_in_progress,
    };
  };

  const ensurePolling = () => {
    const desired = Math.max(
      Number(currentState.refresh_interval) || 300,
      30,
    );
    if (desired !== pollIntervalSeconds) {
      pollIntervalSeconds = desired;
      schedulePolling();
    }
  };

  const renderState = (state) => {
    mergeState(state);
    ensurePolling();
    updateStatusPill(currentState.status);
    updateMetadata(currentState);
    renderSnapshotData(currentState.snapshot);
    const exchangeEntries =
      (currentState.snapshot &&
      Array.isArray(currentState.snapshot.exchange_status)
        ? currentState.snapshot.exchange_status
        : null) ?? currentState.exchange_status ?? [];
    renderExchangeStatus(exchangeEntries);
    currentState.exchange_status = exchangeEntries;
    renderEvents(currentState.events ?? []);
    toggleEmptyState(!currentState.snapshot);
    renderMessages(buildMessages(currentState));
    updateRefreshButton();
  };

  const fetchSnapshot = async (force = false) => {
    if (pollingInFlight) {
      return;
    }
    pollingInFlight = true;
    try {
      const response = await fetch("/api/snapshot", { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`Snapshot request failed: ${response.status}`);
      }
      const payload = await response.json();
      renderState(payload);
      if (force && payload.status === "pending") {
        setTimeout(() => fetchSnapshot(true), 2000);
      }
    } catch (error) {
      console.error(error);
      const fallbackMessages = buildMessages(currentState);
      fallbackMessages.unshift(`Snapshot load error: ${error.message}`);
      renderMessages(fallbackMessages);
    } finally {
      pollingInFlight = false;
    }
  };

  const triggerManualRefresh = async () => {
    if (!elements.refreshButton) {
      return;
    }
    if (currentState.refresh_in_progress) {
      return;
    }
    elements.refreshButton.disabled = true;
    elements.refreshButton.textContent = "Refreshing...";
    try {
      const response = await fetch("/api/refresh", { method: "POST" });
      if (!response.ok) {
        throw new Error(`Refresh failed: ${response.status}`);
      }
      const payload = await response.json();
      if (payload.state) {
        renderState(payload.state);
        if (payload.state.status === "pending") {
          setTimeout(() => fetchSnapshot(true), 2000);
        }
      } else {
        await fetchSnapshot(true);
      }
      if (payload.status === "failed") {
        const messages = buildMessages(currentState);
        messages.unshift("Manual refresh failed. Check logs for details.");
        renderMessages(messages);
      }
    } catch (error) {
      console.error(error);
      const messages = buildMessages(currentState);
      messages.unshift(`Manual refresh error: ${error.message}`);
      renderMessages(messages);
    } finally {
      updateRefreshButton();
    }
  };

  const schedulePolling = () => {
    if (pollingTimer) {
      clearInterval(pollingTimer);
    }
    pollingTimer = setInterval(() => {
      fetchSnapshot(false);
    }, pollIntervalSeconds * 1000);
  };

  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", triggerManualRefresh);
  }

  renderState(initialState);
  schedulePolling();

  if (!initialState.snapshot) {
    fetchSnapshot(true);
  }
})();
