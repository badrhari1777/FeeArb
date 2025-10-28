(() => {
  const initialState = window.__INITIAL_STATE__ || null;
  const refreshInterval = Math.max(Number(window.__REFRESH_INTERVAL__) || 300, 30);

  const elements = {
    generatedAt: document.getElementById("generated-at"),
    screenerSource: document.getElementById("screener-source"),
    coinglassSource: document.getElementById("coinglass-source"),
    opportunityCount: document.getElementById("opportunity-count"),
    screenerTable: document.getElementById("screener-table")?.querySelector("tbody"),
    coinglassTable: document.getElementById("coinglass-table")?.querySelector("tbody"),
    universeTable: document.getElementById("universe-table-body"),
    opportunityTable: document.getElementById("opportunity-table-body"),
    messagesPanel: document.getElementById("messages"),
    refreshButton: document.getElementById("refresh-button"),
    hint: document.querySelector(".hint"),
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
    return value.toFixed(digits);
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

  const renderScreener = (rows = []) => {
    if (!elements.screenerTable) {
      return;
    }
    elements.screenerTable.innerHTML = rows
      .map(
        (row) => `
          <tr>
            <td>${row.symbol}</td>
            <td>${formatPercent(row.spread ?? 0, 4)}</td>
            <td>${row.long_exchange}</td>
            <td>${formatPercent(row.long_fee ?? 0, 4)}</td>
            <td>${row.short_exchange}</td>
            <td>${formatPercent(row.short_fee ?? 0, 4)}</td>
          </tr>
        `
      )
      .join("");
  };

  const renderCoinglass = (rows = []) => {
    if (!elements.coinglassTable) {
      return;
    }
    elements.coinglassTable.innerHTML = rows
      .map(
        (row) => `
          <tr>
            <td>${row.ranking}</td>
            <td>${row.symbol}</td>
            <td>${row.pair}</td>
            <td>${row.long_exchange}</td>
            <td>${row.short_exchange}</td>
            <td>${Number(row.net_funding_rate_percent ?? 0).toFixed(3)}</td>
            <td>${Number(row.apr_percent ?? 0).toFixed(2)}</td>
            <td>${Number(row.spread_rate_percent ?? 0).toFixed(3)}</td>
          </tr>
        `
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
            <td>${row.symbol}</td>
            <td>${row.sources}</td>
          </tr>
        `
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
            <td>${row.symbol}</td>
            <td>${row.long_exchange}</td>
            <td>${formatPercent(row.long_rate, 3)}</td>
            <td>${formatNumber(row.long_bid, 4)}</td>
            <td>${formatNumber(row.long_ask, 4)}</td>
            <td>${formatTime(row.long_next_funding)}</td>
            <td>${row.short_exchange}</td>
            <td>${formatPercent(row.short_rate, 3)}</td>
            <td>${formatNumber(row.short_bid, 4)}</td>
            <td>${formatNumber(row.short_ask, 4)}</td>
            <td>${formatTime(row.short_next_funding)}</td>
            <td>${formatPercent(row.spread, 3)}</td>
            <td>${formatPercent(row.price_diff_pct, 3)}</td>
            <td>${formatPercent(row.effective_spread, 3)}</td>
            <td>${row.participants ?? "-"}</td>
          </tr>
        `
      )
      .join("");
    if (elements.opportunityCount) {
      elements.opportunityCount.textContent = rows.length;
    }
  };

  const renderMessages = (messages = []) => {
    if (!elements.messagesPanel) {
      if (!messages || messages.length === 0) {
        return;
      }
      const panel = document.createElement("section");
      panel.className = "panel panel--alert";
      panel.id = "messages";
      panel.innerHTML = `
        <h2>Status Messages</h2>
        <ul>${messages.map((msg) => `<li>${msg}</li>`).join("")}</ul>
      `;
      document.querySelector(".content")?.prepend(panel);
      elements.messagesPanel = panel;
      return;
    }

    if (messages && messages.length > 0) {
      elements.messagesPanel.innerHTML = `
        <h2>Status Messages</h2>
        <ul>${messages.map((msg) => `<li>${msg}</li>`).join("")}</ul>
      `;
      elements.messagesPanel.style.display = "";
    } else {
      elements.messagesPanel.style.display = "none";
    }
  };

  const renderSnapshot = (payload) => {
    if (!payload) {
      return;
    }
    if (elements.generatedAt) {
      elements.generatedAt.textContent = payload.generated_at ?? "-";
    }
    if (elements.screenerSource) {
      elements.screenerSource.textContent = payload.screener_from_cache ? "cache" : "fresh";
    }
    if (elements.coinglassSource) {
      elements.coinglassSource.textContent = payload.coinglass_from_cache ? "cache" : "fresh";
    }

    renderScreener(payload.screener_rows ?? []);
    renderCoinglass(payload.coinglass_rows ?? []);
    renderUniverse(payload.universe ?? []);
    renderOpportunities(payload.opportunities ?? []);
    renderMessages(payload.messages ?? []);
  };

  let pollingTimer = null;
  let pollingInFlight = false;

  const fetchSnapshot = async (force = false) => {
    if (pollingInFlight) {
      return;
    }
    try {
      pollingInFlight = true;
      const response = await fetch("/api/snapshot");
      if (!response.ok) {
        throw new Error(`Snapshot request failed: ${response.status}`);
      }
      const payload = await response.json();
      if (payload.status === "pending") {
        if (force) {
          setTimeout(() => fetchSnapshot(force), 2000);
        }
        return;
      }
      renderSnapshot(payload);
    } catch (error) {
      console.error(error);
      renderMessages([`Snapshot load error: ${error.message}`]);
    } finally {
      pollingInFlight = false;
    }
  };

  const triggerManualRefresh = async () => {
    if (!elements.refreshButton) {
      return;
    }
    elements.refreshButton.disabled = true;
    elements.refreshButton.textContent = "Refreshing...";
    try {
      const response = await fetch("/api/refresh", { method: "POST" });
      if (!response.ok) {
        throw new Error(`Refresh failed: ${response.status}`);
      }
      await fetchSnapshot(true);
    } catch (error) {
      console.error(error);
      renderMessages([`Manual refresh error: ${error.message}`]);
    } finally {
      elements.refreshButton.disabled = false;
      elements.refreshButton.textContent = "Manual refresh";
    }
  };

  const startPolling = () => {
    if (pollingTimer) {
      clearInterval(pollingTimer);
    }
    pollingTimer = setInterval(() => {
      fetchSnapshot(false);
    }, refreshInterval * 1000);
  };

  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", triggerManualRefresh);
  }

  renderSnapshot(initialState);
  startPolling();
})();
