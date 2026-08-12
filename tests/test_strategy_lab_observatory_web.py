from __future__ import annotations

import asyncio
import importlib

from fastapi.testclient import TestClient

from project_settings import AppSettings, SettingsManager
from webapp.services import DataService

webapp_app = importlib.import_module("webapp.app")


def test_legacy_main_sources_cannot_be_reenabled() -> None:
    settings = AppSettings().with_updates(
        {"sources": {"coinglass": True, "arbitragescanner": True}}
    )
    settings.validate()
    assert settings.sources == {"coinglass": False, "arbitragescanner": False}


def test_legacy_market_refresh_is_a_noop(tmp_path) -> None:
    service = DataService(SettingsManager(tmp_path / "settings.json"))

    result = asyncio.run(service.refresh_markets(force_sources=True))
    state = service.state_payload()

    assert result == "completed"
    assert state["status"] == "ready"
    assert state["snapshot"] is None
    assert state["events"][-1]["event"] == "legacy_discovery:disabled"


def test_main_page_hides_legacy_candidate_discovery() -> None:
    client = TestClient(webapp_app.app)
    response = client.get("/")

    assert response.status_code == 200
    html = response.text
    assert "Data sources" not in html
    assert "Funding Opportunities" not in html
    assert "ArbitrageScanner Top Entries" not in html
    assert "Coinglass Top Entries" not in html
    assert "Parser refresh" not in html
    assert "/strategy-lab-observatory" in html


def test_observatory_page_and_api_are_research_only() -> None:
    client = TestClient(webapp_app.app)

    page = client.get("/strategy-lab-observatory")
    status = client.get("/api/strategy-lab/observatory")

    assert page.status_code == 200
    assert "research-only" in page.text
    assert "sl-refresh-all" in page.text
    assert status.status_code == 200
    payload = status.json()
    assert payload["mode"] == "research_only_no_trading"
    assert payload["scheduler_enabled"] is False
    assert payload["selected_exchanges"] == ["binance", "bybit", "okx", "kucoin", "gate"]
