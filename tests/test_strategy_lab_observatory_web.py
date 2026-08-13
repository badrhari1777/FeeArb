from __future__ import annotations

import importlib
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from project_settings import AppSettings

webapp_app = importlib.import_module("webapp.app")


def test_legacy_main_sources_are_not_part_of_app_settings() -> None:
    settings = AppSettings().with_updates(
        {"sources": {"coinglass": True, "arbitragescanner": True}}
    )
    settings.validate()
    assert "sources" not in settings.to_dict()


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
    assert "sl-registry-refresh" in page.text
    assert "sl-feed-probe" in page.text
    assert "Bounded own-feed probe" in page.text
    assert status.status_code == 200
    payload = status.json()
    assert payload["mode"] == "research_only_no_trading"
    assert payload["scheduler_enabled"] is False
    assert payload["selected_exchanges"] == ["binance", "bybit", "okx", "kucoin", "gate"]
    assert payload["registry"]["status"] == "never_run"
    assert payload["feed_probe"]["status"] == "never_run"


def test_observatory_registry_and_feed_endpoints_delegate_only_to_research_service() -> None:
    client = TestClient(webapp_app.app)
    registry_result = {
        "mode": "research_only_no_trading",
        "scheduler_enabled": False,
        "registry_refresh_result": "completed",
    }
    feed_result = {
        "mode": "research_only_no_trading",
        "scheduler_enabled": False,
        "feed_probe_result": "completed",
    }

    with patch.object(
        webapp_app.strategy_lab_observatory,
        "refresh_registry",
        new=AsyncMock(return_value=registry_result),
    ) as registry_mock, patch.object(
        webapp_app.strategy_lab_observatory,
        "run_feed_probe",
        new=AsyncMock(return_value=feed_result),
    ) as feed_mock:
        registry = client.post("/api/strategy-lab/observatory/registry/refresh")
        feed = client.post(
            "/api/strategy-lab/observatory/feed/probe",
            json={"duration_sec": 7, "max_symbols": 3},
        )

    assert registry.status_code == 200
    assert registry.json()["registry_refresh_result"] == "completed"
    registry_mock.assert_awaited_once_with()
    assert feed.status_code == 200
    assert feed.json()["feed_probe_result"] == "completed"
    feed_mock.assert_awaited_once_with(duration_sec=7.0, max_symbols=3)


def test_observatory_feed_endpoint_rejects_unbounded_probe() -> None:
    client = TestClient(webapp_app.app)

    response = client.post(
        "/api/strategy-lab/observatory/feed/probe",
        json={"duration_sec": 31, "max_symbols": 11},
    )

    assert response.status_code == 422
