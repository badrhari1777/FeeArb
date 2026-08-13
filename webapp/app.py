from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from project_settings import MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS, SettingsManager
from utils import setup_logging

from .services import (
    ADAPTER_FACTORIES,
    DataService,
    FUNDING_HISTORY_EXCLUDED_EXCHANGES,
    FUNDING_HISTORY_DEFAULT_EXCHANGES,
    FUNDING_HISTORY_WINDOWS_HOURS,
)
from .manual_stream import ManualSpreadStream
from .manual_trade_stream import ManualTradeStream
from .balance_views import with_pump_account_balances
from .dashboard import build_dashboard_payload
from .positions_overview import build_positions_overview
from .ws_trade_raw import WsTradeRawStream
from .ws_trade_private_raw import WsTradePrivateRawStream
from .ws_trade_okx_raw import WsTradeOkxRawStream
from .ws_trade_binance_raw import WsTradeBinanceRawStream
from .ws_trade_bitget_raw import WsTradeBitgetRawStream
from .ws_trade_bitget_trade_raw import WsTradeBitgetTradeRawStream
from .ws_trade_bingx_raw import WsTradeBingxRawStream
from .ws_trade_gate_raw import WsTradeGateRawStream
from .ws_trade_kucoin_raw import WsTradeKucoinRawStream
from .remote_access import (
    has_valid_remote_token,
    is_cloudflare_request,
    is_public_proxy_request,
)
from .bybit_pump_short_lab import (
    BybitPumpShortLab,
    normalize_run_config,
    normalize_shadow_config,
    normalize_shadow_schedule_config,
)
from execution.pump_live import PumpLiveController
from strategy_lab import StrategyLabObservatory

BASE_DIR = Path(__file__).resolve().parent
setup_logging(BASE_DIR.parent / "logs")

STATIC_VERSION = "v2026-08-13-dashboard-v4"

app = FastAPI(title="Funding Arbitrage Monitor", version="0.1.0")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

settings_manager = SettingsManager()
service = DataService(settings_manager=settings_manager)
if os.getenv("FEEARB_TESTING") == "1":
    _pump_test_state_dir = Path(tempfile.mkdtemp(prefix="feearb-pump-live-test-"))
    _pump_test_controller = PumpLiveController(
        state_dir=_pump_test_state_dir,
        env_path=_pump_test_state_dir / "pump_live.env",
        start_recovery_monitor=False,
        background_monitor=False,
    )
    bybit_pump_short_lab = BybitPumpShortLab(
        restore_shadow_schedule=False,
        pump_live_controller=_pump_test_controller,
        notifier=service.notification_router,
        main_portfolio_provider=service.mobile_positions_payload,
    )
    strategy_lab_observatory = StrategyLabObservatory(
        state_dir=Path(tempfile.mkdtemp(prefix="feearb-strategy-lab-test-")),
    )
else:
    bybit_pump_short_lab = BybitPumpShortLab(
        notifier=service.notification_router,
        main_portfolio_provider=service.mobile_positions_payload,
    )
    strategy_lab_observatory = StrategyLabObservatory()
logger = logging.getLogger(__name__)


def _dashboard_payload() -> dict[str, object]:
    pump_status = bybit_pump_short_lab.pump_live_status()
    return build_dashboard_payload(
        service.dashboard_runtime_payload(),
        service.mobile_positions_payload(),
        pump_status,
    )


@app.middleware("http")
async def require_cloudflare_remote_token(request: Request, call_next):
    is_remote = is_cloudflare_request(request.headers) or is_public_proxy_request(request.headers)
    if is_remote and not has_valid_remote_token(request.headers):
        return JSONResponse(
            status_code=401,
            content={"detail": "Missing or invalid FeeArb remote access token."},
        )
    return await call_next(request)

class SettingsPayload(BaseModel):
    exchanges: Dict[str, bool]
    analysis_exchanges: Dict[str, bool]
    table_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    account_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    positions_market_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    summary_refresh_seconds: Optional[int] = Field(
        default=None, ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS
    )
    protective: Optional[Dict[str, object]] = None
    manual: Optional[Dict[str, object]] = None


class StrategyLabRefreshPayload(BaseModel):
    sources: Optional[list[str]] = None


class StrategyLabFeedProbePayload(BaseModel):
    duration_sec: float = Field(default=12.0, ge=1.0, le=30.0)
    max_symbols: int = Field(default=5, ge=1, le=10)


class ManualBasePayload(BaseModel):
    symbol: str
    qty: Optional[float] = Field(default=None, gt=0)
    notional: Optional[float] = Field(default=None, gt=0)
    mode: str = "limit-first-expensive"
    max_slippage_bps: Optional[float] = Field(default=12.0, ge=0)
    spread_min_pct: Optional[float] = None
    spread_max_pct: Optional[float] = None
    timeout_sec: Optional[int] = Field(default=15, ge=0)
    max_runtime_sec: Optional[int] = Field(default=None, ge=1, le=1800)
    trigger_wait_sec: Optional[int] = Field(default=30, ge=1, le=60)
    reprice_sec: Optional[float] = Field(default=None, ge=0)
    chunk_qty: Optional[float] = Field(default=None, gt=0)
    chunk_notional: Optional[float] = Field(default=None, gt=0)
    force_chunk_qty: Optional[bool] = None
    hedge_order_type: Optional[str] = None
    hedge_offset_bps: Optional[float] = Field(default=None, ge=0)
    hedge_offset_ticks: Optional[int] = Field(default=None, ge=0)
    hedge_improve_ticks: Optional[int] = Field(default=None, ge=0)
    hedge_limit_mode: Optional[str] = None
    hedge_favorable_bps: Optional[float] = Field(default=None, ge=0)
    hedge_adverse_bps: Optional[float] = Field(default=None, ge=0)
    hedge_adverse_ticks: Optional[float] = Field(default=None, ge=0)
    hedge_reprice_min_sec: Optional[float] = Field(default=None, ge=0)
    hedge_timeout_sec: Optional[float] = Field(default=5.0, ge=1, le=30)
    limit_offset_bps: Optional[float] = Field(default=None, ge=0)
    limit_offset_ticks: Optional[int] = Field(default=None, ge=0)
    limit_improve_ticks: Optional[int] = Field(default=None, ge=0)
    auto_limit_price: bool = True
    min_level_notional: Optional[float] = Field(default=None, ge=0)
    min_level_qty: Optional[float] = Field(default=None, ge=0)
    min_level_chunk_pct: Optional[float] = Field(default=None, ge=0)
    max_limit_deviation_bps: Optional[float] = Field(default=None, ge=0)
    use_orderbook_check: bool = True
    allow_liquidity_chunking: bool = False
    fallback_to_market: bool = False
    async_run: bool = True
    dry_run: bool = False
    limit_price_long: Optional[float] = None
    limit_price_short: Optional[float] = None
    expensive_leg: Optional[str] = None
    margin_mode: Optional[str] = None
    ws_orders_health: Optional[Dict[str, Dict[str, object]]] = None


class ManualEnterPayload(ManualBasePayload):
    long_exchange: str
    short_exchange: str


class ManualExitPayload(ManualBasePayload):
    long_exchange: str
    short_exchange: str
    position_id: Optional[str] = None
    exit_allow_flip: Optional[bool] = False


class ManualRollPayload(ManualBasePayload):
    from_exchange: str
    to_exchange: str
    side: str


class ManualAnalyzePayload(ManualBasePayload):
    long_exchange: str
    short_exchange: str
    action: str = "enter"


class MobileManualSpreadPayload(BaseModel):
    symbol: str
    action: str = "enter"
    long_exchange: Optional[str] = None
    short_exchange: Optional[str] = None
    from_exchange: Optional[str] = None
    to_exchange: Optional[str] = None
    side: Optional[str] = "long"


def _manual_payload_dict(payload: ManualBasePayload) -> dict:
    model_dump = getattr(payload, "model_dump", None)
    data = model_dump() if callable(model_dump) else payload.dict()
    provided_fields = getattr(payload, "model_fields_set", None)
    if provided_fields is None:
        provided_fields = getattr(payload, "__fields_set__", set())
    mode = str(data.get("mode") or "").lower()
    if mode.startswith("smart-") and "allow_liquidity_chunking" not in provided_fields:
        data["allow_liquidity_chunking"] = True
    notional = data.get("notional")
    chunk_qty = data.get("chunk_qty")
    chunk_notional = data.get("chunk_notional")
    if (
        mode.startswith("smart-")
        and not data.get("dry_run")
        and data.get("async_run") is not False
        and isinstance(notional, (int, float))
        and notional > 0
        and not chunk_qty
        and not chunk_notional
    ):
        current_runtime = data.get("max_runtime_sec")
        if (
            "max_runtime_sec" not in provided_fields
            or not isinstance(current_runtime, (int, float))
        ):
            data["max_runtime_sec"] = 600
    return data


class PositionActionPayload(BaseModel):
    symbol: str
    long_exchange: str
    short_exchange: str
    action: str
    percent: float = Field(default=100.0, gt=0, le=100)
    dry_run: bool = True
    async_run: bool = True
    max_runtime_sec: Optional[int] = Field(default=None, ge=60, le=600)


class AutoArbRulePayload(BaseModel):
    id: Optional[str] = None
    symbol: str
    long_exchange: str
    short_exchange: str
    setup_mode: str = "entry_range"
    budget_mode: str = "qty"
    max_qty: Optional[float] = Field(default=None, gt=0)
    max_notional: Optional[float] = Field(default=None, gt=0)
    range_start_pct: float
    range_end_pct: float
    exit_range_start_pct: Optional[float] = None
    exit_range_end_pct: Optional[float] = None
    level_count: Optional[int] = Field(default=None, ge=2, le=20)
    exit_gap_pct: Optional[float] = Field(default=None, gt=0)
    max_slippage_bps: float = Field(default=8.0, ge=0)
    liquidity_safety_factor: float = Field(default=0.70, gt=0, le=1)
    confirm_samples: int = Field(default=2, ge=1, le=10)
    enabled: bool = True
    live: bool = False


class AutoArbLivePayload(BaseModel):
    confirmation: str


class ManualTestPayload(BaseModel):
    exchange: str
    symbol: str
    side: str
    qty: float = Field(..., gt=0)
    price: Optional[float] = Field(default=None, gt=0)
    offset_bps: Optional[float] = Field(default=50.0, ge=0)
    offset_ticks: Optional[int] = Field(default=0, ge=0)
    reduce_only: bool = False
    position_side: Optional[str] = None
    margin_mode: Optional[str] = None


class ManualTestCancelPayload(BaseModel):
    exchange: str
    symbol: str
    order_id: str


class ManualTestPositionPayload(BaseModel):
    exchange: str
    symbol: str
    side: Optional[str] = None


class ManualTestMarginPayload(BaseModel):
    exchange: str
    symbol: str
    amount: float = Field(..., gt=0)
    side: Optional[str] = None


class ManualTestLeveragePayload(BaseModel):
    exchange: str
    symbol: str
    leverage: float = Field(..., gt=0)
    side: Optional[str] = None
    margin_mode: Optional[str] = None


class ManualTestFundingPayload(BaseModel):
    exchange: str
    symbol: str
    include_raw: bool = False
    history_limit: Optional[int] = Field(default=12, ge=1, le=200)


class FundingHistoryAnalyzePayload(BaseModel):
    symbol: str
    exchanges: Optional[list[str]] = None
    windows_hours: Optional[list[int]] = None
    funding_points: Optional[int] = Field(default=200, ge=24, le=200)


class BybitPumpShortStartPayload(BaseModel):
    lookback_days: Optional[int] = Field(default=30, ge=1, le=90)
    sleep_sec: Optional[float] = Field(default=0.8, ge=0.1, le=10.0)
    max_symbols: Optional[int] = Field(default=None, ge=1, le=1000)
    symbols: Optional[list[str]] = None
    newest_first: Optional[bool] = True
    resume: Optional[bool] = True


class BybitPumpShortShadowStartPayload(BaseModel):
    lookback_days: Optional[int] = Field(default=14, ge=2, le=30)
    sleep_sec: Optional[float] = Field(default=0.8, ge=0.1, le=10.0)
    max_symbols: Optional[int] = Field(default=50, ge=1, le=1000)
    symbols: Optional[list[str]] = None
    newest_first: Optional[bool] = True
    recent_event_hours: Optional[int] = Field(default=168, ge=24, le=720)


class BybitPumpShortShadowSchedulePayload(BybitPumpShortShadowStartPayload):
    interval_sec: Optional[int] = Field(default=3600, ge=60, le=86400)
    run_immediately: Optional[bool] = True


class PumpLiveConfirmationPayload(BaseModel):
    confirmation: str


class PumpLivePrefundPayload(BaseModel):
    symbol: str = Field(min_length=2, max_length=30)
    confirmation: str


class PumpLiveCapitalPayload(BaseModel):
    strategy_capital_usd: float = Field(ge=100.0, le=1_000_000.0)
    confirmation: str
    note: Optional[str] = Field(default=None, max_length=200)


class PumpCapitalPromotionPayload(BaseModel):
    target_capital_usd: float = Field(ge=3_000.0, le=3_000.0)
    confirmation: str


class PumpTemporaryTransferPayload(BaseModel):
    amount_usdt: float = Field(ge=0.01, le=100_000.0)
    confirmation: str


class NotificationTestPayload(BaseModel):
    title: Optional[str] = "FeeArb test notification"
    message: Optional[str] = "FeeArb notification test from backend."


@app.on_event("startup")
async def startup_event() -> None:
    # Kick off background startup so FastAPI can serve immediately.
    async def _run_startup() -> None:
        try:
            await service.startup()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Data service startup failed")

    asyncio.create_task(_run_startup())

@app.on_event("shutdown")
async def shutdown_event() -> None:
    bybit_pump_short_lab.shutdown()
    await service.shutdown()


@app.get("/favicon.ico")
async def favicon() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "favicon.svg", media_type="image/svg+xml")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    dashboard = _dashboard_payload()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "dashboard": dashboard,
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/positions", response_class=HTMLResponse)
async def positions_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="positions.html",
        context={
            "request": request,
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/strategy-lab-observatory", response_class=HTMLResponse)
async def strategy_lab_observatory_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="strategy_lab_observatory.html",
        context={
            "request": request,
            "initial": strategy_lab_observatory.status(),
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/funding-history", response_class=HTMLResponse)
async def funding_history_page(
    request: Request,
    symbol: str = "BTCUSDT",
) -> HTMLResponse:
    initial = {
        "symbol": symbol,
        "supported_exchanges": [
            exchange
            for exchange in settings_manager.as_dict().get("analysis_exchanges", {}).keys()
            if exchange in ADAPTER_FACTORIES and exchange not in FUNDING_HISTORY_EXCLUDED_EXCHANGES
        ],
        "default_exchanges": list(FUNDING_HISTORY_DEFAULT_EXCHANGES),
        "windows": [
            {"hours": int(hours), "label": "1d" if int(hours) == 24 else "3d" if int(hours) == 72 else f"{int(hours)}h"}
            for hours in FUNDING_HISTORY_WINDOWS_HOURS
        ],
    }
    return templates.TemplateResponse(
        request=request,
        name="funding_history.html",
        context={
            "request": request,
            "symbol": symbol,
            "initial": initial,
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/pump-short-lab", response_class=HTMLResponse)
async def pump_short_lab_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="pump_short_lab.html",
        context={
            "request": request,
            "initial": bybit_pump_short_lab.status(),
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/pump-short", response_class=HTMLResponse)
async def pump_short_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="pump_short.html",
        context={
            "request": request,
            "initial": bybit_pump_short_lab.pump_dashboard_status(),
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/pump-short-strategies", response_class=HTMLResponse)
async def pump_short_strategies_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="pump_short_strategies.html",
        context={
            "request": request,
            "initial": bybit_pump_short_lab.strategy_monitor_status(),
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/manual", response_class=HTMLResponse)
async def manual_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    return templates.TemplateResponse(
        request=request,
        name="manual.html",
        context={
            "request": request,
            "settings": settings,
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/auto-arbitrage", response_class=HTMLResponse)
async def auto_arbitrage_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    exchanges = [
        name
        for name, enabled in (settings.get("analysis_exchanges") or {}).items()
        if enabled
    ]
    return templates.TemplateResponse(
        request=request,
        name="auto_arbitrage.html",
        context={
            "request": request,
            "initial": {
                "exchanges": exchanges,
                "rules": service.auto_arb_payload().get("rules", []),
            },
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/manual-tests", response_class=HTMLResponse)
async def manual_tests_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    return templates.TemplateResponse(
        request=request,
        name="manual_tests.html",
        context={
            "request": request,
            "settings": settings,
            "static_version": STATIC_VERSION,
        },
    )

@app.get("/spread-monitor", response_class=HTMLResponse)
async def spread_monitor_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    return templates.TemplateResponse(
        request=request,
        name="spread_monitor.html",
        context={
            "request": request,
            "settings": settings,
            "static_version": STATIC_VERSION,
        },
    )

@app.get("/api/dashboard")
async def dashboard_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(_dashboard_payload()))


@app.get("/api/strategy-lab/observatory")
async def strategy_lab_observatory_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(strategy_lab_observatory.status()))


@app.post("/api/strategy-lab/observatory/refresh")
async def strategy_lab_observatory_refresh_api(
    payload: StrategyLabRefreshPayload,
) -> JSONResponse:
    try:
        result = await strategy_lab_observatory.refresh(sources=payload.sources)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/strategy-lab/observatory/registry/refresh")
async def strategy_lab_observatory_registry_refresh_api() -> JSONResponse:
    try:
        result = await strategy_lab_observatory.refresh_registry()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/strategy-lab/observatory/feed/probe")
async def strategy_lab_observatory_feed_probe_api(
    payload: StrategyLabFeedProbePayload,
) -> JSONResponse:
    try:
        result = await strategy_lab_observatory.run_feed_probe(
            duration_sec=payload.duration_sec,
            max_symbols=payload.max_symbols,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.get("/api/positions/overview")
async def positions_overview_api() -> JSONResponse:
    payload = build_positions_overview(
        service.mobile_positions_payload(),
        bybit_pump_short_lab.pump_live_status(),
    )
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/funding-history/analyze")
async def funding_history_analyze_api(payload: FundingHistoryAnalyzePayload) -> JSONResponse:
    try:
        result = await service.analyze_funding_history(
            payload.symbol,
            exchanges=payload.exchanges,
            windows_hours=payload.windows_hours,
            funding_points=payload.funding_points or 200,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.get("/api/pump-short/bybit/status")
async def bybit_pump_short_status_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.status()))


@app.get("/api/pump-short/dashboard")
async def pump_short_dashboard_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.pump_dashboard_status()))


@app.get("/api/pump-short/strategies")
async def pump_short_strategies_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.strategy_monitor_status()))


@app.get("/api/pump-short/live")
async def pump_short_live_status_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.pump_live_status()))


@app.post("/api/pump-short/live/preflight")
async def pump_short_live_preflight_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.pump_live_preflight()))


@app.post("/api/pump-short/live/prepare")
async def pump_short_live_prepare_api(payload: PumpLiveConfirmationPayload) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_live_prepare(payload.confirmation)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/live/arm")
async def pump_short_live_arm_api(payload: PumpLiveConfirmationPayload) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_live_arm(payload.confirmation)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/live/capital")
async def pump_short_live_capital_api(payload: PumpLiveCapitalPayload) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_live_set_strategy_capital(
            payload.strategy_capital_usd,
            payload.confirmation,
            payload.note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/live/capital/promote")
async def pump_short_live_capital_promote_api(
    payload: PumpCapitalPromotionPayload,
) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_live_promote_strategy_capital(
            payload.target_capital_usd,
            payload.confirmation,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/live/disarm")
async def pump_short_live_disarm_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.pump_live_disarm()))


@app.post("/api/pump-short/live/emergency-close")
async def pump_short_live_emergency_close_api(
    payload: PumpLiveConfirmationPayload,
) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_live_emergency_close(payload.confirmation)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/live/prefund-next-ladder")
async def pump_short_live_prefund_next_ladder_api(
    payload: PumpLivePrefundPayload,
) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_live_prefund_next_ladder(
            payload.symbol,
            payload.confirmation,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.get("/api/pump-short/live/transfers")
async def pump_short_live_transfers_status_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.pump_transfer_status()))


@app.post("/api/pump-short/live/transfers/preflight")
async def pump_short_live_transfers_preflight_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.pump_transfer_preflight()))


@app.post("/api/pump-short/live/transfers/in")
async def pump_short_live_transfers_in_api(
    payload: PumpTemporaryTransferPayload,
) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_transfer_in(
            payload.amount_usdt,
            payload.confirmation,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/live/transfers/return")
async def pump_short_live_transfers_return_api(
    payload: PumpTemporaryTransferPayload,
) -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_transfer_return(
            payload.amount_usdt,
            payload.confirmation,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/live/transfers/reconcile")
async def pump_short_live_transfers_reconcile_api() -> JSONResponse:
    try:
        result = bybit_pump_short_lab.pump_transfer_reconcile()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/pump-short/bybit/start")
async def bybit_pump_short_start_api(payload: BybitPumpShortStartPayload) -> JSONResponse:
    try:
        config = normalize_run_config(
            lookback_days=payload.lookback_days,
            sleep_sec=payload.sleep_sec,
            max_symbols=payload.max_symbols,
            symbols=payload.symbols or [],
            newest_first=payload.newest_first,
            resume=payload.resume,
        )
        status = bybit_pump_short_lab.start(config)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(status))


@app.post("/api/pump-short/bybit/stop")
async def bybit_pump_short_stop_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.stop()))


@app.post("/api/pump-short/bybit/shadow/start")
async def bybit_pump_short_shadow_start_api(payload: BybitPumpShortShadowStartPayload) -> JSONResponse:
    try:
        config = normalize_shadow_config(
            lookback_days=payload.lookback_days,
            sleep_sec=payload.sleep_sec,
            max_symbols=payload.max_symbols,
            symbols=payload.symbols or [],
            newest_first=payload.newest_first,
            recent_event_hours=payload.recent_event_hours,
        )
        status = bybit_pump_short_lab.start_shadow_scan(config)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(status))


@app.get("/api/pump-short/bybit/shadow/status")
async def bybit_pump_short_shadow_status_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.shadow_status()))


@app.post("/api/pump-short/bybit/shadow/schedule/start")
async def bybit_pump_short_shadow_schedule_start_api(
    payload: BybitPumpShortShadowSchedulePayload,
) -> JSONResponse:
    try:
        config = normalize_shadow_schedule_config(
            lookback_days=payload.lookback_days,
            sleep_sec=payload.sleep_sec,
            max_symbols=payload.max_symbols,
            symbols=payload.symbols or [],
            newest_first=payload.newest_first,
            recent_event_hours=payload.recent_event_hours,
            interval_sec=payload.interval_sec,
            run_immediately=payload.run_immediately,
        )
        status = bybit_pump_short_lab.start_shadow_schedule(config)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(status))


@app.post("/api/pump-short/bybit/shadow/schedule/stop")
async def bybit_pump_short_shadow_schedule_stop_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.stop_shadow_schedule()))


@app.get("/api/pump-short/bybit/shadow/schedule/status")
async def bybit_pump_short_shadow_schedule_status_api() -> JSONResponse:
    return JSONResponse(jsonable_encoder(bybit_pump_short_lab.shadow_schedule_status()))


@app.post("/api/notifications/test")
async def notification_test_api(payload: NotificationTestPayload) -> JSONResponse:
    status = await service.send_test_notification(
        message=payload.message or "FeeArb notification test from backend.",
        title=payload.title or "FeeArb test notification",
    )
    return JSONResponse(jsonable_encoder(status))



@app.get("/api/settings")
async def get_settings() -> JSONResponse:
    return JSONResponse({"settings": settings_manager.as_dict()})


@app.get("/api/mobile/positions")
async def mobile_positions() -> JSONResponse:
    payload = with_pump_account_balances(
        service.mobile_positions_payload(),
        bybit_pump_short_lab.pump_live_status(),
    )
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/mobile/manual-defaults")
async def mobile_manual_defaults() -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.mobile_manual_defaults_payload()))


@app.post("/api/mobile/manual-spread")
async def mobile_manual_spread(payload: MobileManualSpreadPayload) -> JSONResponse:
    return JSONResponse(jsonable_encoder(await service.mobile_manual_spread(payload.dict(exclude_none=True))))


@app.post("/api/position/action")
async def position_action(payload: PositionActionPayload) -> JSONResponse:
    logger.info(
        "position action request symbol=%s pair=%s/%s action=%s percent=%s dry_run=%s async_run=%s",
        payload.symbol,
        payload.long_exchange,
        payload.short_exchange,
        payload.action,
        payload.percent,
        payload.dry_run,
        payload.async_run,
    )
    try:
        result = await service.position_action(payload.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("position action failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))


@app.get("/api/auto-arb")
async def get_auto_arb() -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.auto_arb_payload()))


@app.post("/api/auto-arb/analyze")
async def auto_arb_analyze(payload: AutoArbRulePayload) -> JSONResponse:
    try:
        result = await service.analyze_auto_arb(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/auto-arb/rules")
async def auto_arb_upsert(payload: AutoArbRulePayload) -> JSONResponse:
    created_rule_id = ""
    try:
        result = await service.upsert_auto_arb_rule(payload.dict(exclude_none=True))
        if payload.live:
            rule_id = str((result.get("rule") or {}).get("id") or "")
            if not payload.id:
                created_rule_id = rule_id
            armed = await service.arm_auto_arb_live(rule_id, f"LIVE {rule_id}")
            result["rule"] = armed["rule"]
            result["live"] = True
    except ValueError as exc:
        if created_rule_id:
            try:
                await service.delete_auto_arb_rule(created_rule_id)
            except ValueError:
                pass
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/auto-arb/rules/{rule_id}/pause")
async def auto_arb_pause(rule_id: str) -> JSONResponse:
    try:
        result = await service.set_auto_arb_rule_enabled(rule_id, False)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/auto-arb/rules/{rule_id}/resume")
async def auto_arb_resume(rule_id: str) -> JSONResponse:
    try:
        result = await service.set_auto_arb_rule_enabled(rule_id, True)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/auto-arb/rules/{rule_id}/arm-live")
async def auto_arb_arm_live(rule_id: str, payload: AutoArbLivePayload) -> JSONResponse:
    try:
        result = await service.arm_auto_arb_live(rule_id, payload.confirmation)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/auto-arb/rules/{rule_id}/shadow")
async def auto_arb_shadow(rule_id: str) -> JSONResponse:
    try:
        result = await service.set_auto_arb_shadow(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.delete("/api/auto-arb/rules/{rule_id}")
async def auto_arb_delete(rule_id: str) -> JSONResponse:
    try:
        result = await service.delete_auto_arb_rule(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.get("/api/auto-arb/rules/{rule_id}/history")
async def auto_arb_history(rule_id: str, limit: int = 100) -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.auto_arb_history(rule_id, limit=limit)))


@app.websocket("/ws/manual")
async def manual_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = ManualSpreadStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass

@app.websocket("/ws/manual-trade")
async def manual_trade_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = ManualTradeStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass

@app.websocket("/ws/trade-raw")
async def trade_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass

@app.websocket("/ws/trade-private-raw")
async def trade_private_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradePrivateRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass

@app.websocket("/ws/trade-okx-raw")
async def trade_okx_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeOkxRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass

@app.websocket("/ws/trade-binance-raw")
async def trade_binance_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeBinanceRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/trade-bitget-raw")
async def trade_bitget_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeBitgetRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/trade-bitget-trade-raw")
async def trade_bitget_trade_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeBitgetTradeRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/trade-bingx-raw")
async def trade_bingx_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeBingxRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/trade-gate-raw")
async def trade_gate_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeGateRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass


@app.websocket("/ws/trade-kucoin-raw")
async def trade_kucoin_raw_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    stream = WsTradeKucoinRawStream(websocket)
    try:
        await stream.run()
    except WebSocketDisconnect:
        pass

@app.post("/api/settings")
async def update_settings(payload: SettingsPayload) -> JSONResponse:
    try:
        settings_manager.update(payload.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await service.on_settings_updated()
    return JSONResponse(
        {
            "settings": settings_manager.as_dict(),
        }
    )


@app.post("/api/manual/enter")
async def manual_enter(payload: ManualEnterPayload) -> JSONResponse:
    data = _manual_payload_dict(payload)
    logger.info("manual enter request %s", data)
    try:
        result = await service.manual_enter(data)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("manual enter failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/manual/exit")
async def manual_exit(payload: ManualExitPayload) -> JSONResponse:
    data = _manual_payload_dict(payload)
    logger.info("manual exit request %s", data)
    try:
        result = await service.manual_exit(data)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("manual exit failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/manual/roll")
async def manual_roll(payload: ManualRollPayload) -> JSONResponse:
    data = _manual_payload_dict(payload)
    logger.info("manual roll request %s", data)
    try:
        result = await service.manual_roll(data)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("manual roll failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/manual/analyze")
async def manual_analyze(payload: ManualAnalyzePayload) -> JSONResponse:
    data = _manual_payload_dict(payload)
    logger.info("manual analyze request %s", data)
    try:
        result = await service.manual_analyze(data)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("manual analyze failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/limit")
async def manual_test_limit(payload: ManualTestPayload) -> JSONResponse:
    result = await service.manual_test_limit(payload.dict())
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/market")
async def manual_test_market(payload: ManualTestPayload) -> JSONResponse:
    result = await service.manual_test_market(payload.dict())
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/cancel")
async def manual_test_cancel(payload: ManualTestCancelPayload) -> JSONResponse:
    result = await service.manual_test_cancel(payload.dict())
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/position")
async def manual_test_position(payload: ManualTestPositionPayload) -> JSONResponse:
    result = await service.manual_test_position(payload.dict())
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/margin/add")
async def manual_test_margin_add(payload: ManualTestMarginPayload) -> JSONResponse:
    result = await service.manual_test_margin(payload.dict(), action="add")
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/margin/reduce")
async def manual_test_margin_reduce(payload: ManualTestMarginPayload) -> JSONResponse:
    result = await service.manual_test_margin(payload.dict(), action="reduce")
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/leverage")
async def manual_test_leverage(payload: ManualTestLeveragePayload) -> JSONResponse:
    result = await service.manual_test_leverage(payload.dict())
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/manual/test/leverage/binance")
async def manual_test_leverage_binance(payload: ManualTestLeveragePayload) -> JSONResponse:
    result = await service.manual_test_binance_leverage(payload.dict())
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/manual/test/funding")
async def manual_test_funding(payload: ManualTestFundingPayload) -> JSONResponse:
    result = await service.manual_test_funding(payload.dict())
    return JSONResponse(jsonable_encoder(result))


@app.exception_handler(Exception)
async def unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(
        {"errors": [str(exc)], "detail": "internal_error"},
        status_code=500,
    )

@app.get("/api/manual/exec")
async def manual_exec_list() -> JSONResponse:
    result = await service.manual_exec_runs()
    return JSONResponse(jsonable_encoder(result))

@app.get("/api/manual/exec/{exec_id}")
async def manual_exec_status(exec_id: str) -> JSONResponse:
    result = await service.manual_exec_status(exec_id)
    if result.get("error") and not result.get("execution_id"):
        raise HTTPException(status_code=404, detail=result["error"])
    return JSONResponse(jsonable_encoder(result))

@app.get("/api/manual/exec/{exec_id}/log")
async def manual_exec_log(exec_id: str):
    result = await service.manual_exec_log(exec_id)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return PlainTextResponse(result.get("log") or "")

@app.post("/api/manual/exec/{exec_id}/stop")
async def manual_exec_stop(exec_id: str) -> JSONResponse:
    result = await service.manual_exec_stop(exec_id)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return JSONResponse(jsonable_encoder(result))
