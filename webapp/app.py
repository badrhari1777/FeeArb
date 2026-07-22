from __future__ import annotations

import asyncio
import logging
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

BASE_DIR = Path(__file__).resolve().parent
setup_logging(BASE_DIR.parent / "logs")

STATIC_VERSION = "v2026-07-12-01"

app = FastAPI(title="Funding Arbitrage Monitor", version="0.1.0")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

settings_manager = SettingsManager()
service = DataService(settings_manager=settings_manager)
bybit_pump_short_lab = BybitPumpShortLab()
logger = logging.getLogger(__name__)


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
    sources: Dict[str, bool]
    exchanges: Dict[str, bool]
    analysis_exchanges: Dict[str, bool]
    parser_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    exchange_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    table_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    account_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    positions_market_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    summary_refresh_seconds: Optional[int] = Field(
        default=None, ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS
    )
    protective: Optional[Dict[str, object]] = None
    manual: Optional[Dict[str, object]] = None


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
    data = payload.dict()
    provided_fields = getattr(payload, "__fields_set__", None)
    if provided_fields is None:
        provided_fields = getattr(payload, "model_fields_set", set())
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
        if not isinstance(current_runtime, (int, float)) or current_runtime < 600:
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


class AutoStrategyPayload(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    type: str
    symbol: str
    long_exchange: str
    short_exchange: str
    enabled: bool = True
    steps: list[Dict[str, object]]


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


class ManualTestCoinAnalysisPayload(BaseModel):
    symbol: str
    window_minutes: Optional[int] = Field(default=4320, ge=60, le=4320)
    funding_points: Optional[int] = Field(default=120, ge=24, le=200)
    include_series: bool = False


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


class NotificationTestPayload(BaseModel):
    title: Optional[str] = "FeeArb test notification"
    message: Optional[str] = "FeeArb notification test from backend."


class AutoExitDefaultsPayload(BaseModel):
    max_runtime_sec: Optional[int] = None
    cooldown_sec: Optional[int] = None
    require_live: Optional[bool] = None
    auto_clear_no_position_sec: Optional[int] = None
    restore_spread_on_missing: Optional[bool] = None
    clear_verified_missing: Optional[bool] = None
    verified_missing_confirmations: Optional[int] = Field(default=None, ge=2)
    position_mode: Optional[str] = None
    spread_confirm_cycles: Optional[int] = Field(default=None, ge=1)


class AutoExitRulePayload(BaseModel):
    symbol: str
    long_exchange: str
    short_exchange: str
    enabled: Optional[bool] = True
    spread_enabled: Optional[bool] = None
    v1_enabled: Optional[bool] = None
    target_spread_pct: Optional[float] = None
    exit_percent: Optional[float] = Field(default=None, gt=0, le=100)
    exit_once: Optional[bool] = None
    position_mode: Optional[str] = None
    spread_confirm_cycles: Optional[int] = Field(default=None, ge=1)


class AutoExitClearSpreadPayload(BaseModel):
    symbol: Optional[str] = None
    clear_v1: Optional[bool] = False


class HedgeClusterRulePayload(BaseModel):
    symbol: str
    kind: Optional[str] = "hedged_pair"
    long_exchange: Optional[str] = None
    short_exchange: Optional[str] = None
    exchange: Optional[str] = None
    side: Optional[str] = None
    enabled: Optional[bool] = True
    qty_tolerance_pct: Optional[float] = None
    rehedge_allowed: Optional[bool] = None


class CoinPaperEnterPayload(BaseModel):
    symbol: str
    qty: float = Field(..., gt=0)
    pair_key: Optional[str] = None
    direction: Optional[str] = None
    action: Optional[str] = None
    note: Optional[str] = None
    source: Optional[str] = None
    window_minutes: Optional[int] = Field(default=240, ge=60, le=4320)
    funding_points: Optional[int] = Field(default=96, ge=24, le=200)


class CoinPaperActionPayload(BaseModel):
    position_key: str
    action: str
    qty: Optional[float] = Field(default=None, gt=0)
    fraction: Optional[float] = Field(default=None, ge=0, le=1)


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
    await service.shutdown()


@app.get("/favicon.ico")
async def favicon() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "favicon.svg", media_type="image/svg+xml")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    state = service.state_payload()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "state": state,
            "static_version": STATIC_VERSION,
        },
    )

@app.get("/coin/{symbol}", response_class=HTMLResponse)
async def coin_analysis_page(
    request: Request,
    symbol: str,
    window_minutes: int = 4320,
    funding_points: int = 120,
) -> HTMLResponse:
    settings = settings_manager.as_dict()
    symbol_session = None
    try:
        symbol_session = await service.bootstrap_symbol_session(symbol)
    except ValueError:
        symbol_session = None
    return templates.TemplateResponse(
        "coin.html",
        {
            "request": request,
            "symbol": symbol,
            "window_minutes": window_minutes,
            "funding_points": funding_points,
            "static_version": STATIC_VERSION,
            "settings": settings,
            "symbol_session": symbol_session,
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
        "funding_history.html",
        {
            "request": request,
            "symbol": symbol,
            "initial": initial,
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/pump-short-lab", response_class=HTMLResponse)
async def pump_short_lab_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "pump_short_lab.html",
        {
            "request": request,
            "initial": bybit_pump_short_lab.status(),
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/pump-short", response_class=HTMLResponse)
async def pump_short_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "pump_short.html",
        {
            "request": request,
            "initial": bybit_pump_short_lab.pump_dashboard_status(),
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/pump-short-strategies", response_class=HTMLResponse)
async def pump_short_strategies_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "pump_short_strategies.html",
        {
            "request": request,
            "initial": bybit_pump_short_lab.strategy_monitor_status(),
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/manual", response_class=HTMLResponse)
async def manual_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    return templates.TemplateResponse(
        "manual.html",
        {
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
        "auto_arbitrage.html",
        {
            "request": request,
            "initial": {
                "exchanges": exchanges,
                "rules": service.auto_arb_payload().get("rules", []),
            },
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/strategies", response_class=HTMLResponse)
async def strategies_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    exchanges = [
        name
        for name, enabled in (settings.get("analysis_exchanges") or {}).items()
        if enabled
    ]
    return templates.TemplateResponse(
        "strategies.html",
        {
            "request": request,
            "initial": {
                "exchanges": exchanges,
                "payload": service.auto_strategy_payload(),
            },
            "static_version": STATIC_VERSION,
        },
    )


@app.get("/manual-tests", response_class=HTMLResponse)
async def manual_tests_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    return templates.TemplateResponse(
        "manual_tests.html",
        {
            "request": request,
            "settings": settings,
            "static_version": STATIC_VERSION,
        },
    )

@app.get("/spread-monitor", response_class=HTMLResponse)
async def spread_monitor_page(request: Request) -> HTMLResponse:
    settings = settings_manager.as_dict()
    return templates.TemplateResponse(
        "spread_monitor.html",
        {
            "request": request,
            "settings": settings,
            "static_version": STATIC_VERSION,
        },
    )

@app.get("/api/snapshot")
async def snapshot_api() -> JSONResponse:
    return JSONResponse(service.state_payload())

@app.get("/api/coin/sessions")
async def coin_sessions_api() -> JSONResponse:
    sessions = await service.list_active_coin_symbol_sessions()
    return JSONResponse({"sessions": sessions})


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


@app.post("/api/coin/sessions/start")
async def coin_sessions_start_api(
    symbol: str,
    ttl_sec: int | None = None,
) -> JSONResponse:
    try:
        payload = await service.start_coin_symbol_session(symbol, ttl_sec=ttl_sec)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/coin/sessions/extend")
async def coin_sessions_extend_api(
    symbol: str,
    ttl_sec: int | None = None,
) -> JSONResponse:
    try:
        payload = await service.extend_coin_symbol_session(symbol, ttl_sec=ttl_sec)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/coin/sessions/stop")
async def coin_sessions_stop_api(symbol: str) -> JSONResponse:
    try:
        payload = await service.stop_coin_symbol_session(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))

@app.get("/api/coin/{symbol}")
async def coin_analysis_api(
    symbol: str,
    window_minutes: int = 4320,
    funding_points: int = 120,
) -> JSONResponse:
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol is required")
    try:
        payload = await service.analyze_symbol(
            symbol,
            window_minutes=window_minutes,
            funding_points=funding_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(payload)


@app.get("/api/coin/focus/{symbol}")
async def coin_focus_api(
    symbol: str,
    exchange: str | None = None,
    limit: int = 200,
) -> JSONResponse:
    try:
        payload = await service.get_coin_focus_snapshots(symbol, exchange=exchange, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(payload)


@app.get("/api/coin/history/focus/{symbol}")
async def coin_focus_history_api(
    symbol: str,
    exchange: str | None = None,
    limit: int = 500,
    since_ts_ms: int | None = None,
    until_ts_ms: int | None = None,
) -> JSONResponse:
    try:
        payload = await service.load_focus_history(
            symbol,
            exchange=exchange,
            limit=limit,
            since_ts_ms=since_ts_ms,
            until_ts_ms=until_ts_ms,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/history/bootstrap/{symbol}")
async def coin_bootstrap_history_api(
    symbol: str,
    exchange: str | None = None,
    funding_limit: int = 500,
    oi_limit: int = 500,
    since_ts_ms: int | None = None,
    until_ts_ms: int | None = None,
) -> JSONResponse:
    try:
        payload = await service.load_bootstrap_history(
            symbol,
            exchange=exchange,
            funding_limit=funding_limit,
            oi_limit=oi_limit,
            since_ts_ms=since_ts_ms,
            until_ts_ms=until_ts_ms,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/context/{symbol}")
async def coin_symbol_context_api(
    symbol: str,
    focus_limit: int = 500,
    funding_limit: int = 500,
    oi_limit: int = 500,
    decision_limit: int = 500,
    outcome_limit: int = 500,
    real_obs_limit: int = 500,
) -> JSONResponse:
    try:
        payload = await service.load_symbol_context(
            symbol,
            focus_limit=focus_limit,
            funding_limit=funding_limit,
            oi_limit=oi_limit,
            decision_limit=decision_limit,
            outcome_limit=outcome_limit,
            real_obs_limit=real_obs_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/positions-watcher/status")
async def coin_positions_watcher_status_api(
    symbol: str | None = None,
) -> JSONResponse:
    try:
        payload = await service.get_coin_position_watcher_status(symbol=symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/coin/positions-watcher/run")
async def coin_positions_watcher_run_api(
    force: bool = False,
    symbols: str = "",
    window_minutes: int = 240,
    funding_points: int = 96,
) -> JSONResponse:
    symbol_list = [item.strip() for item in str(symbols or "").split(",") if item.strip()]
    payload = await service.run_coin_position_watcher_once(
        force=force,
        symbols=(symbol_list or None),
        window_minutes=window_minutes,
        funding_points=funding_points,
    )
    status = await service.get_coin_position_watcher_status()
    return JSONResponse(jsonable_encoder({"cycle": payload, "status": status}))


@app.post("/api/coin/positions-watcher/enabled")
async def coin_positions_watcher_enabled_api(
    enabled: bool = True,
) -> JSONResponse:
    payload = await service.set_coin_position_watcher_enabled(enabled)
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/export/{symbol}")
async def coin_export_json_api(
    symbol: str,
    include_live_analysis: bool = True,
    window_minutes: int = 240,
    funding_points: int = 96,
) -> JSONResponse:
    try:
        payload = await service.export_coin_analysis_json(
            symbol,
            include_live_analysis=include_live_analysis,
            window_minutes=window_minutes,
            funding_points=funding_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/export/{symbol}/timeline.csv")
async def coin_export_timeline_csv_api(
    symbol: str,
    include_live_analysis: bool = True,
    window_minutes: int = 240,
    funding_points: int = 96,
) -> PlainTextResponse:
    try:
        csv_data = await service.export_coin_timeline_csv(
            symbol,
            include_live_analysis=include_live_analysis,
            window_minutes=window_minutes,
            funding_points=funding_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    safe_symbol = str(symbol or "symbol").upper()
    headers = {
        "Content-Disposition": f'attachment; filename="coin_timeline_{safe_symbol}.csv"',
    }
    return PlainTextResponse(csv_data, headers=headers)


@app.get("/api/coin/export/{symbol}/timeline.parquet")
async def coin_export_timeline_parquet_api(
    symbol: str,
    include_live_analysis: bool = True,
    window_minutes: int = 240,
    funding_points: int = 96,
) -> Response:
    try:
        parquet_bytes = await service.export_coin_timeline_parquet(
            symbol,
            include_live_analysis=include_live_analysis,
            window_minutes=window_minutes,
            funding_points=funding_points,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    safe_symbol = str(symbol or "symbol").upper()
    headers = {
        "Content-Disposition": f'attachment; filename="coin_timeline_{safe_symbol}.parquet"',
    }
    return Response(
        content=parquet_bytes,
        media_type="application/x-parquet",
        headers=headers,
    )


@app.get("/api/coin/review/weekly")
async def coin_review_weekly_api(
    days: int = 7,
    top: int = 3,
) -> JSONResponse:
    payload = await service.get_coin_weekly_review(days=days, top=top)
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/review/weekly.csv")
async def coin_review_weekly_csv_api(
    days: int = 7,
    top: int = 3,
) -> PlainTextResponse:
    csv_data = await service.export_coin_review_csv(days=days, top=top)
    headers = {
        "Content-Disposition": 'attachment; filename="coin_review_weekly.csv"',
    }
    return PlainTextResponse(csv_data, headers=headers)


@app.get("/api/coin/review/{symbol}")
async def coin_review_symbol_api(
    symbol: str,
    days: int = 7,
    top: int = 3,
    include_live_analysis: bool = False,
) -> JSONResponse:
    try:
        payload = await service.export_coin_review_json(
            symbol=symbol,
            days=days,
            top=top,
            include_live_analysis=include_live_analysis,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/review/{symbol}/timeline.csv")
async def coin_review_symbol_csv_api(
    symbol: str,
    days: int = 7,
    top: int = 3,
) -> PlainTextResponse:
    try:
        csv_data = await service.export_coin_review_csv(
            symbol=symbol,
            days=days,
            top=top,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    safe_symbol = str(symbol or "symbol").upper()
    headers = {
        "Content-Disposition": f'attachment; filename="coin_review_{safe_symbol}.csv"',
    }
    return PlainTextResponse(csv_data, headers=headers)


@app.get("/api/coin/replay/{symbol}")
async def coin_replay_api(
    symbol: str,
    limit: int = 1000,
    since_ts_ms: int | None = None,
    until_ts_ms: int | None = None,
    include_stored_decisions: bool = True,
) -> JSONResponse:
    try:
        payload = await service.replay_coin_candidate_signals(
            symbol,
            limit=limit,
            since_ts_ms=since_ts_ms,
            until_ts_ms=until_ts_ms,
            include_stored_decisions=include_stored_decisions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/outcomes/auto-status")
async def coin_outcomes_auto_status_api(
    symbol: str | None = None,
) -> JSONResponse:
    try:
        payload = await service.get_coin_outcomes_auto_status(symbol=symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/coin/outcomes/auto-run")
async def coin_outcomes_auto_run_api(
    symbol: str | None = None,
) -> JSONResponse:
    try:
        cycle = await service.evaluate_matured_coin_outcomes_once(symbol=symbol)
        status = await service.get_coin_outcomes_auto_status(symbol=symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder({"cycle": cycle, "status": status}))


@app.post("/api/coin/outcomes/auto-scheduler")
async def coin_outcomes_auto_scheduler_api(
    enabled: bool = True,
) -> JSONResponse:
    payload = await service.set_coin_outcomes_scheduler_enabled(enabled)
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/maintenance/retention-status")
async def coin_retention_status_api() -> JSONResponse:
    payload = await service.get_coin_analysis_maintenance_status()
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/coin/maintenance/retention-run")
async def coin_retention_run_api(
    max_age_days: int | None = None,
    closed_paper_days: int | None = None,
) -> JSONResponse:
    payload = await service.run_coin_analysis_retention_once(
        max_age_days=max_age_days,
        closed_paper_days=closed_paper_days,
        reason="manual_api",
    )
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/outcomes/{symbol}")
async def coin_outcomes_api(
    symbol: str,
    limit: int = 500,
    horizons: str = "",
    phase_buckets: str = "",
    actions: str = "",
) -> JSONResponse:
    horizon_list = [part.strip() for part in str(horizons or "").split(",") if part.strip()]
    phase_bucket_list = [part.strip() for part in str(phase_buckets or "").split(",") if part.strip()]
    action_list = [part.strip() for part in str(actions or "").split(",") if part.strip()]
    try:
        payload = await service.get_coin_outcomes(
            symbol,
            limit=limit,
            horizons=horizon_list,
            phase_buckets=phase_bucket_list,
            actions=action_list,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/coin/outcomes/{symbol}/evaluate")
async def coin_outcomes_evaluate_api(
    symbol: str,
    horizons: str = "15m,1h,4h",
    decision_limit: int = 500,
    force: bool = False,
) -> JSONResponse:
    horizon_list = [part.strip() for part in str(horizons or "").split(",") if part.strip()]
    try:
        payload = await service.evaluate_coin_outcomes(
            symbol,
            horizons=horizon_list,
            decision_limit=decision_limit,
            force=force,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/paper/positions")
async def coin_paper_positions_api(
    symbol: str | None = None,
    status: str | None = None,
) -> JSONResponse:
    payload = await service.get_coin_paper_positions(symbol=symbol, status=status)
    return JSONResponse(jsonable_encoder(payload))


@app.get("/api/coin/paper/events/{position_key}")
async def coin_paper_events_api(
    position_key: str,
    limit: int = 200,
) -> JSONResponse:
    try:
        payload = await service.get_coin_paper_events(position_key, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(payload))


@app.post("/api/coin/paper/enter")
async def coin_paper_enter_api(payload: CoinPaperEnterPayload) -> JSONResponse:
    try:
        result = await service.coin_paper_enter(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/coin/paper/action")
async def coin_paper_action_api(payload: CoinPaperActionPayload) -> JSONResponse:
    try:
        result = await service.coin_paper_apply_action(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/refresh")
async def refresh_snapshot() -> JSONResponse:
    result = await service.refresh_snapshot(force_accounts=True)
    return JSONResponse({"status": result, "state": service.state_payload()})

@app.get("/api/settings")
async def get_settings() -> JSONResponse:
    return JSONResponse({"settings": settings_manager.as_dict()})


@app.get("/api/mobile/positions")
async def mobile_positions() -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.mobile_positions_payload()))


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


@app.get("/api/auto-exit")
async def get_auto_exit() -> JSONResponse:
    return JSONResponse(service.auto_exit_payload())


@app.get("/api/auto-arb")
async def get_auto_arb() -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.auto_arb_payload()))


@app.get("/api/strategies")
async def get_auto_strategies() -> JSONResponse:
    return JSONResponse(jsonable_encoder(service.auto_strategy_payload()))


@app.post("/api/strategies/preflight")
async def preflight_auto_strategy(payload: AutoStrategyPayload) -> JSONResponse:
    try:
        result = await service.analyze_auto_strategy(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/strategies")
async def upsert_auto_strategy(payload: AutoStrategyPayload) -> JSONResponse:
    try:
        result = await service.upsert_auto_strategy(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/strategies/{strategy_id}/pause")
async def pause_auto_strategy(strategy_id: str) -> JSONResponse:
    try:
        result = await service.set_auto_strategy_enabled(strategy_id, False)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.post("/api/strategies/{strategy_id}/resume")
async def resume_auto_strategy(strategy_id: str) -> JSONResponse:
    try:
        result = await service.set_auto_strategy_enabled(strategy_id, True)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


@app.delete("/api/strategies/{strategy_id}")
async def delete_auto_strategy(strategy_id: str) -> JSONResponse:
    try:
        result = await service.delete_auto_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(jsonable_encoder(result))


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


@app.get("/api/hedge-clusters")
async def get_hedge_clusters() -> JSONResponse:
    return JSONResponse(service.hedge_cluster_payload())

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
            "state": service.state_payload(),
        }
    )


@app.post("/api/auto-exit/defaults")
async def update_auto_exit_defaults(payload: AutoExitDefaultsPayload) -> JSONResponse:
    try:
        result = await service.update_auto_exit_defaults(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(result)


@app.post("/api/auto-exit/rule")
async def update_auto_exit_rule(payload: AutoExitRulePayload) -> JSONResponse:
    try:
        result = await service.update_auto_exit_rule(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(result)


@app.post("/api/auto-exit/clear-spread-cache")
async def clear_auto_exit_spread_cache(payload: AutoExitClearSpreadPayload) -> JSONResponse:
    result = await service.clear_auto_exit_spread_cache(payload.symbol, clear_v1=bool(payload.clear_v1))
    return JSONResponse(result)


@app.post("/api/hedge-clusters/rule")
async def update_hedge_cluster_rule(payload: HedgeClusterRulePayload) -> JSONResponse:
    try:
        result = await service.update_hedge_cluster_rule(payload.dict(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(result)

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


@app.post("/api/manual/test/coin-analysis")
async def manual_test_coin_analysis(payload: ManualTestCoinAnalysisPayload) -> JSONResponse:
    result = await service.manual_test_coin_analysis(payload.dict())
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
