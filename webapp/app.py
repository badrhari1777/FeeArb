from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from project_settings import MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS, SettingsManager

from .services import DataService
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

BASE_DIR = Path(__file__).resolve().parent

STATIC_VERSION = "v2026-02-15-01"

app = FastAPI(title="Funding Arbitrage Monitor", version="0.1.0")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

settings_manager = SettingsManager()
service = DataService(settings_manager=settings_manager)
logger = logging.getLogger(__name__)

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
    max_slippage_bps: Optional[float] = Field(default=8.0, ge=0)
    spread_min_pct: Optional[float] = None
    spread_max_pct: Optional[float] = None
    timeout_sec: Optional[int] = Field(default=15, ge=0)
    max_runtime_sec: Optional[int] = Field(default=None, ge=1)
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
    limit_offset_bps: Optional[float] = Field(default=None, ge=0)
    limit_offset_ticks: Optional[int] = Field(default=None, ge=0)
    limit_improve_ticks: Optional[int] = Field(default=None, ge=0)
    auto_limit_price: bool = True
    min_level_notional: Optional[float] = Field(default=None, ge=0)
    min_level_qty: Optional[float] = Field(default=None, ge=0)
    min_level_chunk_pct: Optional[float] = Field(default=None, ge=0)
    max_limit_deviation_bps: Optional[float] = Field(default=None, ge=0)
    use_orderbook_check: bool = True
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


class AutoExitDefaultsPayload(BaseModel):
    max_runtime_sec: Optional[int] = None
    cooldown_sec: Optional[int] = None
    require_live: Optional[bool] = None


class AutoExitRulePayload(BaseModel):
    symbol: str
    long_exchange: str
    short_exchange: str
    enabled: Optional[bool] = True
    target_spread_pct: Optional[float] = None


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
    return templates.TemplateResponse(
        "coin.html",
        {
            "request": request,
            "symbol": symbol,
            "window_minutes": window_minutes,
            "funding_points": funding_points,
            "static_version": STATIC_VERSION,
            "settings": settings,
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


@app.get("/mobile", response_class=HTMLResponse)
async def mobile_page(request: Request) -> HTMLResponse:
    state = service.state_payload()
    return templates.TemplateResponse(
        "mobile.html",
        {
            "request": request,
            "state": state,
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

@app.post("/api/refresh")
async def refresh_snapshot() -> JSONResponse:
    result = await service.refresh_snapshot(force_accounts=True)
    return JSONResponse({"status": result, "state": service.state_payload()})

@app.get("/api/settings")
async def get_settings() -> JSONResponse:
    return JSONResponse({"settings": settings_manager.as_dict()})


@app.get("/api/auto-exit")
async def get_auto_exit() -> JSONResponse:
    return JSONResponse(service.auto_exit_payload())

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

@app.post("/api/manual/enter")
async def manual_enter(payload: ManualEnterPayload) -> JSONResponse:
    logger.info("manual enter request %s", payload.dict())
    try:
        result = await service.manual_enter(payload.dict())
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("manual enter failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/manual/exit")
async def manual_exit(payload: ManualExitPayload) -> JSONResponse:
    logger.info("manual exit request %s", payload.dict())
    try:
        result = await service.manual_exit(payload.dict())
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("manual exit failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/manual/roll")
async def manual_roll(payload: ManualRollPayload) -> JSONResponse:
    logger.info("manual roll request %s", payload.dict())
    try:
        result = await service.manual_roll(payload.dict())
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("manual roll failed: %s", exc)
        result = {"errors": [str(exc)]}
    return JSONResponse(jsonable_encoder(result))

@app.post("/api/manual/analyze")
async def manual_analyze(payload: ManualAnalyzePayload) -> JSONResponse:
    logger.info("manual analyze request %s", payload.dict())
    try:
        result = await service.manual_analyze(payload.dict())
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
    if result.get("error"):
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
