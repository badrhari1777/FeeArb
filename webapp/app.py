from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from project_settings import MIN_REFRESH_SECONDS, MAX_REFRESH_SECONDS, SettingsManager

from .services import DataService
from .realtime import ConnectionManager

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Funding Arbitrage Monitor", version="0.1.0")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

settings_manager = SettingsManager()
service = DataService(settings_manager=settings_manager)
realtime_manager = ConnectionManager()
service.attach_realtime(realtime_manager)

class SettingsPayload(BaseModel):
    sources: Dict[str, bool]
    exchanges: Dict[str, bool]
    parser_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)
    table_refresh_seconds: int = Field(..., ge=MIN_REFRESH_SECONDS, le=MAX_REFRESH_SECONDS)


@app.on_event("startup")
async def startup_event() -> None:
    await service.startup()

@app.on_event("shutdown")
async def shutdown_event() -> None:
    await service.shutdown()

@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    state = service.state_payload()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "state": state,
        },
    )

@app.get("/api/snapshot")
async def snapshot_api() -> JSONResponse:
    return JSONResponse(service.state_payload())

@app.post("/api/refresh")
async def refresh_snapshot() -> JSONResponse:
    result = await service.refresh_snapshot()
    return JSONResponse({"status": result, "state": service.state_payload()})

@app.get("/api/settings")
async def get_settings() -> JSONResponse:
    return JSONResponse({"settings": settings_manager.as_dict()})

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


@app.websocket("/ws/telemetry")
async def telemetry_ws(websocket: WebSocket) -> None:
    await realtime_manager.connect(websocket)
    try:
        for entry in service.telemetry_backlog():
            await websocket.send_json(entry)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await realtime_manager.disconnect(websocket)
    except Exception:
        await realtime_manager.disconnect(websocket)
