from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .services import DataService

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Funding Arbitrage Monitor", version="0.1.0")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

service = DataService()


@app.on_event("startup")
async def startup_event() -> None:
    await service.startup()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    await service.shutdown()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    snapshot = service.latest_snapshot()
    if snapshot is None:
        return templates.TemplateResponse(
            "loading.html",
            {"request": request},
        )

    snapshot_payload = snapshot.as_dict()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "snapshot": snapshot,
            "snapshot_payload": snapshot_payload,
            "screener_rows": snapshot.screener_rows[:10],
            "coinglass_rows": snapshot.coinglass_rows[:10],
            "universe_rows": snapshot.universe,
            "opportunity_rows": snapshot.opportunities,
            "refresh_interval": service.refresh_interval,
        },
    )


@app.get("/api/snapshot")
async def snapshot_api() -> JSONResponse:
    payload = service.latest_snapshot_dict()
    if payload is None:
        return JSONResponse({"status": "pending"})
    return JSONResponse(payload)


@app.post("/api/refresh")
async def refresh_snapshot() -> JSONResponse:
    await service.refresh_snapshot()
    return JSONResponse({"status": "refreshed"})

