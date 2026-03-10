"""
webapp/main.py
==============
FastAPI application — serves the PropFinder dashboard.

Endpoints
---------
GET  /                       → dashboard HTML
GET  /api/opportunities      → list opportunities from DB
GET  /api/listings           → all listings (with filters)
GET  /api/neighborhoods      → neighborhood list + avg prices
GET  /api/config             → current app_config.json
POST /api/config             → save new config
POST /api/scrape             → launch scraping job in background
GET  /api/scrape/status      → status of running/last job
WS   /ws/scrape-progress     → real-time scraping progress stream
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Make sure the project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.opportunity_detector import detect_opportunities
from analysis.price_calculator import compute_market_averages
from config.config_loader import get_config, save_config
from database.db import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webapp")

app = FastAPI(title="PropFinder", version="1.0")

@app.on_event("startup")
async def _on_startup():
    global _event_loop
    _event_loop = asyncio.get_running_loop()


# Serve static files (index.html, etc.)
_STATIC = Path(__file__).parent / "static"
_STATIC.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

# ---------------------------------------------------------------------------
# Scraping job state
# ---------------------------------------------------------------------------

_job: dict[str, Any] = {
    "status": "idle",          # idle | running | done | error
    "started_at": None,
    "finished_at": None,
    "progress": [],            # list of log lines
    "result": None,
}
_job_lock = threading.Lock()
_ws_clients: list[WebSocket] = []
_event_loop: asyncio.AbstractEventLoop | None = None


# ---------------------------------------------------------------------------
# HTML entrypoint
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def root():
    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Config endpoints
# ---------------------------------------------------------------------------

@app.get("/api/config")
async def api_get_config():
    return get_config()


@app.post("/api/config")
async def api_save_config(body: dict):
    try:
        # Validate expected keys exist
        assert "surface_weights" in body
        assert "opportunity" in body
        assert "scraping" in body
        save_config(body)
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ---------------------------------------------------------------------------
# Neighborhoods endpoint
# ---------------------------------------------------------------------------

@app.get("/api/neighborhoods")
async def api_neighborhoods():
    nb_path = Path(__file__).parent.parent / "config" / "neighborhood_prices.json"
    prices = json.loads(nb_path.read_text(encoding="utf-8"))
    return {
        "neighborhoods": sorted(prices.keys()),
        "avg_prices": prices,
    }


# ---------------------------------------------------------------------------
# Listings endpoint
# ---------------------------------------------------------------------------

@app.get("/api/listings")
async def api_listings(
    neighborhood: Optional[str] = None,
    source: Optional[str] = None,
    min_discount: Optional[float] = None,
    condition: Optional[str] = None,
    pricing: str = "referencia",
):
    try:
        with DatabaseManager() as db:
            listings = db.get_all_listings()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    nb_path = Path(__file__).parent.parent / "config" / "neighborhood_prices.json"
    fallback = json.loads(nb_path.read_text(encoding="utf-8"))

    if pricing == "mercado":
        from analysis.price_calculator import compute_market_averages
        avg_prices = compute_market_averages(listings, fallback)
    else:
        avg_prices = fallback

    result = []
    for l in listings:
        avg = avg_prices.get(l.neighborhood, 0)
        discount_pct = round((1 - l.price_m2 / avg) * 100, 1) if avg > 0 else None
        row = {
            "id": l.id,
            "source": l.source,
            "title": l.title,
            "price_usd": l.price_usd,
            "surface_m2": l.surface_m2,
            "rooms": l.rooms,
            "neighborhood": l.neighborhood,
            "price_m2": round(l.price_m2, 0),
            "avg_price_m2": round(avg, 0) if avg > 0 else None,
            "discount_percentage": discount_pct,
            "url": l.url,
            "condition": l.condition,
            "first_seen": l.first_seen.isoformat() if l.first_seen else None,
            "last_seen": l.last_seen.isoformat() if l.last_seen else None,
        }
        if neighborhood and l.neighborhood != neighborhood:
            continue
        if source and l.source != source:
            continue
        if condition and l.condition != condition:
            continue
        result.append(row)

    return {"listings": result, "total": len(result)}


# ---------------------------------------------------------------------------
# Opportunities endpoint
# ---------------------------------------------------------------------------

@app.get("/api/opportunities")
async def api_opportunities(pricing: str = "referencia"):
    try:
        if pricing == "mercado":
            nb_path = Path(__file__).parent.parent / "config" / "neighborhood_prices.json"
            fallback = json.loads(nb_path.read_text(encoding="utf-8"))
            with DatabaseManager() as db:
                listings = db.get_all_listings()
            averages = compute_market_averages(listings, fallback)
            raw_opps = detect_opportunities(listings, averages)
            rows = [
                {
                    "opp_id":              opp["listing"].id,
                    "discount_percentage": opp["discount_percentage"],
                    "detected_at":         None,
                    "listing_id":          opp["listing"].id,
                    "source":              opp["listing"].source,
                    "title":               opp["listing"].title,
                    "price_usd":           opp["listing"].price_usd,
                    "surface_m2":          opp["listing"].surface_m2,
                    "rooms":               opp["listing"].rooms,
                    "neighborhood":        opp["listing"].neighborhood,
                    "price_m2":            round(opp["listing"].price_m2, 0),
                    "url":                 opp["listing"].url,
                    "avg_price_m2":        opp["avg_price_m2"],
                    "condition":            opp["listing"].condition,
                }
                for opp in raw_opps
            ]
            return {"opportunities": rows, "total": len(rows)}
        else:
            nb_path = Path(__file__).parent.parent / "config" / "neighborhood_prices.json"
            ref_prices = json.loads(nb_path.read_text(encoding="utf-8"))
            with DatabaseManager() as db:
                listings = db.get_all_listings()
            raw_opps = detect_opportunities(listings, ref_prices)
            rows = [
                {
                    "opp_id":              opp["listing"].id,
                    "discount_percentage": opp["discount_percentage"],
                    "detected_at":         None,
                    "listing_id":          opp["listing"].id,
                    "source":              opp["listing"].source,
                    "title":               opp["listing"].title,
                    "price_usd":           opp["listing"].price_usd,
                    "surface_m2":          opp["listing"].surface_m2,
                    "rooms":               opp["listing"].rooms,
                    "neighborhood":        opp["listing"].neighborhood,
                    "price_m2":            round(opp["listing"].price_m2, 0),
                    "url":                 opp["listing"].url,
                    "avg_price_m2":        opp["avg_price_m2"],
                    "condition":           opp["listing"].condition,
                }
                for opp in raw_opps
            ]
            return {"opportunities": rows, "total": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Scraping endpoints
# ---------------------------------------------------------------------------

@app.get("/api/scrape/status")
async def api_scrape_status():
    with _job_lock:
        return dict(_job)


@app.post("/api/scrape")
async def api_scrape(body: dict):
    """
    Launch a scraping job in a background thread.
    Body: { "sources": ["zonaprop"], "neighborhoods": [...], "max_pages": 1 }
    """
    with _job_lock:
        if _job["status"] == "running":
            raise HTTPException(status_code=409, detail="A scraping job is already running.")
        _job.update({
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "progress": [],
            "result": None,
        })

    sources       = body.get("sources", ["zonaprop"])
    neighborhoods = body.get("neighborhoods", get_config()["scraping"]["neighborhoods"])
    max_pages     = int(body.get("max_pages", 1))

    thread = threading.Thread(
        target=_run_scraping,
        args=(sources, neighborhoods, max_pages),
        daemon=True,
    )
    thread.start()
    return {"ok": True, "message": "Scraping started."}


def _emit(msg: str) -> None:
    """Append a log line to the job progress and broadcast to all WebSocket clients."""
    with _job_lock:
        _job["progress"].append(msg)
    # Schedule broadcast on the main event loop from this background thread
    if _event_loop is not None:
        asyncio.run_coroutine_threadsafe(_broadcast(msg), _event_loop)


async def _broadcast(msg: str) -> None:
    dead = []
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        if ws in _ws_clients:
            _ws_clients.remove(ws)


def _run_scraping(sources: list[str], neighborhoods: list[str], max_pages: int) -> None:
    """Background thread: runs scrapers and persists results."""
    from analysis.opportunity_detector import detect_opportunities, print_opportunity
    from analysis.price_calculator import load_neighborhood_averages
    from scrapers.zonaprop_scraper import ZonapropScraper

    try:
        all_listings = []

        if "zonaprop" in sources:
            _emit("▶ Iniciando Zonaprop scraper...")
            scraper = ZonapropScraper(
                neighborhoods=neighborhoods,
                max_pages=max_pages,
                headless=True,
            )

            # Monkey-patch to intercept per-neighbourhood logs
            import scrapers.zonaprop_scraper as _zp_mod
            _orig_info = logging.getLogger("scrapers.zonaprop_scraper").info

            def _patched_info(msg, *args, **kwargs):
                _orig_info(msg, *args, **kwargs)
                _emit(msg % args if args else msg)

            logging.getLogger("scrapers.zonaprop_scraper").info = _patched_info  # type: ignore

            listings = scraper.scrape()
            logging.getLogger("scrapers.zonaprop_scraper").info = _orig_info  # type: ignore

            all_listings.extend(listings)
            _emit(f"✓ Zonaprop: {len(listings)} propiedades válidas.")

        _emit("💾 Guardando en base de datos...")
        neighbourhood_averages = load_neighborhood_averages()
        opportunities = detect_opportunities(all_listings, neighbourhood_averages)

        saved = 0
        saved_opps = 0
        with DatabaseManager() as db:
            db.initialize_schema()
            db.update_neighborhood_prices(neighbourhood_averages)
            for lst in all_listings:
                if lst.is_valid():
                    listing_id = db.upsert_listing(lst)
                    if listing_id:
                        lst.id = listing_id
                        saved += 1
            for opp in opportunities:
                lst = opp["listing"]
                if lst.id:
                    db.save_opportunity(lst.id, opp["discount_percentage"])
                    saved_opps += 1

        opp_count = saved_opps
        _emit(f"✓ {saved}/{len(all_listings)} guardadas · {opp_count} oportunidades detectadas.")

        with _job_lock:
            _job["status"] = "done"
            _job["finished_at"] = datetime.now().isoformat()
            _job["result"] = {"listings": saved, "opportunities": opp_count}

    except Exception as exc:
        logger.exception("Scraping job failed")
        _emit(f"❌ Error: {exc}")
        with _job_lock:
            _job["status"] = "error"
            _job["finished_at"] = datetime.now().isoformat()


# ---------------------------------------------------------------------------
# WebSocket for real-time progress
# ---------------------------------------------------------------------------

@app.websocket("/ws/scrape-progress")
async def ws_scrape_progress(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.append(websocket)
    # Send existing log lines on connect (catch up)
    with _job_lock:
        history = list(_job["progress"])
    for line in history:
        await websocket.send_text(line)
    try:
        while True:
            await asyncio.sleep(30)  # keep-alive
    except WebSocketDisconnect:
        if websocket in _ws_clients:
            _ws_clients.remove(websocket)
