"""
YouTube Channel Scraper - FastAPI Application
A beautiful web app to scrape and manage YouTube channel data.
"""
import os
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

import database as db
import scraper

load_dotenv()

# Scheduler instance
scheduler = BackgroundScheduler()
scraper_running = False


def scheduled_scrape():
    """Run the scraper on schedule."""
    global scraper_running
    if scraper_running:
        print("Scraper already running, skipping...")
        return
    
    scraper_running = True
    try:
        print(f"[{datetime.now()}] Starting scheduled scrape...")
        result = scraper.run_scraper()
        print(f"[{datetime.now()}] Scrape completed: {result['message']}")
    finally:
        scraper_running = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the app."""
    # Startup
    db.init_db()
    
    # Start scheduler
    interval_hours = int(os.getenv("SCHEDULER_INTERVAL_HOURS", "1"))
    scheduler.add_job(
        scheduled_scrape,
        trigger=IntervalTrigger(hours=interval_hours),
        id="youtube_scraper",
        name="YouTube Channel Scraper",
        replace_existing=True
    )
    scheduler.start()
    print(f"Scheduler started (runs every {interval_hours} hour(s))")
    
    yield
    
    # Shutdown
    scheduler.shutdown()


app = FastAPI(
    title="YouTube Channel Scraper",
    description="Scrape and manage YouTube channels for influencer discovery",
    version="1.0.0",
    lifespan=lifespan
)

# Static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# Pydantic models
class SearchQueryCreate(BaseModel):
    query: str
    max_results: int = 25
    region_code: str = "US"


class SearchQueryUpdate(BaseModel):
    query: Optional[str] = None
    max_results: Optional[int] = None
    region_code: Optional[str] = None
    is_active: Optional[bool] = None


# Routes
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Render the main dashboard."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/stats")
async def get_stats():
    """Get dashboard statistics."""
    return db.get_stats()


@app.get("/api/channels")
async def get_channels(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    search: str = Query(""),
    country: str = Query(""),
    language: str = Query(""),
    min_subs: int = Query(0, ge=0),
    max_subs: int = Query(0, ge=0)
):
    """Get paginated channel list with filters."""
    channels = db.get_all_channels(limit, offset, search, country, language, min_subs, max_subs)
    total = db.get_channel_count(search, country, language, min_subs, max_subs)
    return {
        "channels": channels,
        "total": total,
        "limit": limit,
        "offset": offset
    }


@app.get("/api/filters")
async def get_filter_options():
    """Get available filter options."""
    # Priority countries (USA, India, Peru at top)
    priority_countries = ["US", "IN", "PE"]
    
    # Full list of 25 major countries
    all_countries = [
        "US", "IN", "PE", "GB", "CA", "AU", "DE", "FR", "BR", "MX",
        "ES", "IT", "NL", "PL", "AR", "CO", "CL", "JP", "KR", "ID",
        "PH", "TH", "VN", "MY", "SG"
    ]
    
    country_names = {
        "US": "United States", "IN": "India", "PE": "Peru", "GB": "United Kingdom",
        "CA": "Canada", "AU": "Australia", "DE": "Germany", "FR": "France",
        "BR": "Brazil", "MX": "Mexico", "ES": "Spain", "IT": "Italy",
        "NL": "Netherlands", "PL": "Poland", "AR": "Argentina", "CO": "Colombia",
        "CL": "Chile", "JP": "Japan", "KR": "South Korea", "ID": "Indonesia",
        "PH": "Philippines", "TH": "Thailand", "VN": "Vietnam", "MY": "Malaysia",
        "SG": "Singapore"
    }
    
    languages = ["english", "hindi", "spanish", "portuguese", "french", "german", 
                 "japanese", "korean", "indonesian", "thai", "vietnamese"]
    
    subscriber_ranges = [
        {"label": "All", "min": 0, "max": 0},
        {"label": "< 1K", "min": 0, "max": 1000},
        {"label": "1K - 10K", "min": 1000, "max": 10000},
        {"label": "10K - 100K", "min": 10000, "max": 100000},
        {"label": "100K - 1M", "min": 100000, "max": 1000000},
        {"label": "> 1M", "min": 1000000, "max": 0}
    ]
    
    return {
        "countries": [{"code": c, "name": country_names.get(c, c)} for c in all_countries],
        "languages": languages,
        "subscriber_ranges": subscriber_ranges
    }


@app.delete("/api/channels/{channel_id}")
async def delete_channel(channel_id: str):
    """Delete a channel."""
    if db.delete_channel(channel_id):
        return {"success": True, "message": "Channel deleted"}
    raise HTTPException(status_code=404, detail="Channel not found")


@app.get("/api/queries")
async def get_queries():
    """Get all search queries."""
    return db.get_search_queries()


@app.post("/api/queries")
async def create_query(query: SearchQueryCreate):
    """Create a new search query."""
    query_id = db.add_search_query(query.query, query.max_results, query.region_code)
    return {"success": True, "id": query_id}


@app.put("/api/queries/{query_id}")
async def update_query(query_id: int, query: SearchQueryUpdate):
    """Update a search query."""
    if db.update_search_query(
        query_id,
        query.query,
        query.max_results,
        query.region_code,
        query.is_active
    ):
        return {"success": True}
    raise HTTPException(status_code=404, detail="Query not found")


@app.delete("/api/queries/{query_id}")
async def delete_query(query_id: int):
    """Delete a search query."""
    if db.delete_search_query(query_id):
        return {"success": True}
    raise HTTPException(status_code=404, detail="Query not found")


@app.post("/api/queries/reset")
async def reset_queries():
    """Reset search queries to creator-focused defaults."""
    count = db.reset_search_queries_to_creator_focused()
    return {"success": True, "message": f"Reset to {count} creator-focused queries"}


@app.delete("/api/queries")
async def clear_all_queries():
    """Clear all search queries."""
    with db.get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM search_queries")
        count = cursor.rowcount
        conn.commit()
    return {"success": True, "cleared": count, "message": f"Cleared {count} queries"}


class BulkQueriesRequest(BaseModel):
    queries: str  # Newline-separated queries
    max_results: int = 25
    region_code: str = "US"
    clear_existing: bool = False


@app.post("/api/queries/bulk")
async def add_bulk_queries(request: BulkQueriesRequest):
    """Add multiple queries at once (newline-separated)."""
    # Parse queries from text
    lines = request.queries.strip().split("\n")
    queries = [line.strip() for line in lines if line.strip()]
    
    if not queries:
        return {"success": False, "message": "No queries provided"}
    
    # Clear existing if requested
    if request.clear_existing:
        with db.get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM search_queries")
            conn.commit()
    
    # Add each query
    added = 0
    for query in queries:
        try:
            db.add_search_query(query, request.max_results, request.region_code)
            added += 1
        except Exception:
            pass  # Skip duplicates or errors
    
    return {"success": True, "added": added, "message": f"Added {added} queries"}


class ScrapeRequest(BaseModel):
    clear_previous: bool = False
    countries: list = ["US"]
    languages: list = ["english"]
    min_subscribers: int = 0


@app.post("/api/scrape")
async def trigger_scrape(request: ScrapeRequest = ScrapeRequest()):
    """Manually trigger a scrape with filters."""
    global scraper_running
    
    if scraper_running:
        return {"success": False, "message": "Scraper is already running"}
    
    # Run scraper with filters
    scraper_running = True
    try:
        result = scraper.run_scraper(
            clear_previous=request.clear_previous,
            countries=request.countries,
            languages=request.languages,
            min_subscribers=request.min_subscribers
        )
        return result
    finally:
        scraper_running = False


@app.delete("/api/channels")
async def clear_all_channels():
    """Clear all channels from the database."""
    count = db.clear_all_channels()
    return {"success": True, "cleared": count, "message": f"Cleared {count} channels"}


@app.get("/api/scrape/status")
async def scrape_status():
    """Get current scrape status."""
    return {
        "running": scraper_running,
        "scheduler_running": scheduler.running,
        "next_run": str(scheduler.get_job("youtube_scraper").next_run_time) if scheduler.get_job("youtube_scraper") else None
    }


@app.get("/api/history")
async def get_history():
    """Get scrape history."""
    return db.get_scrape_history()


@app.get("/api/export")
async def export_channels(format: str = Query("csv")):
    """Export channels as CSV."""
    channels = db.get_all_channels(limit=10000)
    
    if format == "csv":
        import csv
        import io
        
        output = io.StringIO()
        if channels:
            writer = csv.DictWriter(output, fieldnames=channels[0].keys())
            writer.writeheader()
            writer.writerows(channels)
        
        return JSONResponse(
            content={"csv": output.getvalue(), "count": len(channels)},
            headers={"Content-Type": "application/json"}
        )
    
    return {"channels": channels, "count": len(channels)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
