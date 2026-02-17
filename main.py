"""
YouTube Channel Scraper - FastAPI Application
A beautiful web app to scrape and manage YouTube channel data.
"""
import os
from datetime import datetime
from typing import Optional, List
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
import email_service
import ai_outreach

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


# ============================================================
# EMAIL & OUTREACH PYDANTIC MODELS
# ============================================================

class EmailAccountCreate(BaseModel):
    email: str
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str
    display_name: str = ""
    daily_limit: int = 100
    skip_test: bool = False


class EmailAccountBulkAdd(BaseModel):
    accounts_text: str  # Format: email,password per line


class CampaignCreate(BaseModel):
    name: str
    brief: str = ""
    budget_min: float = 0
    budget_max: float = 0
    topic: str = ""
    requirements: str = ""
    deadline: str = ""


class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    brief: Optional[str] = None
    budget_min: Optional[float] = None
    budget_max: Optional[float] = None
    topic: Optional[str] = None
    requirements: Optional[str] = None
    deadline: Optional[str] = None
    status: Optional[str] = None


class OutreachCreate(BaseModel):
    campaign_id: int
    channel_ids: list  # List of channel IDs
    email_account_id: int


class GenerateEmailRequest(BaseModel):
    campaign_id: int
    channel_id: str
    recipient_email: str
    email_account_id: int


class SendEmailRequest(BaseModel):
    outreach_id: int


class UpdateChannelEmail(BaseModel):
    email: str


class NegotiationRequest(BaseModel):
    outreach_id: int
    creator_response: str


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


# ============================================================
# EMAIL ACCOUNTS API
# ============================================================

@app.get("/api/email-accounts")
async def get_email_accounts():
    """Get all email accounts."""
    accounts = db.get_email_accounts()
    # Remove passwords from response
    for acc in accounts:
        acc['smtp_password'] = '***hidden***'
    return {"accounts": accounts}


@app.post("/api/email-accounts")
async def create_email_account(account: EmailAccountCreate):
    """Add a new email account."""
    # Auto-detect SMTP if not provided
    if not account.smtp_host:
        config = email_service.get_smtp_config(account.email)
        account.smtp_host = config["host"]
        account.smtp_port = config["port"]
    
    # Use provided smtp_user or default to email
    smtp_user = account.smtp_user if account.smtp_user else account.email
    
    # Test connection (unless skipped)
    if not account.skip_test:
        success, message = email_service.test_smtp_connection(
            account.email, account.smtp_host, account.smtp_port,
            smtp_user, account.smtp_password
        )
        
        if not success:
            raise HTTPException(status_code=400, detail=message)
    
    account_id = db.add_email_account(
        email=account.email,
        smtp_host=account.smtp_host,
        smtp_port=account.smtp_port,
        smtp_user=smtp_user,
        smtp_password=account.smtp_password,
        display_name=account.display_name,
        daily_limit=account.daily_limit
    )
    
    if account_id == -1:
        raise HTTPException(status_code=400, detail="Email already exists")
    
    return {"success": True, "id": account_id, "message": "Email account added"}


@app.post("/api/email-accounts/bulk")
async def bulk_add_email_accounts(request: EmailAccountBulkAdd):
    """Add multiple email accounts at once."""
    results = email_service.bulk_add_email_accounts(request.accounts_text)
    return results


@app.delete("/api/email-accounts/{account_id}")
async def delete_email_account(account_id: int):
    """Delete an email account."""
    if db.delete_email_account(account_id):
        return {"success": True, "message": "Account deleted"}
    raise HTTPException(status_code=404, detail="Account not found")


@app.post("/api/email-accounts/{account_id}/test")
async def test_email_account(account_id: int):
    """Test an email account connection."""
    account = db.get_email_account(account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    success, message = email_service.test_smtp_connection(
        account['email'], account['smtp_host'], account['smtp_port'],
        account['smtp_user'], account['smtp_password']
    )
    
    return {"success": success, "message": message}


# ============================================================
# CAMPAIGNS API
# ============================================================

@app.get("/api/campaigns")
async def get_campaigns(status: Optional[str] = None):
    """Get all campaigns."""
    campaigns = db.get_campaigns(status)
    return {"campaigns": campaigns}


@app.post("/api/campaigns")
async def create_campaign(campaign: CampaignCreate):
    """Create a new campaign."""
    campaign_id = db.create_campaign(
        name=campaign.name,
        brief=campaign.brief,
        budget_min=campaign.budget_min,
        budget_max=campaign.budget_max,
        topic=campaign.topic,
        requirements=campaign.requirements,
        deadline=campaign.deadline
    )
    return {"success": True, "id": campaign_id}


@app.get("/api/campaigns/{campaign_id}")
async def get_campaign(campaign_id: int):
    """Get a single campaign with its outreach emails."""
    campaign = db.get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    outreach = db.get_outreach_emails(campaign_id=campaign_id)
    return {"campaign": campaign, "outreach": outreach}


@app.put("/api/campaigns/{campaign_id}")
async def update_campaign(campaign_id: int, update: CampaignUpdate):
    """Update a campaign."""
    update_data = {k: v for k, v in update.dict().items() if v is not None}
    if db.update_campaign(campaign_id, **update_data):
        return {"success": True}
    raise HTTPException(status_code=404, detail="Campaign not found")


@app.delete("/api/campaigns/{campaign_id}")
async def delete_campaign(campaign_id: int):
    """Delete a campaign."""
    if db.delete_campaign(campaign_id):
        return {"success": True}
    raise HTTPException(status_code=404, detail="Campaign not found")


# ============================================================
# OUTREACH API
# ============================================================

@app.get("/api/outreach")
async def get_all_outreach(campaign_id: Optional[int] = None, status: Optional[str] = None):
    """Get all outreach emails."""
    outreach = db.get_outreach_emails(campaign_id=campaign_id, status=status)
    return {"outreach": outreach}


@app.get("/api/outreach/stats")
async def get_outreach_stats():
    """Get outreach statistics."""
    return db.get_outreach_stats()


@app.post("/api/outreach/generate")
async def generate_outreach_email(request: GenerateEmailRequest):
    """Generate an AI-powered outreach email."""
    # Get campaign details
    campaign = db.get_campaign(request.campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    # Get channel details
    channels = db.get_all_channels(limit=1, search=request.channel_id)
    if not channels:
        raise HTTPException(status_code=404, detail="Channel not found")
    channel = channels[0]
    
    # Generate email using AI
    try:
        email_content = ai_outreach.generate_outreach_email(
            creator_name=channel.get('channel_title', ''),
            channel_title=channel.get('channel_title', ''),
            subscribers=channel.get('subscribers', 0),
            content_focus=channel.get('description', '')[:200],
            campaign_brief=campaign.get('brief', ''),
            budget_min=campaign.get('budget_min', 0),
            budget_max=campaign.get('budget_max', 0),
            topic=campaign.get('topic', ''),
            requirements=campaign.get('requirements', ''),
            deadline=campaign.get('deadline', '')
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Create outreach record
    outreach_id = db.create_outreach(
        campaign_id=request.campaign_id,
        channel_id=request.channel_id,
        recipient_email=request.recipient_email,
        email_account_id=request.email_account_id,
        subject=email_content['subject'],
        body=email_content['body']
    )
    
    return {
        "success": True,
        "outreach_id": outreach_id,
        "email": email_content
    }


@app.post("/api/outreach/{outreach_id}/send")
async def send_outreach(outreach_id: int):
    """Send an outreach email."""
    success, message = email_service.send_outreach_email(outreach_id)
    if success:
        return {"success": True, "message": message}
    raise HTTPException(status_code=400, detail=message)


@app.get("/api/outreach/{outreach_id}")
async def get_outreach_detail(outreach_id: int):
    """Get outreach details with thread."""
    outreach = db.get_outreach(outreach_id)
    if not outreach:
        raise HTTPException(status_code=404, detail="Outreach not found")
    
    thread = db.get_email_thread(outreach_id)
    return {"outreach": outreach, "thread": thread}


@app.post("/api/outreach/{outreach_id}/negotiate")
async def handle_negotiation(outreach_id: int, request: NegotiationRequest):
    """Handle a creator's response with AI negotiation."""
    outreach = db.get_outreach(outreach_id)
    if not outreach:
        raise HTTPException(status_code=404, detail="Outreach not found")
    
    campaign = db.get_campaign(outreach['campaign_id'])
    thread = db.get_email_thread(outreach_id)
    
    # Add creator's response to thread
    db.add_email_thread(
        outreach_id=outreach_id,
        direction='inbound',
        subject=f"Re: {outreach['subject']}",
        body=request.creator_response
    )
    
    # Update status
    db.update_outreach(outreach_id, status='replied', reply_content=request.creator_response)
    
    # Generate AI response
    try:
        ai_response = ai_outreach.generate_negotiation_response(
            conversation_history=[{"direction": t['direction'], "body": t['body']} for t in thread],
            creator_response=request.creator_response,
            campaign_brief=campaign.get('brief', ''),
            budget_min=campaign.get('budget_min', 0),
            budget_max=campaign.get('budget_max', 0),
            negotiation_stage=outreach.get('negotiation_stage', 'initial')
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    # Update negotiation stage
    db.update_outreach(outreach_id, 
                       negotiation_stage=ai_response.get('new_stage', 'negotiating'),
                       ai_response=ai_response.get('response_body', ''))
    
    return {
        "success": True,
        "ai_analysis": ai_response
    }


@app.put("/api/channels/{channel_id}/email")
async def update_channel_email(channel_id: str, update: UpdateChannelEmail):
    """Update email for a channel."""
    if db.update_channel_email(channel_id, update.email):
        return {"success": True}
    raise HTTPException(status_code=404, detail="Channel not found")


class TestEmailRequest(BaseModel):
    to_email: str
    subject: str = "Test Email from YouTube Scraper"
    body: str = "This is a test email. If you received this, the email system is working!"


@app.post("/api/test-email")
async def send_test_email(req: TestEmailRequest):
    """Send a test email."""
    # Get the first active email account
    account = email_service.get_available_account()
    if not account:
        raise HTTPException(status_code=400, detail="No email accounts configured. Please add one first.")
    
    success, message = email_service.send_email(
        account_id=account["id"],
        to_email=req.to_email,
        subject=req.subject,
        body=req.body
    )
    
    if success:
        return {"success": True, "message": f"Test email sent to {req.to_email}"}
    else:
        raise HTTPException(status_code=500, detail=message)


# ============ Mailing List Endpoints ============

class MailingListContact(BaseModel):
    name: str
    email: str
    channel_id: str = None
    channel_title: str = None
    subscribers: int = None
    notes: str = None
    campaign_id: int = None


class MailingListBulkAdd(BaseModel):
    contacts_text: str  # Format: "Name, Email" per line
    campaign_id: int = None


class BulkSendRequest(BaseModel):
    campaign_id: int
    contact_ids: List[int] = None  # If None, send to all pending in campaign


@app.get("/api/mailing-list")
async def get_mailing_list(campaign_id: int = None, status: str = None):
    """Get mailing list contacts."""
    contacts = db.get_mailing_list(campaign_id, status)
    stats = db.get_mailing_list_stats()
    return {"contacts": contacts, "stats": stats}


@app.post("/api/mailing-list")
async def add_to_mailing_list(contact: MailingListContact):
    """Add a contact to mailing list."""
    contact_id = db.add_to_mailing_list(
        name=contact.name,
        email=contact.email,
        channel_id=contact.channel_id,
        channel_title=contact.channel_title,
        subscribers=contact.subscribers,
        notes=contact.notes,
        campaign_id=contact.campaign_id
    )
    return {"success": True, "id": contact_id}


@app.post("/api/mailing-list/bulk")
async def bulk_add_to_mailing_list(data: MailingListBulkAdd):
    """Bulk add contacts to mailing list."""
    contacts = []
    for line in data.contacts_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 2:
            contacts.append({
                "name": parts[0],
                "email": parts[1],
                "channel_title": parts[2] if len(parts) > 2 else None,
                "notes": parts[3] if len(parts) > 3 else None
            })
        elif "@" in parts[0]:
            # Just email
            contacts.append({
                "name": parts[0].split("@")[0],
                "email": parts[0]
            })
    
    added = db.add_bulk_to_mailing_list(contacts, data.campaign_id)
    return {"success": True, "added": added, "total_parsed": len(contacts)}


@app.delete("/api/mailing-list/{contact_id}")
async def delete_mailing_list_contact(contact_id: int):
    """Delete a contact from mailing list."""
    if db.delete_mailing_list_contact(contact_id):
        return {"success": True}
    raise HTTPException(status_code=404, detail="Contact not found")


@app.delete("/api/mailing-list")
async def clear_mailing_list(campaign_id: int = None):
    """Clear mailing list."""
    deleted = db.clear_mailing_list(campaign_id)
    return {"success": True, "deleted": deleted}


@app.post("/api/mailing-list/send-all")
async def send_to_mailing_list(req: BulkSendRequest):
    """Send emails to all contacts in mailing list."""
    # Get campaign
    campaign = db.get_campaign(req.campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    # Get contacts
    if req.contact_ids:
        contacts = [db.get_mailing_list_contact(cid) for cid in req.contact_ids]
        contacts = [c for c in contacts if c]
    else:
        contacts = db.get_mailing_list(campaign_id=req.campaign_id, status="pending")
    
    if not contacts:
        raise HTTPException(status_code=400, detail="No pending contacts to send to")
    
    # Get available email account
    account = email_service.get_available_account()
    if not account:
        raise HTTPException(status_code=400, detail="No email accounts available")
    
    sent_count = 0
    errors = []
    
    for contact in contacts:
        try:
            # Generate AI email for this contact
            email_content = ai_outreach.generate_outreach_email(
                creator_name=contact["name"],
                channel_title=contact.get("channel_title") or contact["name"],
                subscribers=contact.get("subscribers") or 0,
                content_focus="content creation",
                campaign_brief=campaign["brief"] or "",
                budget_min=campaign.get("budget_min") or 100,
                budget_max=campaign.get("budget_max") or 500,
                topic=campaign.get("topic") or "",
                requirements=campaign.get("requirements") or "",
                deadline=campaign.get("deadline") or "",
                sender_name=account.get("display_name") or "Marketing Team"
            )
            
            # Create outreach record
            outreach_id = db.create_outreach(
                campaign_id=req.campaign_id,
                channel_id=contact.get("channel_id"),
                recipient_email=contact["email"],
                email_account_id=account["id"],
                subject=email_content["subject"],
                body=email_content["body"]
            )
            
            # Send email
            success, message = email_service.send_email(
                account_id=account["id"],
                to_email=contact["email"],
                subject=email_content["subject"],
                body=email_content["body"]
            )
            
            if success:
                db.mark_outreach_sent(outreach_id, account["id"])
                db.update_mailing_list_contact(contact["id"], status="sent", outreach_id=outreach_id)
                sent_count += 1
            else:
                errors.append({"email": contact["email"], "error": message})
                
        except Exception as e:
            errors.append({"email": contact["email"], "error": str(e)})
    
    return {
        "success": True,
        "sent": sent_count,
        "total": len(contacts),
        "errors": errors[:10]  # Limit errors shown
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
