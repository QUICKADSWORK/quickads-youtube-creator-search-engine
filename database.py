"""
SQLite Database Module for YouTube Channel Scraper
"""
import sqlite3
import json
import os
from datetime import datetime
from typing import List, Dict, Optional
from contextlib import contextmanager

# Use /data directory for Render persistent disk, fallback to local
DATA_DIR = "/data" if os.path.exists("/data") else "."

# Ensure directory exists
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except:
    DATA_DIR = "."

DATABASE_PATH = os.path.join(DATA_DIR, "youtube_channels.db")
print(f"Database path: {DATABASE_PATH}")


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initialize the database with required tables."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Channels table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT UNIQUE NOT NULL,
                channel_url TEXT,
                channel_title TEXT,
                description TEXT,
                country TEXT,
                detected_language TEXT,
                subscribers INTEGER DEFAULT 0,
                total_views INTEGER DEFAULT 0,
                video_count INTEGER DEFAULT 0,
                email TEXT,
                thumbnail_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Search queries table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                max_results INTEGER DEFAULT 25,
                region_code TEXT DEFAULT 'US',
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Scrape history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scrape_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                channels_found INTEGER DEFAULT 0,
                channels_added INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                error_message TEXT
            )
        """)
        
        conn.commit()
    
    # Initialize email tables
    init_email_tables()
    
    with get_db() as conn:
        cursor = conn.cursor()
        # Insert default search queries if empty
        cursor.execute("SELECT COUNT(*) FROM search_queries")
        if cursor.fetchone()[0] == 0:
            # Queries targeting INDIVIDUAL CREATORS, not brands
            default_queries = [
                # Personal success stories
                ("how I made money with facebook ads", 50, "US"),
                ("my dropshipping journey", 25, "US"),
                ("how I grew my ecommerce store", 25, "US"),
                ("my marketing agency story", 25, "US"),
                # Tutorial creators (individuals)
                ("facebook ads tutorial for beginners 2024", 25, "US"),
                ("meta ads case study results", 25, "US"),
                ("shopify store review", 25, "US"),
                ("ecommerce tips entrepreneur", 25, "US"),
                # Influencer/Creator focused
                ("day in my life digital marketer", 25, "US"),
                ("marketing tips small youtuber", 25, "US"),
                ("side hustle online business", 25, "US"),
                ("affiliate marketing income report", 25, "US"),
            ]
            cursor.executemany(
                "INSERT INTO search_queries (query, max_results, region_code) VALUES (?, ?, ?)",
                default_queries
            )
        
        conn.commit()


def get_all_channels(limit: int = 100, offset: int = 0, search: str = "", 
                     country: str = "", language: str = "", 
                     min_subs: int = 0, max_subs: int = 0) -> List[Dict]:
    """Get all channels with pagination, search, and filters."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        conditions = []
        params = []
        
        if search:
            conditions.append("(channel_title LIKE ? OR description LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        if country:
            conditions.append("country = ?")
            params.append(country)
        
        if language:
            conditions.append("detected_language = ?")
            params.append(language)
        
        if min_subs > 0:
            conditions.append("subscribers >= ?")
            params.append(min_subs)
        
        if max_subs > 0:
            conditions.append("subscribers <= ?")
            params.append(max_subs)
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
            SELECT * FROM channels 
            {where_clause}
            ORDER BY subscribers DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_channel_count(search: str = "", country: str = "", language: str = "",
                      min_subs: int = 0, max_subs: int = 0) -> int:
    """Get total channel count with filters."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        conditions = []
        params = []
        
        if search:
            conditions.append("(channel_title LIKE ? OR description LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        if country:
            conditions.append("country = ?")
            params.append(country)
        
        if language:
            conditions.append("detected_language = ?")
            params.append(language)
        
        if min_subs > 0:
            conditions.append("subscribers >= ?")
            params.append(min_subs)
        
        if max_subs > 0:
            conditions.append("subscribers <= ?")
            params.append(max_subs)
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        cursor.execute(f"SELECT COUNT(*) FROM channels {where_clause}", params)
        return cursor.fetchone()[0]


def clear_all_channels():
    """Clear all channels from the database."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM channels")
        conn.commit()
        return cursor.rowcount


def get_unique_countries() -> List[str]:
    """Get list of unique countries in database."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT country FROM channels WHERE country != '' ORDER BY country")
        return [row[0] for row in cursor.fetchall()]


def get_unique_languages() -> List[str]:
    """Get list of unique languages in database."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT detected_language FROM channels WHERE detected_language != '' ORDER BY detected_language")
        return [row[0] for row in cursor.fetchall()]


def get_existing_channel_ids() -> set:
    """Get all existing channel IDs."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT channel_id FROM channels")
        return {row[0] for row in cursor.fetchall()}


def add_channel(channel_data: Dict) -> bool:
    """Add a new channel to the database."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO channels (
                    channel_id, channel_url, channel_title, description,
                    country, detected_language, subscribers, total_views,
                    video_count, email, thumbnail_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                channel_data.get("channel_id"),
                channel_data.get("channel_url"),
                channel_data.get("channel_title"),
                channel_data.get("description"),
                channel_data.get("country"),
                channel_data.get("detected_language"),
                channel_data.get("subscribers", 0),
                channel_data.get("total_views", 0),
                channel_data.get("video_count", 0),
                channel_data.get("email", ""),
                channel_data.get("thumbnail_url", ""),
            ))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def delete_channel(channel_id: str) -> bool:
    """Delete a channel by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM channels WHERE channel_id = ?", (channel_id,))
        conn.commit()
        return cursor.rowcount > 0


def get_search_queries(active_only: bool = False) -> List[Dict]:
    """Get all search queries."""
    with get_db() as conn:
        cursor = conn.cursor()
        if active_only:
            cursor.execute("SELECT * FROM search_queries WHERE is_active = 1")
        else:
            cursor.execute("SELECT * FROM search_queries")
        return [dict(row) for row in cursor.fetchall()]


def add_search_query(query: str, max_results: int = 25, region_code: str = "US") -> int:
    """Add a new search query."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO search_queries (query, max_results, region_code) VALUES (?, ?, ?)",
            (query, max_results, region_code)
        )
        conn.commit()
        return cursor.lastrowid


def update_search_query(query_id: int, query: str = None, max_results: int = None, 
                        region_code: str = None, is_active: bool = None) -> bool:
    """Update a search query."""
    with get_db() as conn:
        cursor = conn.cursor()
        updates = []
        values = []
        
        if query is not None:
            updates.append("query = ?")
            values.append(query)
        if max_results is not None:
            updates.append("max_results = ?")
            values.append(max_results)
        if region_code is not None:
            updates.append("region_code = ?")
            values.append(region_code)
        if is_active is not None:
            updates.append("is_active = ?")
            values.append(is_active)
        
        if not updates:
            return False
        
        values.append(query_id)
        cursor.execute(
            f"UPDATE search_queries SET {', '.join(updates)} WHERE id = ?",
            values
        )
        conn.commit()
        return cursor.rowcount > 0


def delete_search_query(query_id: int) -> bool:
    """Delete a search query."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM search_queries WHERE id = ?", (query_id,))
        conn.commit()
        return cursor.rowcount > 0


def reset_search_queries_to_creator_focused():
    """Reset search queries to creator-focused defaults (for influencer marketing)."""
    with get_db() as conn:
        cursor = conn.cursor()
        # Clear existing queries
        cursor.execute("DELETE FROM search_queries")
        
        # Add creator-focused queries - targeting INDIVIDUALS not brands
        creator_queries = [
            # ===== PERSONAL JOURNEY STORIES =====
            ("how I made my first $10k online", 30, "US"),
            ("my journey to 100k subscribers", 25, "US"),
            ("I quit my 9 to 5 job", 25, "US"),
            ("my first year as entrepreneur", 25, "US"),
            ("how I started my online business from scratch", 25, "US"),
            
            # ===== INCOME/RESULTS REPORTS =====
            ("income report youtuber", 25, "US"),
            ("how much I made this month", 25, "US"),
            ("revealing my earnings", 25, "US"),
            ("my affiliate marketing income", 25, "US"),
            ("dropshipping income proof", 25, "US"),
            
            # ===== PERSONAL ADS/MARKETING CONTENT =====
            ("I spent $1000 on facebook ads", 25, "US"),
            ("my facebook ads results", 25, "US"),
            ("testing tiktok ads for my store", 25, "US"),
            ("my meta ads strategy", 25, "US"),
            ("I tried google ads for 30 days", 25, "US"),
            
            # ===== ECOMMERCE CREATOR STORIES =====
            ("my shopify store journey", 25, "US"),
            ("I started dropshipping with $0", 25, "US"),
            ("my etsy shop income", 25, "US"),
            ("amazon fba beginner journey", 25, "US"),
            ("print on demand real results", 25, "US"),
            
            # ===== SIDE HUSTLE/ENTREPRENEUR =====
            ("best side hustles I actually tried", 25, "US"),
            ("how I make passive income", 25, "US"),
            ("day in my life online entrepreneur", 25, "US"),
            ("work from home business ideas that work", 25, "US"),
            ("my online business income streams", 25, "US"),
            
            # ===== YOUTUBE/CONTENT CREATOR GROWTH =====
            ("how I grew my youtube channel", 25, "US"),
            ("my content creation journey", 25, "US"),
            ("youtube monetization tips from experience", 25, "US"),
            ("growing on social media as beginner", 25, "US"),
            
            # ===== NICHE SPECIFIC CREATORS =====
            ("real estate agent marketing tips", 25, "US"),
            ("fitness influencer income", 25, "US"),
            ("travel youtuber behind the scenes", 25, "US"),
            ("food blogger income report", 25, "US"),
        ]
        
        cursor.executemany(
            "INSERT INTO search_queries (query, max_results, region_code) VALUES (?, ?, ?)",
            creator_queries
        )
        conn.commit()
        return len(creator_queries)


def start_scrape_history() -> int:
    """Start a new scrape history entry."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO scrape_history (started_at, status) VALUES (?, 'running')",
            (datetime.now(),)
        )
        conn.commit()
        return cursor.lastrowid


def complete_scrape_history(history_id: int, channels_found: int, channels_added: int, 
                            status: str = "completed", error_message: str = None):
    """Complete a scrape history entry."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE scrape_history 
            SET completed_at = ?, channels_found = ?, channels_added = ?, 
                status = ?, error_message = ?
            WHERE id = ?
        """, (datetime.now(), channels_found, channels_added, status, error_message, history_id))
        conn.commit()


def get_scrape_history(limit: int = 20) -> List[Dict]:
    """Get recent scrape history."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM scrape_history 
            ORDER BY started_at DESC 
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in cursor.fetchall()]


def get_stats() -> Dict:
    """Get dashboard statistics."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Total channels
        cursor.execute("SELECT COUNT(*) FROM channels")
        total_channels = cursor.fetchone()[0]
        
        # Channels by country
        cursor.execute("""
            SELECT country, COUNT(*) as count 
            FROM channels 
            GROUP BY country 
            ORDER BY count DESC 
            LIMIT 10
        """)
        by_country = [{"country": row[0] or "Unknown", "count": row[1]} for row in cursor.fetchall()]
        
        # Channels by language
        cursor.execute("""
            SELECT detected_language, COUNT(*) as count 
            FROM channels 
            GROUP BY detected_language
        """)
        by_language = [{"language": row[0] or "Unknown", "count": row[1]} for row in cursor.fetchall()]
        
        # Total subscribers
        cursor.execute("SELECT SUM(subscribers) FROM channels")
        total_subscribers = cursor.fetchone()[0] or 0
        
        # Active queries
        cursor.execute("SELECT COUNT(*) FROM search_queries WHERE is_active = 1")
        active_queries = cursor.fetchone()[0]
        
        # Last scrape
        cursor.execute("""
            SELECT * FROM scrape_history 
            ORDER BY started_at DESC 
            LIMIT 1
        """)
        last_scrape_row = cursor.fetchone()
        last_scrape = dict(last_scrape_row) if last_scrape_row else None
        
        return {
            "total_channels": total_channels,
            "total_subscribers": total_subscribers,
            "active_queries": active_queries,
            "by_country": by_country,
            "by_language": by_language,
            "last_scrape": last_scrape
        }


# ============================================================
# EMAIL ACCOUNTS MANAGEMENT
# ============================================================

def init_email_tables():
    """Initialize email-related tables."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Email accounts table (SMTP credentials)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                smtp_host TEXT NOT NULL,
                smtp_port INTEGER DEFAULT 587,
                smtp_user TEXT NOT NULL,
                smtp_password TEXT NOT NULL,
                display_name TEXT,
                is_active BOOLEAN DEFAULT 1,
                last_used TIMESTAMP,
                emails_sent_today INTEGER DEFAULT 0,
                daily_limit INTEGER DEFAULT 50,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Campaigns table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                brief TEXT,
                budget_min REAL,
                budget_max REAL,
                max_offer REAL DEFAULT 500,
                offer_increment REAL DEFAULT 50,
                topic TEXT,
                requirements TEXT,
                deadline TEXT,
                status TEXT DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Outreach emails table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS outreach_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER,
                channel_id TEXT,
                email_account_id INTEGER,
                recipient_email TEXT,
                subject TEXT,
                body TEXT,
                status TEXT DEFAULT 'draft',
                sent_at TIMESTAMP,
                opened_at TIMESTAMP,
                replied_at TIMESTAMP,
                reply_content TEXT,
                ai_response TEXT,
                negotiation_stage TEXT DEFAULT 'initial',
                current_offer REAL DEFAULT 0,
                negotiation_rounds INTEGER DEFAULT 0,
                followup_count INTEGER DEFAULT 0,
                last_followup_at TIMESTAMP,
                last_inbound_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
                FOREIGN KEY (email_account_id) REFERENCES email_accounts(id)
            )
        """)
        
        # Email threads table (for tracking conversations)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                outreach_id INTEGER,
                direction TEXT,
                subject TEXT,
                body TEXT,
                message_hash TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (outreach_id) REFERENCES outreach_emails(id)
            )
        """)
        
        # Processed email IDs to prevent duplicates
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE,
                from_email TEXT,
                subject TEXT,
                body_hash TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Mailing list table (for bulk outreach)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mailing_list (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                channel_id TEXT,
                channel_title TEXT,
                subscribers INTEGER,
                notes TEXT,
                status TEXT DEFAULT 'pending',
                campaign_id INTEGER,
                outreach_id INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
                FOREIGN KEY (outreach_id) REFERENCES outreach_emails(id)
            )
        """)
        
        conn.commit()
        
        # Run migrations to add new columns to existing tables
        migrate_database(conn)


def migrate_database(conn):
    """Add missing columns to existing tables (for upgrades)."""
    cursor = conn.cursor()
    
    def column_exists(table, column):
        """Check if a column exists in a table using SQLite pragma."""
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        return column in columns
    
    def add_column_if_missing(table, column, col_type, default=None):
        """Add column if it doesn't exist."""
        if not column_exists(table, column):
            default_clause = f" DEFAULT {default}" if default is not None else ""
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}{default_clause}")
                print(f"Added {column} column to {table}")
            except Exception as e:
                print(f"Error adding {column} to {table}: {e}")
    
    # Add missing columns to campaigns table
    add_column_if_missing('campaigns', 'max_offer', 'REAL', 500)
    add_column_if_missing('campaigns', 'offer_increment', 'REAL', 50)
    
    # Add missing columns to outreach_emails table
    add_column_if_missing('outreach_emails', 'current_offer', 'REAL', 0)
    add_column_if_missing('outreach_emails', 'negotiation_rounds', 'INTEGER', 0)
    add_column_if_missing('outreach_emails', 'followup_count', 'INTEGER', 0)
    add_column_if_missing('outreach_emails', 'last_followup_at', 'TIMESTAMP', None)
    add_column_if_missing('outreach_emails', 'last_inbound_at', 'TIMESTAMP', None)
    
    # Create processed_emails table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            from_email TEXT,
            subject TEXT,
            body_hash TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    print("Database migration complete.")


def add_email_account(email: str, smtp_host: str, smtp_port: int, 
                      smtp_user: str, smtp_password: str, 
                      display_name: str = "", daily_limit: int = 50) -> int:
    """Add a new email account."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO email_accounts 
                (email, smtp_host, smtp_port, smtp_user, smtp_password, display_name, daily_limit)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (email, smtp_host, smtp_port, smtp_user, smtp_password, display_name, daily_limit))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return -1


def get_email_accounts(active_only: bool = False) -> List[Dict]:
    """Get all email accounts."""
    with get_db() as conn:
        cursor = conn.cursor()
        if active_only:
            cursor.execute("SELECT * FROM email_accounts WHERE is_active = 1")
        else:
            cursor.execute("SELECT * FROM email_accounts")
        return [dict(row) for row in cursor.fetchall()]


def get_email_account(account_id: int) -> Optional[Dict]:
    """Get a single email account by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM email_accounts WHERE id = ?", (account_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_email_account(account_id: int, **kwargs) -> bool:
    """Update an email account."""
    if not kwargs:
        return False
    with get_db() as conn:
        cursor = conn.cursor()
        updates = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [account_id]
        cursor.execute(f"UPDATE email_accounts SET {updates} WHERE id = ?", values)
        conn.commit()
        return cursor.rowcount > 0


def delete_email_account(account_id: int) -> bool:
    """Delete an email account."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM email_accounts WHERE id = ?", (account_id,))
        conn.commit()
        return cursor.rowcount > 0


def increment_email_sent(account_id: int):
    """Increment the emails sent counter for an account."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE email_accounts 
            SET emails_sent_today = emails_sent_today + 1, last_used = ?
            WHERE id = ?
        """, (datetime.now(), account_id))
        conn.commit()


def reset_daily_email_counts():
    """Reset daily email counts for all accounts (call daily)."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE email_accounts SET emails_sent_today = 0")
        conn.commit()


# ============================================================
# CAMPAIGNS MANAGEMENT
# ============================================================

def create_campaign(name: str, brief: str = "", budget_min: float = 0, 
                   budget_max: float = 0, max_offer: float = 500,
                   offer_increment: float = 50, topic: str = "", 
                   requirements: str = "", deadline: str = "") -> int:
    """Create a new campaign."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO campaigns 
            (name, brief, budget_min, budget_max, max_offer, offer_increment, topic, requirements, deadline, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft')
        """, (name, brief, budget_min, budget_max, max_offer, offer_increment, topic, requirements, deadline))
        conn.commit()
        return cursor.lastrowid


def get_campaigns(status: str = None) -> List[Dict]:
    """Get all campaigns."""
    with get_db() as conn:
        cursor = conn.cursor()
        if status:
            cursor.execute("SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC", (status,))
        else:
            cursor.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
        return [dict(row) for row in cursor.fetchall()]


def get_campaign(campaign_id: int) -> Optional[Dict]:
    """Get a single campaign by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_campaign(campaign_id: int, **kwargs) -> bool:
    """Update a campaign."""
    if not kwargs:
        return False
    kwargs['updated_at'] = datetime.now()
    with get_db() as conn:
        cursor = conn.cursor()
        updates = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [campaign_id]
        cursor.execute(f"UPDATE campaigns SET {updates} WHERE id = ?", values)
        conn.commit()
        return cursor.rowcount > 0


def delete_campaign(campaign_id: int) -> bool:
    """Delete a campaign."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
        conn.commit()
        return cursor.rowcount > 0


# ============================================================
# OUTREACH EMAILS MANAGEMENT
# ============================================================

def create_outreach(campaign_id: int, channel_id: str, recipient_email: str,
                   email_account_id: int, subject: str, body: str) -> int:
    """Create a new outreach email."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO outreach_emails 
            (campaign_id, channel_id, email_account_id, recipient_email, subject, body, status)
            VALUES (?, ?, ?, ?, ?, ?, 'draft')
        """, (campaign_id, channel_id, email_account_id, recipient_email, subject, body))
        conn.commit()
        return cursor.lastrowid


def get_outreach_emails(campaign_id: int = None, status: str = None) -> List[Dict]:
    """Get outreach emails with optional filters."""
    with get_db() as conn:
        cursor = conn.cursor()
        conditions = []
        params = []
        
        if campaign_id:
            conditions.append("o.campaign_id = ?")
            params.append(campaign_id)
        if status:
            conditions.append("o.status = ?")
            params.append(status)
        
        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        
        cursor.execute(f"""
            SELECT o.*, c.channel_title, c.subscribers, c.thumbnail_url
            FROM outreach_emails o
            LEFT JOIN channels c ON o.channel_id = c.channel_id
            {where_clause}
            ORDER BY o.created_at DESC
        """, params)
        return [dict(row) for row in cursor.fetchall()]


def get_outreach(outreach_id: int) -> Optional[Dict]:
    """Get a single outreach email by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT o.*, c.channel_title, c.subscribers, c.thumbnail_url
            FROM outreach_emails o
            LEFT JOIN channels c ON o.channel_id = c.channel_id
            WHERE o.id = ?
        """, (outreach_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_outreach(outreach_id: int, **kwargs) -> bool:
    """Update an outreach email."""
    if not kwargs:
        return False
    with get_db() as conn:
        cursor = conn.cursor()
        updates = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [outreach_id]
        cursor.execute(f"UPDATE outreach_emails SET {updates} WHERE id = ?", values)
        conn.commit()
        return cursor.rowcount > 0


def mark_outreach_sent(outreach_id: int, email_account_id: int = None):
    """Mark an outreach email as sent."""
    with get_db() as conn:
        cursor = conn.cursor()
        if email_account_id:
            cursor.execute("""
                UPDATE outreach_emails 
                SET status = 'sent', sent_at = ?, email_account_id = ?
                WHERE id = ?
            """, (datetime.now(), email_account_id, outreach_id))
        else:
            cursor.execute("""
                UPDATE outreach_emails 
                SET status = 'sent', sent_at = ?
                WHERE id = ?
            """, (datetime.now(), outreach_id))
        conn.commit()


def add_email_thread(outreach_id: int, direction: str, subject: str, body: str) -> int:
    """Add a message to an email thread."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO email_threads (outreach_id, direction, subject, body)
            VALUES (?, ?, ?, ?)
        """, (outreach_id, direction, subject, body))
        conn.commit()
        return cursor.lastrowid


def get_email_thread(outreach_id: int) -> List[Dict]:
    """Get all messages in an email thread."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM email_threads 
            WHERE outreach_id = ? 
            ORDER BY sent_at ASC
        """, (outreach_id,))
        return [dict(row) for row in cursor.fetchall()]


def is_email_processed(message_id: str = None, body_hash: str = None) -> bool:
    """Check if an email has already been processed."""
    with get_db() as conn:
        cursor = conn.cursor()
        if message_id:
            cursor.execute("SELECT id FROM processed_emails WHERE message_id = ?", (message_id,))
            if cursor.fetchone():
                return True
        if body_hash:
            cursor.execute("SELECT id FROM processed_emails WHERE body_hash = ?", (body_hash,))
            if cursor.fetchone():
                return True
        return False


def mark_email_processed(message_id: str, from_email: str, subject: str, body_hash: str):
    """Mark an email as processed to prevent duplicate handling."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO processed_emails (message_id, from_email, subject, body_hash)
                VALUES (?, ?, ?, ?)
            """, (message_id, from_email, subject, body_hash))
            conn.commit()
        except:
            pass  # Ignore duplicates


def get_thread_stats(outreach_id: int) -> Dict:
    """Get stats about an email thread."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Count inbound messages
        cursor.execute("""
            SELECT COUNT(*) FROM email_threads 
            WHERE outreach_id = ? AND direction = 'inbound'
        """, (outreach_id,))
        inbound_count = cursor.fetchone()[0]
        
        # Count outbound messages
        cursor.execute("""
            SELECT COUNT(*) FROM email_threads 
            WHERE outreach_id = ? AND direction = 'outbound'
        """, (outreach_id,))
        outbound_count = cursor.fetchone()[0]
        
        # Get last inbound time
        cursor.execute("""
            SELECT MAX(sent_at) FROM email_threads 
            WHERE outreach_id = ? AND direction = 'inbound'
        """, (outreach_id,))
        last_inbound = cursor.fetchone()[0]
        
        # Get last outbound time
        cursor.execute("""
            SELECT MAX(sent_at) FROM email_threads 
            WHERE outreach_id = ? AND direction = 'outbound'
        """, (outreach_id,))
        last_outbound = cursor.fetchone()[0]
        
        return {
            "inbound_count": inbound_count,
            "outbound_count": outbound_count,
            "last_inbound": last_inbound,
            "last_outbound": last_outbound
        }


def update_channel_email(channel_id: str, email: str) -> bool:
    """Update the email for a channel."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE channels SET email = ? WHERE channel_id = ?", (email, channel_id))
        conn.commit()
        return cursor.rowcount > 0


def get_outreach_stats() -> Dict:
    """Get outreach statistics."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM outreach_emails")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM outreach_emails WHERE status = 'draft'")
        drafts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM outreach_emails WHERE status = 'sent'")
        sent = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM outreach_emails WHERE status = 'replied'")
        replied = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM outreach_emails WHERE negotiation_stage = 'deal_closed'")
        deals = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM campaigns")
        campaigns = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM email_accounts WHERE is_active = 1")
        active_accounts = cursor.fetchone()[0]
        
        return {
            "total_outreach": total,
            "drafts": drafts,
            "sent": sent,
            "replied": replied,
            "deals_closed": deals,
            "campaigns": campaigns,
            "active_email_accounts": active_accounts
        }


# ============ Mailing List Functions ============

def add_to_mailing_list(name: str, email: str, channel_id: str = None, 
                        channel_title: str = None, subscribers: int = None,
                        notes: str = None, campaign_id: int = None) -> int:
    """Add a contact to the mailing list."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO mailing_list 
            (name, email, channel_id, channel_title, subscribers, notes, campaign_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (name, email, channel_id, channel_title, subscribers, notes, campaign_id))
        conn.commit()
        return cursor.lastrowid


def add_bulk_to_mailing_list(contacts: List[Dict], campaign_id: int = None) -> int:
    """Add multiple contacts to mailing list."""
    added = 0
    with get_db() as conn:
        cursor = conn.cursor()
        for contact in contacts:
            try:
                cursor.execute("""
                    INSERT INTO mailing_list 
                    (name, email, channel_id, channel_title, subscribers, notes, campaign_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    contact.get("name", ""),
                    contact.get("email"),
                    contact.get("channel_id"),
                    contact.get("channel_title"),
                    contact.get("subscribers"),
                    contact.get("notes"),
                    campaign_id
                ))
                added += 1
            except Exception:
                continue
        conn.commit()
    return added


def get_mailing_list(campaign_id: int = None, status: str = None) -> List[Dict]:
    """Get mailing list contacts."""
    with get_db() as conn:
        cursor = conn.cursor()
        query = "SELECT * FROM mailing_list WHERE 1=1"
        params = []
        
        if campaign_id:
            query += " AND campaign_id = ?"
            params.append(campaign_id)
        if status:
            query += " AND status = ?"
            params.append(status)
            
        query += " ORDER BY added_at DESC"
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_mailing_list_contact(contact_id: int) -> Optional[Dict]:
    """Get a single mailing list contact."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mailing_list WHERE id = ?", (contact_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_mailing_list_contact(contact_id: int, **kwargs) -> bool:
    """Update a mailing list contact."""
    with get_db() as conn:
        cursor = conn.cursor()
        updates = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [contact_id]
        cursor.execute(f"UPDATE mailing_list SET {updates} WHERE id = ?", values)
        conn.commit()
        return cursor.rowcount > 0


def delete_mailing_list_contact(contact_id: int) -> bool:
    """Delete a contact from mailing list."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM mailing_list WHERE id = ?", (contact_id,))
        conn.commit()
        return cursor.rowcount > 0


def clear_mailing_list(campaign_id: int = None) -> int:
    """Clear mailing list (optionally for a specific campaign)."""
    with get_db() as conn:
        cursor = conn.cursor()
        if campaign_id:
            cursor.execute("DELETE FROM mailing_list WHERE campaign_id = ?", (campaign_id,))
        else:
            cursor.execute("DELETE FROM mailing_list")
        conn.commit()
        return cursor.rowcount


def get_mailing_list_stats() -> Dict:
    """Get mailing list statistics."""
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM mailing_list")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM mailing_list WHERE status = 'pending'")
        pending = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM mailing_list WHERE status = 'sent'")
        sent = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM mailing_list WHERE status = 'replied'")
        replied = cursor.fetchone()[0]
        
        return {
            "total": total,
            "pending": pending,
            "sent": sent,
            "replied": replied
        }
