"""
SQLite Database Module for YouTube Channel Scraper
"""
import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional
from contextlib import contextmanager

DATABASE_PATH = "youtube_channels.db"


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
        
        # Add creator-focused queries
        creator_queries = [
            # Personal success stories (individuals sharing their journey)
            ("how I made money with facebook ads", 50, "US"),
            ("my dropshipping journey 2024", 30, "US"),
            ("how I grew my ecommerce store", 30, "US"),
            ("my marketing agency story", 25, "US"),
            ("i quit my job to start business", 25, "US"),
            
            # Tutorial creators (individuals teaching)
            ("facebook ads tutorial for beginners", 30, "US"),
            ("meta ads case study real results", 25, "US"),
            ("shopify store review honest", 25, "US"),
            ("tiktok ads strategy 2024", 25, "US"),
            
            # Entrepreneur/Side hustle creators
            ("day in my life entrepreneur", 25, "US"),
            ("side hustle ideas that work", 25, "US"),
            ("passive income online business", 25, "US"),
            ("affiliate marketing income report", 25, "US"),
            ("make money online real results", 25, "US"),
            
            # Niche marketing creators
            ("amazon fba journey", 25, "US"),
            ("print on demand tutorial", 25, "US"),
            ("email marketing tips small business", 25, "US"),
            ("instagram growth strategy creator", 25, "US"),
            ("youtube automation channel", 25, "US"),
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
