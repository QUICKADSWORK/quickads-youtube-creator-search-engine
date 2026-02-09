"""
YouTube Channel Scraper Module
"""
import os
import requests
from typing import List, Dict, Optional
from dotenv import load_dotenv

import database as db

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"

# Countries to filter - now allowing all countries (filtering done in UI)
ALLOWED_COUNTRIES = None  # Set to None to allow all countries

# Hindi detection signals
HINDI_SIGNALS = [
    "india", "bharat", "hindi", "desi", "nikhil", "damini",
    "rana", "loveneek", "jaimin"
]

# Official/Brand channel indicators to EXCLUDE
BRAND_INDICATORS = [
    # Company suffixes
    "official", "inc", "llc", "ltd", "corp", "corporation", "company",
    "™", "®", "©",
    # Major brands/platforms to exclude
    "coursera", "udemy", "udacity", "linkedin learning", "skillshare",
    "google", "microsoft", "amazon", "meta", "facebook", "hubspot",
    "shopify", "wix", "squarespace", "godaddy", "hostinger",
    "semrush", "ahrefs", "moz", "mailchimp", "salesforce",
    "adobe", "canva", "figma", "notion",
    # Education platforms
    "khan academy", "ted-ed", "ted talks", "great courses",
    "masterclass", "brilliant", "codecademy", "freecodecamp",
    # News/Media
    "news", "times", "post", "journal", "magazine", "media group",
    "network", "studios", "productions", "entertainment",
    # Generic brand patterns
    "headquarters", "hq", "global", "worldwide", "international",
]

# Patterns that suggest individual creators (GOOD)
CREATOR_INDICATORS = [
    "with me", "my journey", "how i", "i made", "i earned",
    "tips from", "secrets", "honest review", "real talk",
    "day in my life", "behind the scenes", "entrepreneur",
]


def is_likely_brand_channel(channel: Dict) -> bool:
    """
    Check if a channel is likely an official brand/company channel.
    Returns True if it should be EXCLUDED.
    """
    title = channel.get("channel_title", "").lower()
    description = channel.get("description", "").lower()
    
    # Check for brand indicators in title
    for indicator in BRAND_INDICATORS:
        if indicator in title:
            return True
    
    # Channels with very high subscribers (>5M) are usually brands
    subs = channel.get("subscribers", 0)
    if subs > 5000000:
        return True
    
    # Check description for corporate language
    corporate_phrases = [
        "we are a", "our company", "our team", "our mission",
        "founded in", "established in", "leading provider",
        "official channel", "official youtube", "subscribe to our"
    ]
    for phrase in corporate_phrases:
        if phrase in description:
            return True
    
    return False


def is_likely_creator(channel: Dict) -> bool:
    """
    Check if a channel is likely an individual creator.
    Returns True if it's a GOOD match for influencer marketing.
    """
    title = channel.get("channel_title", "").lower()
    description = channel.get("description", "").lower()
    subs = channel.get("subscribers", 0)
    
    # Sweet spot for influencers: 5K - 2M subscribers
    if subs < 1000 or subs > 2000000:
        return False
    
    # Check for creator indicators
    for indicator in CREATOR_INDICATORS:
        if indicator in description:
            return True
    
    # Personal pronouns in description suggest individual creator
    personal_phrases = ["i am", "i'm", "my name", "hey guys", "hey everyone", 
                        "welcome to my", "i help", "i teach", "i show"]
    for phrase in personal_phrases:
        if phrase in description:
            return True
    
    # If no strong signals but reasonable sub count, include it
    if 5000 <= subs <= 500000:
        return True
    
    return True  # Default include if passed brand check


def search_youtube_channels(query: str, max_results: int = 25, region_code: str = "US") -> List[str]:
    """
    Search YouTube for channels matching the query.
    Returns a list of channel IDs.
    """
    if not YOUTUBE_API_KEY:
        raise ValueError("YouTube API key not configured")
    
    params = {
        "part": "snippet",
        "q": query,
        "type": "channel",
        "maxResults": min(max_results, 50),  # YouTube API max is 50
        "key": YOUTUBE_API_KEY,
        "regionCode": region_code
    }
    
    try:
        response = requests.get(YOUTUBE_SEARCH_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        channel_ids = []
        for item in data.get("items", []):
            channel_id = item.get("snippet", {}).get("channelId")
            if channel_id:
                channel_ids.append(channel_id)
        
        return channel_ids
    
    except requests.RequestException as e:
        print(f"Error searching YouTube: {e}")
        return []


def get_channel_details(channel_ids: List[str]) -> List[Dict]:
    """
    Get detailed information for a list of channel IDs.
    """
    if not channel_ids or not YOUTUBE_API_KEY:
        return []
    
    # YouTube API allows up to 50 IDs per request
    all_channels = []
    
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        
        params = {
            "part": "snippet,statistics",
            "id": ",".join(batch),
            "key": YOUTUBE_API_KEY
        }
        
        try:
            response = requests.get(YOUTUBE_CHANNELS_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("items", []):
                channel = parse_channel_data(item)
                if channel:
                    all_channels.append(channel)
        
        except requests.RequestException as e:
            print(f"Error getting channel details: {e}")
    
    return all_channels


def parse_channel_data(item: Dict) -> Optional[Dict]:
    """
    Parse YouTube API response into channel data.
    """
    channel_id = item.get("id")
    snippet = item.get("snippet", {})
    statistics = item.get("statistics", {})
    
    if not channel_id:
        return None
    
    title = snippet.get("title", "").lower()
    country = (snippet.get("country", "") or "").upper()
    
    # Detect language (basic Hindi vs English detection)
    detected_language = "english"
    if country == "IN":
        detected_language = "hindi"
    else:
        for signal in HINDI_SIGNALS:
            if signal in title:
                detected_language = "hindi"
                break
    
    # Get thumbnail
    thumbnails = snippet.get("thumbnails", {})
    thumbnail_url = thumbnails.get("medium", {}).get("url", "")
    if not thumbnail_url:
        thumbnail_url = thumbnails.get("default", {}).get("url", "")
    
    return {
        "channel_id": channel_id,
        "channel_url": f"https://www.youtube.com/channel/{channel_id}",
        "channel_title": snippet.get("title", ""),
        "description": snippet.get("description", "")[:500],  # Limit description
        "country": country,
        "detected_language": detected_language,
        "subscribers": int(statistics.get("subscriberCount", 0)),
        "total_views": int(statistics.get("viewCount", 0)),
        "video_count": int(statistics.get("videoCount", 0)),
        "thumbnail_url": thumbnail_url,
        "email": ""
    }


def filter_channels(channels: List[Dict], existing_ids: set) -> List[Dict]:
    """
    Filter channels:
    - Remove duplicates
    - Remove already existing channels
    """
    filtered = []
    seen_ids = set()
    
    for channel in channels:
        channel_id = channel.get("channel_id")
        
        # Skip if already in database or already processed
        if channel_id in existing_ids or channel_id in seen_ids:
            continue
        
        seen_ids.add(channel_id)
        filtered.append(channel)
    
    return filtered


def filter_channels_with_criteria(channels: List[Dict], existing_ids: set, 
                                   languages: List[str], min_subscribers: int) -> List[Dict]:
    """
    Filter channels with additional criteria:
    - Remove duplicates
    - Remove already existing channels
    - Filter by language
    - Filter by minimum subscribers
    - EXCLUDE official brand/company channels
    - PREFER individual creators
    """
    filtered = []
    seen_ids = set()
    excluded_brands = 0
    
    # If no languages specified, accept all
    accept_all_languages = not languages or len(languages) == 0
    
    for channel in channels:
        channel_id = channel.get("channel_id")
        
        # Skip if already in database or already processed
        if channel_id in existing_ids or channel_id in seen_ids:
            continue
        
        # IMPORTANT: Skip official brand/company channels
        if is_likely_brand_channel(channel):
            excluded_brands += 1
            print(f"  Excluded brand: {channel.get('channel_title')}")
            continue
        
        # Filter by minimum subscribers
        subs = channel.get("subscribers", 0)
        if min_subscribers > 0 and subs < min_subscribers:
            continue
        
        # Filter by language
        channel_lang = channel.get("detected_language", "english").lower()
        if not accept_all_languages and channel_lang not in [l.lower() for l in languages]:
            continue
        
        # Prefer individual creators
        if is_likely_creator(channel):
            seen_ids.add(channel_id)
            filtered.append(channel)
    
    print(f"  Excluded {excluded_brands} brand/official channels")
    return filtered


def run_scraper(clear_previous: bool = False, countries: list = None, 
                languages: list = None, min_subscribers: int = 0) -> Dict:
    """
    Run the full scraping process with filters.
    Returns a summary of the scrape.
    
    Args:
        clear_previous: If True, clears all previous channels before scraping
        countries: List of country codes to search in (e.g., ["US", "IN", "PE"])
        languages: List of languages to filter (e.g., ["english", "hindi"])
        min_subscribers: Minimum subscriber count to include
    """
    # Default values
    if countries is None:
        countries = ["US"]
    if languages is None:
        languages = ["english"]
    
    # Start history entry
    history_id = db.start_scrape_history()
    
    try:
        # Clear previous channels if requested
        if clear_previous:
            cleared = db.clear_all_channels()
            print(f"Cleared {cleared} previous channels")
        
        # Get active search queries
        queries = db.get_search_queries(active_only=True)
        
        if not queries:
            db.complete_scrape_history(history_id, 0, 0, "completed", "No active queries")
            return {"success": True, "found": 0, "added": 0, "message": "No active queries"}
        
        # Get existing channel IDs for deduplication
        existing_ids = db.get_existing_channel_ids()
        
        # Search for channels across all queries AND all selected countries
        all_channel_ids = []
        for query_row in queries:
            for country in countries:
                print(f"Searching: '{query_row['query']}' in {country}")
                channel_ids = search_youtube_channels(
                    query_row["query"],
                    query_row["max_results"],
                    country  # Use the selected country instead of query's region
                )
                all_channel_ids.extend(channel_ids)
        
        # Remove duplicate IDs
        unique_channel_ids = list(set(all_channel_ids))
        print(f"Found {len(unique_channel_ids)} unique channel IDs")
        
        # Get channel details
        channels = get_channel_details(unique_channel_ids)
        
        # Filter channels by language and minimum subscribers
        filtered_channels = filter_channels_with_criteria(
            channels, 
            existing_ids, 
            languages, 
            min_subscribers
        )
        
        print(f"After filtering: {len(filtered_channels)} channels match criteria")
        
        # Add to database
        added_count = 0
        for channel in filtered_channels:
            if db.add_channel(channel):
                added_count += 1
        
        db.complete_scrape_history(
            history_id, 
            len(channels), 
            added_count, 
            "completed"
        )
        
        return {
            "success": True,
            "found": len(channels),
            "added": added_count,
            "message": f"Found {len(channels)} channels, added {added_count} new ones"
        }
    
    except Exception as e:
        db.complete_scrape_history(history_id, 0, 0, "failed", str(e))
        return {
            "success": False,
            "found": 0,
            "added": 0,
            "message": f"Error: {str(e)}"
        }
