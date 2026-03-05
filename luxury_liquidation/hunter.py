import os
import json
import asyncio
import requests
import warnings
import yaml
import feedparser
import re
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

# Suppress harmless pydantic warnings thrown by google-genai
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")

# Use the new Google GenAI SDK as updated in requirements.txt and app.py
from google import genai

# Load environment variables
load_dotenv()

# Initialize Gemini Client
client = genai.Client()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SEEN_POSTS_FILE = os.path.join(BASE_DIR, "seen_posts.json")
CONFIG_FILE = os.path.join(BASE_DIR, "hunter_config.yaml")

def load_config():
    """Load Omni-Source YAML Configuration."""
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_seen_posts():
    """Load previously seen post IDs from a JSON file."""
    if os.path.exists(SEEN_POSTS_FILE):
        try:
            with open(SEEN_POSTS_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, ValueError):
            return set()
    return set()

def save_seen_posts(seen_posts):
    """Save seen post IDs to a JSON file."""
    with open(SEEN_POSTS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(seen_posts), f)

async def appraise_scraped_gear(post_title, post_body, keywords):
    """Calls Gemini API to provide a wholesale cash-buyout estimate."""
    try:
        prompt = (
            "You are the Head Appraiser for Lumina, a luxury wholesale liquidator, and also a personal buyer of shop equipment. "
            f"Given these target keywords: {', '.join(keywords)}\n"
            "Identify the specific item in the text below and provide a concise 40-60% wholesale cash-buyout estimate. "
            "If it's shop equipment rather than audio, estimate a used market value based on general knowledge. "
            "Keep the response highly concise.\n\n"
            f"Title: {post_title}\n\nBody: {post_body}"
        )
        response = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        return response.text.strip()
    except Exception as e:
        print(f"Gemini API Error: {e}")
        return "Unknown (Appraisal Error)"

def parse_reddit_json(url):
    """Fetch the newest posts from a Reddit JSON feed."""
    headers = {"User-Agent": "LuminaHunter/2.0"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        posts = []
        if not data or "data" not in data or "children" not in data["data"]:
            return posts
            
        for item in data["data"]["children"]:
            post_data = item.get("data", {})
            title = post_data.get("title", "")
            
            # Reddit specific filter logic (e.g. requires [WTS])
            if "[wts]" not in title.lower():
                continue
                
            posts.append({
                "id": f"reddit_{post_data.get('id', 'unknown')}",
                "title": title,
                "body": post_data.get("selftext", ""),
                "author": post_data.get("author", "Unknown"),
                "url": post_data.get("url", "Unknown"),
                "source": "Reddit"
            })
        return posts
    except Exception as e:
        print(f"Error fetching Reddit JSON {url}: {e}")
        return []

def parse_rss_xml(url):
    """Fetch and parse an RSS feed."""
    try:
        feed = feedparser.parse(url)
        posts = []
        for entry in feed.entries:
            # Create a somewhat unique ID from the link and published date if available
            post_id = entry.link.split("/")[-1].replace(".html", "")
            
            # Clean up HTML description slightly if present
            body = entry.description if hasattr(entry, 'description') else ""
            body = re.sub('<[^<]+>', '', body)  # rough html strip
            
            posts.append({
                "id": f"rss_{post_id}",
                "title": entry.title,
                "body": body,
                "author": "Unknown Craigslist Poster",
                "url": entry.link,
                "source": "RSS"
            })
        return posts
    except Exception as e:
        print(f"Error fetching RSS {url}: {e}")
        return []

def parse_html_usam(url):
    """Fetch and parse US Audio Mart HTML."""
    headers = {"User-Agent": "LuminaHunter/4.0"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        posts = []
        
        # Typically USAM listings are within .list-item or similar.
        listings = soup.select(".list-item, .listing, .classified-listing, .browse-item, .item-list")
        if not listings:
            # Fallback to broader <a> tags if specific classes aren't matched perfectly
            listings = soup.select("h3 a, h2 a, .title a, .price a")
            
        for item in listings:
            if item.name == 'a':
                a_tag = item
                price_elem = item.find_next_sibling(class_=re.compile(r'price', re.I))
            else:
                a_tag = item.find('a')
                price_elem = item.select_one('.price, .item-price, .listing-price')
                
            if not a_tag or not a_tag.text.strip():
                continue
                
            title = a_tag.text.strip()
            link = a_tag.get('href', '')
            if link.startswith('/'):
                link = "https://www.usaudiomart.com" + link
            elif not link.startswith('http'):
                continue
                
            price = price_elem.text.strip() if price_elem else "Unknown Price"
            
            # Simple unique ID from the link
            post_id_parts = [p for p in link.split('/') if p]
            post_id = post_id_parts[-1] if post_id_parts else link
            if post_id == 'usaudiomart.com':
                 post_id = link.replace('http:', '').replace('https:', '').replace('/', '')
                 
            # Append entry
            posts.append({
                "id": f"usam_{post_id}",
                "title": f"{title} - {price}",
                "body": f"Found on US Audio Mart: {title} for {price}",
                "author": "USAM Seller",
                "url": link,
                "source": "US Audio Mart"
            })
            
        return posts
    except Exception as e:
        print(f"Error fetching USAM HTML {url}: {e}")
        return []

async def process_omni_sources():
    """Main execution loop for all targets and sources."""
    config = load_config()
    targets = config.get("targets", {})
    sources = config.get("sources", {})
    
    seen_posts = load_seen_posts()
    export_dir = os.path.join(BASE_DIR, "leads_export")
    os.makedirs(export_dir, exist_ok=True)
    
    total_scanned = 0
    total_matches = 0

    for source_name, source_data in sources.items():
        print(f"Scanning source: {source_name} ({source_data['url']})")
        
        # 1. Fetch raw posts from the source type
        posts = []
        if source_data["type"] == "reddit_json":
            posts = parse_reddit_json(source_data["url"])
        elif source_data["type"] == "rss":
            posts = parse_rss_xml(source_data["url"])
        elif source_data["type"] == "html_usam":
            posts = parse_html_usam(source_data["url"])
        else:
            print(f"Unknown source type: {source_data['type']}")
            continue
            
        total_scanned += len(posts)
        
        # 2. Iterate through posts and match against categories
        for post in posts:
            if post["id"] in seen_posts:
                continue
                
            matched_category = None
            matched_keywords = []
            
            title_lower = post["title"].lower()
            body_lower = post["body"].lower()
            searchable_text = title_lower + " " + body_lower
            
            # Check the specific target categories assigned to this source
            for category_name in source_data.get("categories", []):
                category_data = targets.get(category_name)
                if not category_data:
                    continue
                    
                keywords_lower = [k.lower() for k in category_data.get("keywords", [])]
                
                if any(kw in searchable_text for kw in keywords_lower):
                    matched_category = category_name
                    matched_keywords = category_data.get("keywords", [])
                    break
            
            # 3. Export lead if a match occurred
            if matched_category:
                estimated_value = await appraise_scraped_gear(post["title"], post["body"], matched_keywords)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = "".join(c for c in post["author"] if c.isalnum() or c in " _-").replace(" ", "_").strip()
                if not safe_name: safe_name = "ScrapedLead"
                
                filename = f"Lead_{matched_category}_{safe_name}_{post['id']}_{timestamp}.md"
                filepath = os.path.join(export_dir, filename)
                
                md_content = f"""---
title: "Lead: {post['title'][:50]}..."
tags: [scrape, {source_name}, {matched_category}]
category: {matched_category}
---
# New Lead: {post['title']}
**Category:** {matched_category}
**Original Source:** {post['source']}
**Zip Code:** Unknown
**Equipment:** {post['title']}
**Condition:** Unknown
**Preferred Timeline:** Unknown
**Estimated Value:** {estimated_value}
**Source URL:** {post['url']}

**Snippet:** 
{post['body'][:300]}...
"""
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(md_content)
                    
                print(f"  -> Exported match {matched_category} post to {filename}")
                total_matches += 1
                
                seen_posts.add(post["id"])
                save_seen_posts(seen_posts)

    return total_scanned, total_matches

async def main():
    print(f"[{datetime.now().isoformat()}] Starting Lumina Phase 3.2 Omni-Hunter Engine...")
    scanned, matched = await process_omni_sources()
    print(f"[{datetime.now().isoformat()}] Hunter Engine cycle complete. Scanned {scanned} distinct entries, found {matched} new matches.")

if __name__ == "__main__":
    asyncio.run(main())
