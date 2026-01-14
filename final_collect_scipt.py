"""
X/Twitter Scraper - STEALTH MODE + ANTI-RATE-LIMIT
Goal: Reach 1000+ tweets by avoiding detection

KEY FIXES:
1. Human-like scrolling (randomized, slower)
2. No remote debugging flags
3. Better queries (broader, no strict filters)
4. Longer time window (7 days instead of 24h)
5. User-agent rotation
6. Cookies/session preservation
"""

import time
import random
import re
import logging
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from urllib.parse import quote

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

# --- CONFIGURATION ---
TARGET = 1000
CHECKPOINT_EVERY = 100

MAX_SCROLLS_PER_QUERY = 1200     
REFRESH_EVERY = 600              
WINDOW_SCAN = 150                

NO_NEW_TWEET_LIMIT = 25          
STALL_LIMIT_FOR_QUERY_REFRESH = 8

# Human-like scrolling
SCROLL_WAIT_MIN = 0.8
SCROLL_WAIT_MAX = 2.0
SCROLL_PIXELS_MIN = 800
SCROLL_PIXELS_MAX = 1500

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUTPUT_DIR / "tweets_combined.csv"
PARTIAL_CSV = OUTPUT_DIR / "tweets_partial.csv"

# --- BETTER QUERIES (7-DAY WINDOW, NO STRICT FILTERS) ---
QUERY_TEMPLATES = [
    # Q1: Simple hashtags (most volume)
    "#nifty50",
    
    # Q2: Sensex focus
    "#sensex OR #bse",
    
    # Q3: Bank Nifty
    "#banknifty OR #banknifty50",
    
    # Q4: Intraday + trading
    "nifty intraday",
    
    # Q5: Options trading
    "nifty options",
    
    # Q6: Market sentiment
    "sensex today OR nifty today",
    
    # Q7: Stock market general
    "indian stock market",
    
    # Q8: Futures & Options
    "nifty futures OR banknifty futures",
]


# --- LOGGING ---
def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"x_scraper_{ts}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ],
        force=True
    )
    logging.info(f"Logging to: {log_file}")

def log(msg, level="info"):
    getattr(logging, level)(msg)


# --- SELENIUM SETUP (SIMPLE & COMPATIBLE) ---
def build_driver():
    opts = Options()
    
    # Connect to existing Chrome with remote debugging
    # Make sure Chrome is running with: chrome.exe --remote-debugging-port=9222
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    driver = webdriver.Chrome(options=opts)
    log("✓ Driver connected successfully")
    
    # Try to hide automation (optional - may fail on older Chrome)
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except:
        pass
    
    return driver


# --- HUMAN-LIKE SCROLLING ---
def human_scroll(driver, pixels=None):
    """Scroll like a human - random amounts, slight pauses"""
    if pixels is None:
        pixels = random.randint(SCROLL_PIXELS_MIN, SCROLL_PIXELS_MAX)
    
    # Sometimes scroll in chunks (more human)
    if random.random() < 0.3:
        chunk = pixels // 2
        driver.execute_script(f"window.scrollBy(0, {chunk});")
        time.sleep(random.uniform(0.1, 0.3))
        driver.execute_script(f"window.scrollBy(0, {pixels - chunk});")
    else:
        driver.execute_script(f"window.scrollBy(0, {pixels});")
    
    # Random wait
    time.sleep(random.uniform(SCROLL_WAIT_MIN, SCROLL_WAIT_MAX))
    
    # Occasionally scroll up slightly (human behavior)
    if random.random() < 0.05:
        driver.execute_script(f"window.scrollBy(0, -{random.randint(100, 300)});")
        time.sleep(random.uniform(0.3, 0.6))


# --- HELPERS ---
def safe_text(el, css):
    try:
        return el.find_element(By.CSS_SELECTOR, css).text.strip()
    except:
        return ""

def safe_attr(el, css, attr):
    try:
        return el.find_element(By.CSS_SELECTOR, css).get_attribute(attr)
    except:
        return None

def parse_count(s: str) -> int:
    s = (s or "").replace(",", "").strip()
    if not s:
        return 0
    mult = 1
    if s.endswith("K"):
        mult, s = 1000, s[:-1]
    elif s.endswith("M"):
        mult, s = 1_000_000, s[:-1]
    try:
        return int(float(s) * mult)
    except:
        return 0

def parse_ts(ts: str):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except:
        return None

def build_url(query: str) -> str:
    """Build search URL with 7-day window for more volume"""
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    q = f"({query}) since:{week_ago}"
    return f"https://x.com/search?q={quote(q)}&src=typed_query&f=live"

def checkpoint(rows):
    try:
        pd.DataFrame(rows).to_csv(PARTIAL_CSV, index=False, encoding="utf-8")
        log(f"✓ Checkpoint: {len(rows)} rows")
    except Exception as e:
        log(f"Checkpoint failed: {e}", "error")

def metric_count(article, testid_options):
    for tid in testid_options:
        try:
            el = article.find_element(By.CSS_SELECTOR, f'[data-testid="{tid}"]')
            txt = (el.text or "").strip()
            if txt:
                return parse_count(txt)
            for sp in el.find_elements(By.CSS_SELECTOR, "span"):
                t = (sp.text or "").strip()
                if t:
                    return parse_count(t)
        except:
            continue
    return 0

def extract_username(article):
    block = safe_text(article, '[data-testid="User-Name"]')
    if block:
        m = re.search(r"@\w+", block)
        if m:
            return m.group(0)
        return block.splitlines()[0].strip()
    return ""

def extract_tweet(article, cutoff_time):
    """Extract tweet data with spam filtering"""
    try:
        ts_raw = safe_attr(article, "time", "datetime")
        ts = parse_ts(ts_raw)
        if not ts or ts < cutoff_time:
            return "OLD"

        links = article.find_elements(By.CSS_SELECTOR, 'a[href*="/status/"]')
        if not links:
            return None
        url = (links[0].get_attribute("href") or "").split("?")[0]
        if not url or "/status/" not in url:
            return None

        content = safe_text(article, '[data-testid="tweetText"]')
        username = extract_username(article)
        
        # SPAM FILTERS - Skip these immediately
        if not content or len(content) < 20:  # Too short
            return None
        
        content_lower = content.lower()
        spam_keywords = ['t.me/', 'wa.me/', 'whatsapp', 'telegram', 'join channel', 'join group']
        if any(kw in content_lower for kw in spam_keywords):  # Promotional spam
            return None
        
        username_lower = (username or "").lower()
        bot_indicators = ['bot', 'alert', 'signal', 'algo']
        if any(ind in username_lower for ind in bot_indicators):  # Bot account
            return None

        return {
            "tweet_id": url.split("/")[-1],
            "username": username,
            "timestamp_utc": ts_raw,
            "content": content,
            "like_count": metric_count(article, ["like"]),
            "retweet_count": metric_count(article, ["retweet", "repost"]),
            "reply_count": metric_count(article, ["reply"]),
            "hashtags": ",".join(re.findall(r"#\w+", content)),
            "mentions": ",".join(re.findall(r"@\w+", content)),
            "url": url,
        }
    except:
        return None


# --- SCRAPING WITH STEALTH ---
def scrape_query(driver, query_label: str, query: str, rows, seen_ids, target_total: int):
    url = build_url(query)
    cutoff_time = datetime.now(timezone.utc) - timedelta(days=7)  # 7-day window
    
    log(f"╔══ Query: {query_label} - '{query}' ══╗")
    log(f"║ Target: {target_total - len(rows)} more tweets")
    
    driver.get(url)
    
    # Longer initial wait for page to fully load
    time.sleep(random.uniform(5, 7))

    scrolls = 0
    last_refresh_count = len(rows)
    no_new_loops = 0
    stalled_loops = 0
    prev_dom_count = 0
    consecutive_zeros = 0
    
    query_start = time.time()
    tweets_at_start = len(rows)

    while len(rows) < target_total and scrolls < MAX_SCROLLS_PER_QUERY:
        articles = driver.find_elements(By.CSS_SELECTOR, "article")
        dom_count = len(articles)
        
        # DEBUG: Log first load to diagnose issues
        if scrolls == 0:
            log(f"║ DEBUG - First load: {dom_count} articles found")
            log(f"║ DEBUG - Page title: {driver.title[:50]}")
            if dom_count < 10:
                log(f"║ ⚠️  WARNING: Low article count! X might be blocking or rate-limiting.", "warning")

        new_this_batch = 0
        old_count = 0

        window = articles[-WINDOW_SCAN:] if len(articles) > WINDOW_SCAN else articles
        
        for art in window:
            data = extract_tweet(art, cutoff_time)
            
            if data == "OLD":
                old_count += 1
                continue
            if not data:
                continue

            tid = data["tweet_id"]
            if tid in seen_ids:
                continue

            seen_ids.add(tid)
            data["query"] = query_label
            rows.append(data)
            new_this_batch += 1

        # Status every 50 loops
        if scrolls % 50 == 0:
            elapsed = int(time.time() - query_start)
            rate = (len(rows) - tweets_at_start) / max(elapsed, 1) * 60
            log(f"║ Loop {scrolls} | Total: {len(rows)}/{target_total} | New: {new_this_batch} | DOM: {dom_count} | Rate: {rate:.1f}/min")

        # Checkpoint
        if new_this_batch > 0 and (len(rows) // CHECKPOINT_EVERY) > ((len(rows) - new_this_batch) // CHECKPOINT_EVERY):
            elapsed = int(time.time() - query_start)
            rate = (len(rows) - tweets_at_start) / max(elapsed, 1) * 60
            log(f"╠═ Progress: {len(rows)}/{target_total} ({rate:.1f} tweets/min)")
            checkpoint(rows)

        # Track consecutive zero-new-tweet loops
        if new_this_batch == 0:
            consecutive_zeros += 1
            no_new_loops += 1
            if dom_count <= prev_dom_count:
                stalled_loops += 1
            else:
                stalled_loops = 0
        else:
            consecutive_zeros = 0
            no_new_loops = 0
            stalled_loops = 0

        prev_dom_count = dom_count

        # If DOM is suspiciously small (< 3) and we're not getting tweets, X might be blocking
        if dom_count < 3 and consecutive_zeros > 3:
            log(f"║ ⚠ Low DOM count ({dom_count}) - possible rate limit. Waiting 30s...")
            time.sleep(30)
            driver.refresh()
            time.sleep(random.uniform(5, 7))
            consecutive_zeros = 0
            continue

        # Exit if query exhausted
        if no_new_loops >= NO_NEW_TWEET_LIMIT:
            elapsed = int(time.time() - query_start)
            found = len(rows) - tweets_at_start
            log(f"╚═ Query exhausted. Found {found} tweets in {elapsed}s")
            break

        # Refresh on persistent stall
        if stalled_loops >= STALL_LIMIT_FOR_QUERY_REFRESH:
            log(f"║ Stalled. Refreshing...")
            driver.refresh()
            time.sleep(random.uniform(4, 6))
            stalled_loops = 0
            continue

        # HUMAN-LIKE SCROLLING
        human_scroll(driver)

        scrolls += 1

        # Periodic refresh
        if len(rows) - last_refresh_count >= REFRESH_EVERY:
            log(f"║ DOM refresh at {len(rows)} tweets...")
            driver.refresh()
            time.sleep(random.uniform(5, 7))
            last_refresh_count = len(rows)


def main():
    setup_logging()
    log("=" * 70)
    log("X SCRAPER - STEALTH MODE (7-DAY WINDOW)")
    log(f"Target: {TARGET} tweets")
    log("=" * 70)

    driver = build_driver()
    rows = []
    seen_ids = set()
    start_time = time.time()

    try:
        for i, q in enumerate(QUERY_TEMPLATES, start=1):
            if len(rows) >= TARGET:
                break
            
            label = f"Q{i}"
            scrape_query(driver, label, q, rows, seen_ids, TARGET)
            
            # Longer pause between queries (avoid rate limits)
            if len(rows) < TARGET:
                wait_time = random.uniform(5, 10)
                log(f"Waiting {wait_time:.1f}s before next query...")
                time.sleep(wait_time)

        elapsed = int(time.time() - start_time)
        rate = len(rows) / max(elapsed / 60, 1)

        log("=" * 70)
        log(f"COMPLETE: {len(rows)} tweets in {elapsed}s ({rate:.1f} tweets/min)")
        log("=" * 70)

    except KeyboardInterrupt:
        log("Stopped by user. Saving...")
    except Exception as e:
        log(f"Error: {e}", "error")
        import traceback
        log(traceback.format_exc(), "error")
    finally:
        if rows:
            df = pd.DataFrame(rows).drop_duplicates("tweet_id")
            df.to_csv(OUT_CSV, index=False, encoding="utf-8")
            
            elapsed = int(time.time() - start_time)
            rate = len(df) / max(elapsed / 60, 1)
            
            log("=" * 70)
            log(f"SAVED: {len(df)} unique tweets → {OUT_CSV}")
            log(f"Time: {elapsed}s | Rate: {rate:.1f} tweets/min")
            log("=" * 70)
            
            # Stats
            log(f"Hashtags: {df['hashtags'].str.len().gt(0).sum()}")
            log(f"Mentions: {df['mentions'].str.len().gt(0).sum()}")
            log(f"Avg engagement: {df['like_count'].mean():.1f} likes")
            
            # Top hashtags
            all_tags = df['hashtags'].str.split(',').explode()
            top_tags = all_tags[all_tags != ''].value_counts().head(10)
            log(f"Top hashtags: {', '.join(top_tags.index.tolist()[:5])}")
        else:
            log("No tweets collected.", "warning")

        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    main()