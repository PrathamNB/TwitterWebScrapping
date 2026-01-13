"""
Twitter Scraper - Unified Turbo Version (MINIMAL FIXES APPLIED)

Applied (as requested):
1) Process more DOM tweets each loop: articles[-20:] -> articles[-80:]
2) Replace PAGE_DOWN scrolling with JS scroll + simple stall nudge

Everything else kept the same (no overcoding).
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

# --- CONFIGURATION ---
TARGET = 2000
REFRESH_EVERY = 150  # Clears browser memory every 150 tweets
CHECKPOINT_EVERY = 50
MAX_SCROLLS = 8000
OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUTPUT_DIR / "tweets_combined.csv"
PARTIAL_CSV = OUTPUT_DIR / "tweets_partial.csv"


# --- LOGGING SETUP ---
def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
    )


def log(msg, level="info"):
    getattr(logging, level)(msg)


# --- HELPER FUNCTIONS ---
def build_driver():
    opts = Options()
    # Assumes Chrome started with remote debugging, e.g.:
    # chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\chrome-profile"
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    return webdriver.Chrome(options=opts)


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
        mult, s = 1000000, s[:-1]
    try:
        return int(float(s) * mult)
    except:
        return 0


def parse_ts(ts: str):
    """Robust ISO timestamp parsing."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except:
        return None


def build_url():
    """Builds the search query for since yesterday (date-based); we enforce true rolling 24h in code."""
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    query = f"(#nifty50 OR #sensex OR #intraday OR #banknifty) since:{yesterday}"
    return f"https://x.com/search?q={quote(query)}&src=typed_query&f=live"


def checkpoint(rows):
    """Saves progress periodically to partial_csv."""
    try:
        pd.DataFrame(rows).to_csv(PARTIAL_CSV, index=False, encoding="utf-8")
        log(f"Checkpoint saved: {len(rows)} total rows processed.")
    except Exception as e:
        log(f"Checkpoint failed: {e}", "error")


def metric_count(article, testid_options):
    """
    Minimal robustness:
    - sometimes 'repost' instead of 'retweet'
    - sometimes count text is inside spans
    """
    for tid in testid_options:
        try:
            el = article.find_element(By.CSS_SELECTOR, f'[data-testid="{tid}"]')
            txt = (el.text or "").strip()
            if txt:
                return parse_count(txt)

            spans = el.find_elements(By.CSS_SELECTOR, "span")
            for sp in spans:
                t = (sp.text or "").strip()
                if t:
                    return parse_count(t)
        except:
            continue
    return 0


# --- CORE EXTRACTION ---
def extract_tweet(article):
    """Full extraction including metrics, optimized for speed."""
    try:
        ts_raw = safe_attr(article, "time", "datetime")
        ts = parse_ts(ts_raw)
        if not ts:
            return None

        # 24h Filter
        if ts < datetime.now(timezone.utc) - timedelta(hours=24):
            return "OLD"

        content = safe_text(article, '[data-testid="tweetText"]')

        url = ""
        for a in article.find_elements(By.CSS_SELECTOR, "a"):
            h = a.get_attribute("href") or ""
            if "/status/" in h:
                url = h.split("?")[0]
                break
        if not url:
            return None

        return {
            "tweet_id": url.split("/")[-1],
            "timestamp_utc": ts_raw,
            "content": content,
            "like_count": metric_count(article, ["like"]),
            "retweet_count": metric_count(article, ["retweet", "repost"]),
            "reply_count": metric_count(article, ["reply"]),
            "hashtags": ",".join(re.findall(r"#\w+", content)),
            "mentions": ",".join(re.findall(r"@\w+", content)),
            "url": url,
            "query": "combined_finance",
        }
    except:
        return None


# --- MAIN LOOP ---
def main():
    setup_logging()
    log("=" * 70)
    log("UNIFIED TURBO SCRAPER STARTING (MINIMAL FIXES)")
    log("=" * 70)

    driver = build_driver()
    rows, seen_ids = [], set()

    url = build_url()
    log(f"Target URL: {url}")
    driver.get(url)
    time.sleep(6)

    scrolls = 0
    last_refresh_count = 0

    try:
        while len(rows) < TARGET and scrolls < MAX_SCROLLS:
            articles = driver.find_elements(By.CSS_SELECTOR, "article")
            new_this_batch = 0
            old_this_batch = 0

            # CHANGE #1: process more recent tweets per loop
            for art in articles[-80:]:
                data = extract_tweet(art)

                if data == "OLD":
                    old_this_batch += 1
                    continue

                if not data or data["tweet_id"] in seen_ids:
                    continue

                seen_ids.add(data["tweet_id"])
                rows.append(data)
                new_this_batch += 1

            # CHANGE #2: JS scroll + simple stall nudge
            prev_count = len(articles)
            driver.execute_script("window.scrollBy(0, 1400);")
            time.sleep(random.uniform(1.0, 1.6))

            new_count = len(driver.find_elements(By.CSS_SELECTOR, "article"))
            if new_count <= prev_count and new_this_batch == 0:
                # nudge harder if stuck
                driver.execute_script("window.scrollBy(0, 2600);")
                time.sleep(2.0)

            scrolls += 1

            if len(rows) % CHECKPOINT_EVERY == 0 and new_this_batch > 0:
                log(
                    f"Progress: {len(rows)}/{TARGET} | Scrolls: {scrolls} | old_in_batch={old_this_batch}"
                )
                checkpoint(rows)

            # BROWSER REFRESH: Clears DOM memory lag
            if len(rows) - last_refresh_count >= REFRESH_EVERY:
                log("Refreshing page to clear DOM memory and restore speed...")
                driver.refresh()
                time.sleep(8)
                last_refresh_count = len(rows)

    except KeyboardInterrupt:
        log("Manual stop. Saving data...")
    except Exception as e:
        log(f"Critical Error: {e}", "error")
    finally:
        if rows:
            df = pd.DataFrame(rows).drop_duplicates("tweet_id")
            df.to_csv(OUT_CSV, index=False, encoding="utf-8")
            log("=" * 70)
            log(f"FINISHED: {len(df)} unique tweets saved to {OUT_CSV}.")
            log("=" * 70)
        else:
            log("No rows collected. Nothing to save.", "warning")

        try:
            driver.quit()
        except:
            pass


if __name__ == "__main__":
    main()
