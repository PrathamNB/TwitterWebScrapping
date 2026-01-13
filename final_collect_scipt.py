# collect_combined_final_prod.py
# Production-ready single combined-query collector (attach mode) with:
# - debug-chrome attach (no login automation)
# - combined hashtags query + since:YYYY-MM-DD (no until)
# - extracts hashtags + mentions columns
# - efficient O(1) dedupe while streaming (sets)
# - tail-only parsing to reduce repeated work
# - robust handling of transient UI failures:
#     * detects "Something went wrong / Try reloading" (and similar)
#     * cooldown + refresh with backoff
#     * additional cooldown on throttling before stopping
# - graceful stop conditions
# - logging to console + file
# - periodic checkpoint saves (so you don’t lose progress)
#
# Usage:
# 1) Start debug Chrome (keep open) and login to X:
#    chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\temp\x_debug_profile" --start-minimized
# 2) Run:
#    python collect_combined_final_prod.py
#
# Output:
#   data/raw/tweets_combined.csv
#   data/raw/tweets_partial.csv   (checkpoint)
#   collector.log                (logs)

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


# -------------------------
# Config (tweak if needed)
# -------------------------
OUT_CSV = Path("data/raw/tweets_combined.csv")
PARTIAL_CSV = Path("data/raw/tweets_partial.csv")
LOG_FILE = Path("collector.log")

TARGET = 2000                 # target rows
MAX_SCROLLS = 2500            # hard safety cap

TAIL_PARSE_N = 70             # parse only last N articles per loop (optimization)
MAX_NO_NEW_ROUNDS = 18        # stop if no new unique tweets for many rounds
MAX_ERROR_RECOVERS = 8        # stop if too many overlay recoveries
MAX_OLD_HITS = 450            # stop if many older-than-24h tweets (boundary crossed)

# checkpoint saves
CHECKPOINT_EVERY = 200         # save partial file every N collected tweets

# Cooldown grows on consecutive errors (in seconds)
COOLDOWN_BASE = 10
COOLDOWN_CAP = 90

# If throttling is detected, pause once before giving up
THROTTLE_COOLDOWN_SECONDS = 60
THROTTLE_COOLDOWN_TRIGGER = 10  # when no_new_rounds reaches this, do a 60s pause once


# -------------------------
# Logging
# -------------------------
def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def log(msg):
    logging.info(msg)


# -------------------------
# Selenium attach
# -------------------------
def build_attached_driver():
    opts = Options()
    # Attach to already-open Chrome started with --remote-debugging-port=9222
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    return webdriver.Chrome(options=opts)


# -------------------------
# Page + parsing helpers
# -------------------------
def has_error_overlay(driver) -> bool:
    src = (driver.page_source or "").lower()
    return (
        ("something went wrong" in src and "try reloading" in src)
        or ("try again" in src)
        or ("rate limit" in src)
    )


def cooldown_then_refresh(driver, attempt: int):
    cooldown = min(COOLDOWN_CAP, COOLDOWN_BASE * (attempt + 1))
    log(f"[RECOVER] Error overlay detected. Cooling down {cooldown}s...")
    time.sleep(cooldown)
    log("[RECOVER] Refreshing page...")
    driver.refresh()
    time.sleep(random.uniform(4.0, 7.0))


def warmup_scrolls(driver, n=2):
    # After refresh, X sometimes needs a couple scrolls before loading again
    for _ in range(n):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(1.2, 2.0))


def human_sleep(a=0.9, b=1.9):
    time.sleep(random.uniform(a, b))


def parse_ts(ts: str):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def safe_text(parent, css):
    try:
        return parent.find_element(By.CSS_SELECTOR, css).text.strip()
    except Exception:
        return ""


def safe_attr(parent, css, attr):
    try:
        return parent.find_element(By.CSS_SELECTOR, css).get_attribute(attr)
    except Exception:
        return None


def parse_count(s: str) -> int:
    s = (s or "").replace(",", "").strip()
    if not s:
        return 0
    mult = 1
    if s.endswith("K"):
        mult = 1000
        s = s[:-1]
    elif s.endswith("M"):
        mult = 1_000_000
        s = s[:-1]
    try:
        return int(float(s) * mult)
    except Exception:
        return 0


def extract_hashtags(text: str):
    return re.findall(r"#\w+", text or "")


def extract_mentions(text: str):
    return re.findall(r"@\w+", text or "")


def extract_one(article):
    ts_raw = safe_attr(article, "time", "datetime")
    ts = parse_ts(ts_raw)
    if not ts:
        return None

    # authoritative last-24h filter (UTC)
    if ts < datetime.now(timezone.utc) - timedelta(hours=24):
        return "OLD"

    content = safe_text(article, '[data-testid="tweetText"]')
    if not content:
        return None

    url, tid = "", ""
    try:
        for a in article.find_elements(By.CSS_SELECTOR, "a"):
            h = a.get_attribute("href") or ""
            if "/status/" in h:
                url = h
                tid = h.split("/status/")[-1].split("?")[0]
                break
    except Exception:
        pass

    tags = extract_hashtags(content)
    ments = extract_mentions(content)

    return {
        "tweet_id": tid,
        "timestamp_utc": ts_raw,
        "content": content,
        "like_count": parse_count(safe_text(article, '[data-testid="like"]')),
        "retweet_count": parse_count(safe_text(article, '[data-testid="retweet"]')),
        "reply_count": parse_count(safe_text(article, '[data-testid="reply"]')),
        "hashtags": ",".join(tags),
        "mentions": ",".join(ments),
        "url": url,
        "query": "combined_since",
    }


def build_search_url_since_only():
    # You asked for "since:YYYY-MM-DD" only (no until).
    # We'll keep it dynamic: since yesterday (local date).
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    query_text = f"(#nifty50 OR #sensex OR #intraday OR #banknifty) since:{yesterday}"
    q = quote(query_text)
    return f"https://x.com/search?q={q}&src=typed_query&f=live"


def checkpoint_save(rows):
    # Save partial progress safely
    try:
        pd.DataFrame(rows).to_csv(PARTIAL_CSV, index=False, encoding="utf-8")
        log(f"[CHECKPOINT] Saved partial file: {PARTIAL_CSV} (rows={len(rows)})")
    except Exception as e:
        log(f"[CHECKPOINT] Failed to save partial: {e}")


# -------------------------
# Main
# -------------------------
def main():
    setup_logging()
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    driver = build_attached_driver()

    rows = []
    seen_ids = set()
    seen_urls = set()

    search_url = build_search_url_since_only()
    log("Opening search URL:")
    log(search_url)

    throttling_cooldown_used = False

    try:
        driver.get(search_url)
        time.sleep(5)

        scrolls = 0
        no_new_rounds = 0
        old_hits = 0
        error_recovers = 0

        while len(rows) < TARGET and scrolls < MAX_SCROLLS:
            # 1) Error overlay recovery
            if has_error_overlay(driver):
                cooldown_then_refresh(driver, error_recovers)
                warmup_scrolls(driver, n=2)
                error_recovers += 1
                if error_recovers >= MAX_ERROR_RECOVERS:
                    log("[STOP] Too many overlay recover attempts. Stopping gracefully.")
                    break
                continue
            else:
                error_recovers = 0

            articles = driver.find_elements(By.CSS_SELECTOR, "article")
            if not articles:
                no_new_rounds += 1
                log(f"[WARN] No articles visible (no_new_rounds={no_new_rounds}).")
                if no_new_rounds >= MAX_NO_NEW_ROUNDS:
                    log("[STOP] No content repeatedly. Stopping.")
                    break
                warmup_scrolls(driver, n=1)
                continue

            # 2) Optimization: only parse tail window
            tail = articles[-TAIL_PARSE_N:] if len(articles) > TAIL_PARSE_N else articles

            new_this_round = 0
            for art in tail:
                data = extract_one(art)

                if data == "OLD":
                    old_hits += 1
                    continue
                if not data:
                    continue

                tid = data.get("tweet_id") or ""
                url = data.get("url") or ""

                # streaming dedupe
                if tid and tid in seen_ids:
                    continue
                if url and url in seen_urls:
                    continue

                if tid:
                    seen_ids.add(tid)
                if url:
                    seen_urls.add(url)

                rows.append(data)
                new_this_round += 1

                # progress logs + checkpoint
                if len(rows) % 100 == 0:
                    log(f"Collected {len(rows)} tweets (scrolls={scrolls})")

                if len(rows) % CHECKPOINT_EVERY == 0:
                    checkpoint_save(rows)

                if len(rows) >= TARGET:
                    break

            # 3) Stop if we crossed the 24h boundary often
            if old_hits >= MAX_OLD_HITS:
                log("[STOP] Seeing many older-than-24h tweets. Likely past boundary. Stopping.")
                break

            # 4) Throttling / no-progress logic
            if new_this_round == 0:
                no_new_rounds += 1
            else:
                no_new_rounds = 0

            if no_new_rounds == THROTTLE_COOLDOWN_TRIGGER and not throttling_cooldown_used:
                log(f"[THROTTLE] No new tweets for {no_new_rounds} rounds. Cooling down {THROTTLE_COOLDOWN_SECONDS}s...")
                time.sleep(THROTTLE_COOLDOWN_SECONDS)
                throttling_cooldown_used = True
                warmup_scrolls(driver, n=2)

            if no_new_rounds >= MAX_NO_NEW_ROUNDS:
                log("[STOP] No new unique tweets for many rounds (throttling). Stopping.")
                break

            # 5) Scroll + pacing
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # gentle adaptive pacing
            if new_this_round >= 10:
                time.sleep(random.uniform(0.6, 1.1))
            elif new_this_round >= 3:
                time.sleep(random.uniform(1.0, 1.7))
            else:
                time.sleep(random.uniform(1.8, 2.8))

            scrolls += 1

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    if not rows:
        log("No tweets collected. Ensure X results are visible in debug Chrome.")
        return

    df = pd.DataFrame(rows)

    # Final safety: authoritative last-24h filter + dedupe again
    df["ts_dt"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df[df["ts_dt"].notna()]
    df = df[df["ts_dt"] >= datetime.now(timezone.utc) - timedelta(hours=24)]
    df = df.drop(columns=["ts_dt"])

    if "tweet_id" in df.columns:
        df = df.drop_duplicates(subset=["tweet_id"], keep="first")
    df = df.drop_duplicates(subset=["url"], keep="first")

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    log(f"\nFinal count: {len(df)} tweets")
    log(f"Saved to {OUT_CSV}")

    # one last checkpoint copy
    checkpoint_save(rows)


if __name__ == "__main__":
    main()
