import time
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MERGED_CSV = OUT_DIR / "tweets_merged.csv"

HASHTAGS = ["nifty50", "sensex", "intraday", "banknifty"]  # PDF hashtags
TARGET_PER_TAG = 400  # 4*600 = 2400 (you can reduce to 500)
MAX_SCROLLS_PER_TAG = 1200


def log(msg):
    print(msg, flush=True)


def human_sleep(a=0.9, b=1.8):
    time.sleep(random.uniform(a, b))


def build_attached_driver():
    opts = Options()
    # attaches to your already-open debug Chrome
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    return webdriver.Chrome(options=opts)


def has_error_overlay(driver) -> bool:
    src = (driver.page_source or "").lower()
    return ("something went wrong" in src and "try reloading" in src) or ("try again" in src)


def recover(driver, attempt: int):
    wait_s = min(60, 6 * (attempt + 1))
    log(f"[RECOVER] Overlay detected. Refreshing + waiting {wait_s}s...")
    driver.refresh()
    time.sleep(wait_s)


def parse_ts(ts):
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


def extract(article):
    ts_raw = safe_attr(article, "time", "datetime")
    ts = parse_ts(ts_raw)
    if not ts:
        return None

    # last 24 hours only
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

    return {
        "tweet_id": tid,
        "timestamp_utc": ts_raw,
        "content": content,
        "like_count": parse_count(safe_text(article, '[data-testid="like"]')),
        "retweet_count": parse_count(safe_text(article, '[data-testid="retweet"]')),
        "reply_count": parse_count(safe_text(article, '[data-testid="reply"]')),
        "url": url,
    }


def wait_for_more_articles(driver, prev_count, timeout=12):
    start = time.time()
    while time.time() - start < timeout:
        cur = len(driver.find_elements(By.CSS_SELECTOR, "article"))
        if cur > prev_count:
            return True
        time.sleep(0.6)
    return False


def collect_for_hashtag(driver, tag: str, target: int):
    """
    Collect tweets for one hashtag from Live feed.
    Auto-recovers on X overlay errors.
    Stops when older tweets dominate or target reached.
    """
    rows, seen = [], set()

    q = f"%23{tag}"
    url = f"https://x.com/search?q={q}&src=typed_query&f=live"
    log(f"\n=== Collecting for #{tag} ===")
    log("Opening: " + url)
    driver.get(url)
    time.sleep(4)

    recover_attempts = 0
    old_hits = 0
    stagnant = 0

    scrolls = 0
    while len(rows) < target and scrolls < MAX_SCROLLS_PER_TAG:
        # recover if overlay
        if has_error_overlay(driver):
            recover(driver, recover_attempts)
            recover_attempts += 1
            if recover_attempts >= 6:
                log("[STOP] Too many recover attempts for this hashtag.")
                break
            continue
        else:
            recover_attempts = 0

        articles = driver.find_elements(By.CSS_SELECTOR, "article")
        new_this_round = 0

        for art in articles:
            data = extract(art)

            if data == "OLD":
                old_hits += 1
                continue

            if not data:
                continue

            key = data["tweet_id"] or data["url"] or (data["content"] + data["timestamp_utc"])
            if key in seen:
                continue

            seen.add(key)
            rows.append(data)
            new_this_round += 1

            if len(rows) % 100 == 0:
                log(f"#{tag}: {len(rows)} collected")

            if len(rows) >= target:
                break

        # stop if mostly older tweets (means we passed 24h window)
        if old_hits > 350:
            log(f"[STOP] #{tag}: reached older tweets frequently (24h boundary).")
            break

        prev_count = len(articles)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        loaded_more = wait_for_more_articles(driver, prev_count, timeout=12)
        human_sleep(0.9, 1.8)

        if new_this_round == 0 and not loaded_more:
            stagnant += 1
        else:
            stagnant = 0

        if stagnant >= 10:
            log(f"[STOP] #{tag}: no new tweets for many scrolls (throttling).")
            break

        scrolls += 1

    log(f"#{tag}: finished with {len(rows)} tweets")
    return rows


def main():
    driver = build_attached_driver()
    all_rows = []

    try:
        for tag in HASHTAGS:
            rows = collect_for_hashtag(driver, tag, TARGET_PER_TAG)

            # Save per-hashtag for safety/debug
            out_csv = OUT_DIR / f"tweets_{tag}.csv"
            pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8")
            log(f"Saved per-tag file: {out_csv}")

            all_rows.extend(rows)

    finally:
        # Do NOT close debug chrome; only detaching selenium
        try:
            driver.quit()
        except Exception:
            pass

    df = pd.DataFrame(all_rows)
    if df.empty:
        log("No data collected. Check if X feed is loading in debug Chrome.")
        return

    # Dedup: tweet_id then url
    if "tweet_id" in df.columns:
        df = df.drop_duplicates(subset=["tweet_id"], keep="first")
    df = df.drop_duplicates(subset=["url"], keep="first")

    # last-24h final filter (safety)
    df["ts_dt"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df[df["ts_dt"].notna()]
    df = df[df["ts_dt"] >= datetime.now(timezone.utc) - timedelta(hours=24)]
    df = df.drop(columns=["ts_dt"])

    df.to_csv(MERGED_CSV, index=False, encoding="utf-8")
    log(f"\nMerged final count: {len(df)} tweets")
    log(f"Saved merged CSV: {MERGED_CSV}")


if __name__ == "__main__":
    main()
