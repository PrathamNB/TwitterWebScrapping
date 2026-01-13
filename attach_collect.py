import time, random, re
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

OUT_CSV = "data/raw/tweets_sample.csv"
QUERY = "(%23nifty50%20OR%20%23sensex%20OR%20%23intraday%20OR%20%23banknifty)"

def human_sleep(a=0.8, b=1.6):
    time.sleep(random.uniform(a, b))

def build_attached_driver():
    opts = Options()
    #  Attach to the already-open, logged-in Chrome
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    return webdriver.Chrome(options=opts)

def parse_count(s: str) -> int:
    s = (s or "").replace(",", "").strip()
    if not s:
        return 0
    mult = 1
    if s.endswith("K"):
        mult = 1000; s = s[:-1]
    elif s.endswith("M"):
        mult = 1_000_000; s = s[:-1]
    try:
        return int(float(s) * mult)
    except:
        return 0

def safe_find_text(parent, css):
    try: return parent.find_element(By.CSS_SELECTOR, css).text.strip()
    except: return ""

def safe_find_attr(parent, css, attr):
    try: return parent.find_element(By.CSS_SELECTOR, css).get_attribute(attr) or ""
    except: return ""

def extract_one_article(article):
    ts = safe_find_attr(article, "time", "datetime")
    content = safe_find_text(article, '[data-testid="tweetText"]')

    username, handle = "", ""
    try:
        spans = article.find_elements(By.CSS_SELECTOR, "span")
        for sp in spans:
            t = (sp.text or "").strip()
            if t.startswith("@"):
                handle = t
                break
        if spans:
            username = (spans[0].text or "").strip()
    except:
        pass

    tweet_url, tweet_id = "", ""
    try:
        for a in article.find_elements(By.CSS_SELECTOR, "a"):
            href = a.get_attribute("href") or ""
            if "/status/" in href:
                tweet_url = href
                tweet_id = href.split("/status/")[-1].split("?")[0]
                break
    except:
        pass

    reply_count = parse_count(safe_find_text(article, '[data-testid="reply"]'))
    retweet_count = parse_count(safe_find_text(article, '[data-testid="retweet"]'))
    like_count = parse_count(safe_find_text(article, '[data-testid="like"]'))

    hashtags = re.findall(r"#\w+", content)
    mentions = re.findall(r"@\w+", content)

    if not content and not tweet_url:
        return None

    return {
        "tweet_id": tweet_id,
        "username": username,
        "handle": handle,
        "timestamp_utc": ts,
        "content": content,
        "like_count": like_count,
        "retweet_count": retweet_count,
        "reply_count": reply_count,
        "mentions": ",".join(mentions),
        "hashtags": ",".join(hashtags),
        "url": tweet_url,
    }

def collect(driver, limit=50):
    rows, seen = [], set()
    url = f"https://x.com/search?q={QUERY}&src=typed_query&f=live"
    driver.get(url)
    human_sleep(2.0, 3.0)

    scrolls = 0
    while len(rows) < limit and scrolls < 120:
        for art in driver.find_elements(By.CSS_SELECTOR, "article"):
            data = extract_one_article(art)
            if not data:
                continue
            key = data["tweet_id"] or data["url"] or (data["content"] + data["timestamp_utc"])
            if key in seen:
                continue
            seen.add(key)
            rows.append(data)
            if len(rows) >= limit:
                break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        human_sleep(1.0, 2.0)
        scrolls += 1

    return rows

if __name__ == "__main__":
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    print("SCRIPT STARTED", flush=True)
    driver = build_attached_driver()
    print("DRIVER ATTACHED", flush=True)
    rows = collect(driver, limit=50)
    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"Saved {len(df)} tweets -> {OUT_CSV}")
    print(df.head(3).to_string(index=False))
