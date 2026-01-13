import os
import time
import random
import re
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


OUT_CSV = "data/raw/tweets_sample.csv"

QUERY_TEXT = "#nifty50 OR #sensex OR #intraday OR #banknifty"


def human_sleep(a=0.8, b=1.6):
    time.sleep(random.uniform(a, b))


def build_driver(headless=False):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")

    # ✅ Use your normal Chrome user data dir (auto from Windows env)
    # This is the correct base path for Chrome on Windows:
    chrome_user_data = os.path.join(os.environ["LOCALAPPDATA"], "Google", "Chrome", "User Data")
    opts.add_argument(rf"--user-data-dir={chrome_user_data}")

    # ✅ Use Default profile (as you asked)
    opts.add_argument("--profile-directory=Default")

    opts.add_argument("--log-level=3")

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver


def looks_like_login(driver) -> bool:
    u = (driver.current_url or "").lower()
    if "login" in u or "i/flow/login" in u:
        return True
    src = (driver.page_source or "").lower()
    if "log in" in src and "sign up" in src:
        return True
    return False


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
    except:
        return 0


def safe_find_text(parent, css):
    try:
        return parent.find_element(By.CSS_SELECTOR, css).text.strip()
    except:
        return ""


def safe_find_attr(parent, css, attr):
    try:
        return parent.find_element(By.CSS_SELECTOR, css).get_attribute(attr) or ""
    except:
        return ""


def extract_one_article(article):
    timestamp_utc = safe_find_attr(article, "time", "datetime")
    content = safe_find_text(article, '[data-testid="tweetText"]')

    username = ""
    handle = ""
    try:
        spans = article.find_elements(By.CSS_SELECTOR, "span")
        for sp in spans:
            txt = (sp.text or "").strip()
            if txt.startswith("@"):
                handle = txt
                break
        if spans:
            username = (spans[0].text or "").strip()
    except:
        pass

    tweet_url = ""
    tweet_id = ""
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
        "timestamp_utc": timestamp_utc,
        "content": content,
        "like_count": like_count,
        "retweet_count": retweet_count,
        "reply_count": reply_count,
        "mentions": ",".join(mentions),
        "hashtags": ",".join(hashtags),
        "url": tweet_url,
    }


def collect_by_typing(driver, limit=50):
    wait = WebDriverWait(driver, 25)

    print("Opening explore page...")
    driver.get("https://x.com/explore")
    human_sleep(2.0, 3.0)

    print("Current URL:", driver.current_url)
    if looks_like_login(driver):
        raise RuntimeError(
            "X is showing login. Fix:\n"
            "1) Open normal Chrome (not Selenium)\n"
            "2) Login to X\n"
            "3) Close Chrome fully\n"
            "4) Run this script again"
        )

    # Wait for the search input, then type query
    print("Waiting for search box...")
    search_input = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'input[data-testid="SearchBox_Search_Input"]'))
    )

    print("Typing query...")
    search_input.click()
    search_input.send_keys(Keys.CONTROL, "a")
    search_input.send_keys(Keys.BACKSPACE)
    search_input.send_keys(QUERY_TEXT)
    search_input.send_keys(Keys.ENTER)

    human_sleep(2.0, 3.0)

    # Click "Latest" tab to get recent tweets
    print("Switching to Latest tab...")
    try:
        latest_tab = wait.until(EC.element_to_be_clickable((By.XPATH, '//span[text()="Latest"]')))
        latest_tab.click()
    except Exception:
        # Sometimes the tab text differs; if not found, proceed anyway
        print("Could not click Latest tab (continuing).")

    human_sleep(2.0, 3.0)

    # Now collect articles
    rows = []
    seen = set()
    scrolls = 0

    print("Collecting tweets...")
    while len(rows) < limit and scrolls < 120:
        articles = driver.find_elements(By.CSS_SELECTOR, "article")
        for art in articles:
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
        human_sleep(1.2, 2.0)
        scrolls += 1

    return rows


def main():
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    driver = build_driver(headless=False)
    try:
        rows = collect_by_typing(driver, limit=50)
        df = pd.DataFrame(rows)
        df.to_csv(OUT_CSV, index=False, encoding="utf-8")
        print(f"Saved: {len(df)} tweets to {OUT_CSV}")
        print(df.head(3).to_string(index=False))
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
