import re
import json
import unicodedata
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd


INPUT_CSV = r"C:\Users\pratham.shetty\Desktop\market_intel\data\raw\tweets_sample.csv"          # put your path if different
OUT_DIR = "data/processed_parquet"       # output folder


# --- Cleaning helpers ---
ZW_CHARS = "".join(["\u200b", "\u200c", "\u200d", "\ufeff"])

def normalize_text(text: str) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    t = str(text)
    t = unicodedata.normalize("NFKC", t)
    t = t.translate({ord(c): None for c in ZW_CHARS})
    t = re.sub(r"\s+", " ", t).strip()
    return t

def to_int(x, default=0):
    try:
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default

def parse_ts(ts: str):
    # converts "2026-01-13T07:25:12.000Z" to datetime
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    s = str(ts).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def extract_tags(text: str):
    # fallback extraction from content if hashtags column is empty
    if not text:
        return []
    return re.findall(r"#\w+", text)

def extract_mentions(text: str):
    if not text:
        return []
    return re.findall(r"@\w+", text)


def main():
    df = pd.read_csv(INPUT_CSV, encoding="utf-8")

    # Normalize / clean
    df["content"] = df["content"].apply(normalize_text)
    df["username"] = df["username"].apply(normalize_text)
    df["handle"] = df["handle"].apply(normalize_text)

    # Fix numeric types
    df["like_count"] = df["like_count"].apply(to_int)
    df["retweet_count"] = df["retweet_count"].apply(to_int)
    df["reply_count"] = df["reply_count"].apply(to_int)

    # Parse timestamps
    df["ts_dt"] = df["timestamp_utc"].apply(parse_ts)

    # Filter: last 24 hours (based on UTC)
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=24)

    df = df[df["ts_dt"].notna()]
    df = df[df["ts_dt"] >= cutoff]

    # Mentions / hashtags: turn into JSON arrays (unicode safe)
    # if original mentions/hashtags column exists, use it, otherwise extract from content
    def split_csv_cell(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return []
        s = str(x).strip()
        if not s:
            return []
        return [p.strip() for p in s.split(",") if p.strip()]

    hashtags_list = df["hashtags"].apply(split_csv_cell) if "hashtags" in df.columns else df["content"].apply(extract_tags)
    mentions_list = df["mentions"].apply(split_csv_cell) if "mentions" in df.columns else df["content"].apply(extract_mentions)

    # If hashtags column is empty for some rows, fallback to extracting from content
    hashtags_list = [
        (h if len(h) > 0 else extract_tags(c))
        for h, c in zip(hashtags_list, df["content"].tolist())
    ]
    mentions_list = [
        (m if len(m) > 0 else extract_mentions(c))
        for m, c in zip(mentions_list, df["content"].tolist())
    ]

    df["hashtags_json"] = [json.dumps(x, ensure_ascii=False) for x in hashtags_list]
    df["mentions_json"] = [json.dumps(x, ensure_ascii=False) for x in mentions_list]

    # Deduplication: prefer tweet_id; fallback to url
    # Keep the newest if duplicates exist
    df = df.sort_values("ts_dt", ascending=False)
    if "tweet_id" in df.columns:
        df = df.drop_duplicates(subset=["tweet_id"], keep="first")
    df = df.drop_duplicates(subset=["url"], keep="first")

    # Partition key (date)
    df["date"] = df["ts_dt"].dt.strftime("%Y-%m-%d")

    # Save to Parquet (partitioned by date)
    out = Path(OUT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    # Keep only final columns (clean)
    final_cols = [
        "tweet_id","username","handle","timestamp_utc","content",
        "like_count","retweet_count","reply_count",
        "hashtags_json","mentions_json","url","date"
    ]
    final_cols = [c for c in final_cols if c in df.columns]
    df_final = df[final_cols].copy()

    for date, part in df_final.groupby("date"):
        part_dir = out / f"date={date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part.to_parquet(part_dir / "tweets.parquet", index=False)

    print(f"Rows after last-24h filter + dedupe: {len(df_final)}")
    print(f"Saved parquet to: {OUT_DIR}")


if __name__ == "__main__":
    main()
