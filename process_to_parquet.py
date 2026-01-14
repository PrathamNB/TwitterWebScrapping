"""
Data Processing Pipeline with Concurrent Processing
Cleans, normalizes, and partitions tweet data into Parquet format
"""

import re
import json
import unicodedata
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm


INPUT_CSV = "data/raw/tweets_combined.csv"
OUTPUT_DIR = Path("data/processed_parquet")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MAX_WORKERS = 4

ZERO_WIDTH_CHARS = "".join(["\u200b", "\u200c", "\u200d", "\ufeff"])


def normalize_text(text: str) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKC", text)
    text = text.translate({ord(c): None for c in ZERO_WIDTH_CHARS})
    return re.sub(r"\s+", " ", text).strip()


def safe_int(value, default=0) -> int:
    try:
        return default if pd.isna(value) else int(value)
    except (ValueError, TypeError):
        return default


def parse_timestamp(ts_str: str):
    if ts_str is None or (isinstance(ts_str, float) and pd.isna(ts_str)):
        return None
    try:
        return datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def extract_hashtags(text: str) -> List[str]:
    return re.findall(r"#\w+", text or "")


def extract_mentions(text: str) -> List[str]:
    return re.findall(r"@\w+", text or "")


def parse_csv_col(value) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    s = str(value).strip()
    return [x.strip() for x in s.split(",") if x.strip()] if s else []


def process_partition(date_str: str, partition_df: pd.DataFrame) -> Tuple[str, int]:
    try:
        partition_df["like_count"] = partition_df["like_count"].apply(safe_int)
        partition_df["retweet_count"] = partition_df["retweet_count"].apply(safe_int)
        partition_df["reply_count"] = partition_df["reply_count"].apply(safe_int)

        cols = [
            "tweet_id",
            "username",
            "handle",
            "timestamp_utc",
            "content",
            "like_count",
            "retweet_count",
            "reply_count",
            "hashtags_json",
            "mentions_json",
            "url",
            "date",
        ]
        cols = [c for c in cols if c in partition_df.columns]

        partition_dir = OUTPUT_DIR / f"date={date_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_df[cols].to_parquet(
            partition_dir / "tweets.parquet", index=False, compression="snappy"
        )

        return date_str, len(partition_df)
    except Exception as e:
        print(f"Error processing {date_str}: {e}")
        return date_str, 0


def main():
    print("=" * 70)
    print("Data Processing Pipeline")
    print("=" * 70)

    try:
        df = pd.read_csv(INPUT_CSV, encoding="utf-8")
        print(f"\n[1/6] Loaded {len(df)} rows")
    except FileNotFoundError:
        print(f"ERROR: {INPUT_CSV} not found. Run scraper first.")
        return

    print("[2/6] Normalizing text...")
    for col in ["content", "username", "handle"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text)

    print("[3/6] Filtering last 24h...")
    df["ts_dt"] = df["timestamp_utc"].apply(parse_timestamp)
    df = df[df["ts_dt"].notna()]
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    df = df[df["ts_dt"] >= cutoff]
    print(f"    Valid: {len(df)} tweets")

    print("[4/6] Extracting hashtags/mentions...")
    hashtags = (
        df["hashtags"].apply(parse_csv_col)
        if "hashtags" in df.columns
        else df["content"].apply(extract_hashtags)
    )
    mentions = (
        df["mentions"].apply(parse_csv_col)
        if "mentions" in df.columns
        else df["content"].apply(extract_mentions)
    )

    hashtags = [
        (h if h else extract_hashtags(c)) for h, c in zip(hashtags, df["content"])
    ]
    mentions = [
        (m if m else extract_mentions(c)) for m, c in zip(mentions, df["content"])
    ]

    df["hashtags_json"] = [json.dumps(h, ensure_ascii=False) for h in hashtags]
    df["mentions_json"] = [json.dumps(m, ensure_ascii=False) for m in mentions]

    print("[5/6] Deduplicating...")
    df = df.sort_values("ts_dt", ascending=False)
    initial = len(df)
    if "tweet_id" in df.columns:
        df = df.drop_duplicates("tweet_id", keep="first")
    df = df.drop_duplicates("url", keep="first")
    print(f"Removed {initial - len(df)} duplicates - {len(df)} unique")

    df["date"] = df["ts_dt"].dt.strftime("%Y-%m-%d")

    print(f"[6/6] Saving to Parquet ({MAX_WORKERS} workers)...")
    date_groups = list(df.groupby("date"))

    results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_partition, date, group): date
            for date, group in date_groups
        }

        with tqdm(total=len(date_groups), desc="Partitions") as pbar:
            for future in as_completed(futures):
                try:
                    date, count = future.result()
                    results.append((date, count))
                except Exception as e:
                    print(f"\nError: {e}")
                pbar.update(1)

    print(f"\n{'='*70}")
    print(f"Complete{len(df)} tweets {len(results)} partitions")
    print(f"Output: {OUTPUT_DIR}")
    for date, count in sorted(results):
        print(f"  {date}: {count} tweets")
    print("=" * 70)


if __name__ == "__main__":
    main()
