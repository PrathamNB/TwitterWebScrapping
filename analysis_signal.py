import json
import math
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


PARQUET_DIR = "data/processed_parquet"

# Very simple, explainable keyword lists
BULLISH = {
    "buy", "bull", "bullish", "breakout", "long",
    "support", "upside", "target", "rally"
}

BEARISH = {
    "sell", "bear", "bearish", "short", "breakdown",
    "resistance", "downside", "stoploss"
}


def tokenize(text: str):
    if not text:
        return set()
    return {
        t.strip(".,!?:;()[]{}").lower()
        for t in text.split()
        if len(t) > 1
    }


def direction_score(text: str) -> int:
    toks = tokenize(text)
    score = 0
    if toks & BULLISH:
        score += 1
    if toks & BEARISH:
        score -= 1
    return score


def engagement_weight(row):
    # simple, smooth weighting
    raw = (
        row["like_count"] * 1.0 +
        row["retweet_count"] * 2.0 +
        row["reply_count"] * 0.5
    )
    return math.log1p(max(raw, 0.0))


def tweet_score(row):
    base = direction_score(row["content"])
    if base == 0:
        return 0.0
    return base * engagement_weight(row)


def bootstrap_ci(values, n=500, alpha=0.05):
    if len(values) == 0:
        return 0.0, (0.0, 0.0)

    rng = np.random.default_rng(42)
    means = []

    for _ in range(n):
        sample = rng.choice(values, size=len(values), replace=True)
        means.append(sample.mean())

    mean_val = float(np.mean(values))
    lo = float(np.quantile(means, alpha / 2))
    hi = float(np.quantile(means, 1 - alpha / 2))
    return mean_val, (lo, hi)


def main():
    # Load all parquet partitions
    dfs = []
    for p in Path(PARQUET_DIR).rglob("tweets.parquet"):
        dfs.append(pd.read_parquet(p))

    df = pd.concat(dfs, ignore_index=True)

    # Parse timestamps
    df["ts"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])

    # Compute per-tweet score
    df["score"] = df.apply(tweet_score, axis=1)

    # Aggregate hourly (memory-efficient)
    hourly = (
        df.set_index("ts")
          .resample("1H")["score"]
          .apply(list)
          .reset_index()
    )

    records = []
    for _, row in hourly.iterrows():
        mean, (lo, hi) = bootstrap_ci(np.array(row["score"]))
        records.append({
            "time": row["ts"],
            "signal_mean": mean,
            "ci_low": lo,
            "ci_high": hi,
            "tweet_count": len(row["score"])
        })

    sig = pd.DataFrame(records)

    print("\nHourly Signal:")
    print(sig)

    # Plot (low memory — aggregated only)
    plt.figure(figsize=(10, 4))
    plt.plot(sig["time"], sig["signal_mean"], label="Signal")
    plt.fill_between(
        sig["time"],
        sig["ci_low"],
        sig["ci_high"],
        alpha=0.3,
        label="Confidence Interval"
    )
    plt.title("Market Sentiment Signal (Hourly)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Signal")
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
