"""
Market Sentiment Signal Generation with TF-IDF and Keyword Analysis
Transforms textual tweet data into quantitative trading signals
"""

import json
import math
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import StandardScaler


PARQUET_DIR = "data/processed_parquet"
OUTPUT_DIR = Path("data/signals")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BULLISH = {
    "buy",
    "bull",
    "bullish",
    "breakout",
    "long",
    "support",
    "upside",
    "target",
    "rally",
    "green",
    "profit",
    "gain",
    "moon",
    "rocket",
    "accumulate",
    "hold",
    "bounce",
    "surge",
    "pump",
}

BEARISH = {
    "sell",
    "bear",
    "bearish",
    "short",
    "breakdown",
    "resistance",
    "downside",
    "stoploss",
    "crash",
    "dump",
    "fall",
    "red",
    "loss",
    "exit",
    "panic",
    "drop",
    "decline",
    "correction",
}


def tokenize(text: str) -> set:
    if not text:
        return set()
    return {t.strip(".,!?:;()[]{}\"'").lower() for t in text.split() if len(t) > 1}


def keyword_sentiment(text: str) -> int:
    tokens = tokenize(text)
    score = 0
    if tokens & BULLISH:
        score += 1
    if tokens & BEARISH:
        score -= 1
    return score


def engagement_weight(row) -> float:
    raw = (
        row["like_count"] * 1.0 + row["retweet_count"] * 2.0 + row["reply_count"] * 0.5
    )
    return math.log1p(max(raw, 0.0))


def keyword_signal(row) -> float:
    base = keyword_sentiment(row["content"])
    return base * engagement_weight(row) if base != 0 else 0.0


def compute_tfidf_sentiment(df: pd.DataFrame) -> np.ndarray:
    if len(df) < 10:
        return np.zeros(len(df))

    try:
        vectorizer = TfidfVectorizer(
            max_features=200,
            min_df=2,
            max_df=0.7,
            stop_words="english",
            ngram_range=(1, 2),
            sublinear_tf=True,
        )
        tfidf_matrix = vectorizer.fit_transform(df["content"].fillna(""))

        svd = TruncatedSVD(n_components=1, random_state=42)
        sentiment_raw = svd.fit_transform(tfidf_matrix).flatten()

        scaler = StandardScaler()
        return scaler.fit_transform(sentiment_raw.reshape(-1, 1)).flatten()
    except Exception as e:
        print(f"TF-IDF failed: {e}")
        return np.zeros(len(df))


def combined_signal(row, tfidf_score: float) -> float:
    kw = keyword_signal(row)
    eng = engagement_weight(row)
    return (0.6 * kw) + (0.4 * tfidf_score * eng)


def bootstrap_ci(values: np.ndarray, n=1000, alpha=0.05):
    if len(values) == 0:
        return 0.0, (0.0, 0.0)

    rng = np.random.default_rng(42)
    means = [
        rng.choice(values, size=len(values), replace=True).mean() for _ in range(n)
    ]

    return float(np.mean(values)), (
        float(np.percentile(means, alpha / 2 * 100)),
        float(np.percentile(means, (1 - alpha / 2) * 100)),
    )


def main():
    print("=" * 60)
    print("Signal Generation Pipeline")
    print("=" * 60)

    # Load data
    parquet_files = list(Path(PARQUET_DIR).rglob("tweets.parquet"))
    if not parquet_files:
        print(f"ERROR: No parquet files in {PARQUET_DIR}")
        return

    df = pd.concat([pd.read_parquet(p) for p in parquet_files], ignore_index=True)
    print(f"\nLoaded {len(df)} tweets from {len(parquet_files)} partitions")

    # Parse timestamps
    df["ts"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])

    # Compute signals
    print("Computing keyword signals...")
    df["keyword_signal"] = df.apply(keyword_signal, axis=1)

    print("Computing TF-IDF signals...")
    df["tfidf_sentiment"] = compute_tfidf_sentiment(df)

    print("Computing combined signals...")
    df["combined_signal"] = df.apply(
        lambda r: combined_signal(r, r["tfidf_sentiment"]), axis=1
    )

    # Hourly aggregation
    print("Aggregating hourly with confidence intervals...")
    hourly = (
        df.set_index("ts")
        .resample("1H")[["keyword_signal", "tfidf_sentiment", "combined_signal"]]
        .apply(list)
        .reset_index()
    )

    records = []
    for _, row in hourly.iterrows():
        kw_mean, kw_ci = bootstrap_ci(np.array(row["keyword_signal"]))
        tfidf_mean, tfidf_ci = bootstrap_ci(np.array(row["tfidf_sentiment"]))
        comb_mean, comb_ci = bootstrap_ci(np.array(row["combined_signal"]))

        records.append(
            {
                "time": row["ts"],
                "keyword_signal": kw_mean,
                "keyword_ci_low": kw_ci[0],
                "keyword_ci_high": kw_ci[1],
                "tfidf_signal": tfidf_mean,
                "tfidf_ci_low": tfidf_ci[0],
                "tfidf_ci_high": tfidf_ci[1],
                "combined_signal": comb_mean,
                "combined_ci_low": comb_ci[0],
                "combined_ci_high": comb_ci[1],
                "tweet_count": len(row["combined_signal"]),
            }
        )

    signals = pd.DataFrame(records)

    # Save
    output_csv = OUTPUT_DIR / "hourly_signals.csv"
    signals.to_csv(output_csv, index=False)
    print(f"\n✓ Saved: {output_csv}")
    print(f"\nSignals:\n{signals}")

    # Visualize
    fig, axes = plt.subplots(3, 1, figsize=(12, 10))

    for ax, signal_col, ci_low, ci_high, title, color in [
        (
            axes[0],
            "keyword_signal",
            "keyword_ci_low",
            "keyword_ci_high",
            "Keyword Sentiment",
            "blue",
        ),
        (
            axes[1],
            "tfidf_signal",
            "tfidf_ci_low",
            "tfidf_ci_high",
            "TF-IDF Semantic",
            "green",
        ),
        (
            axes[2],
            "combined_signal",
            "combined_ci_low",
            "combined_ci_high",
            "Combined Hybrid",
            "red",
        ),
    ]:
        ax.plot(
            signals["time"], signals[signal_col], label=title, color=color, linewidth=2
        )
        ax.fill_between(signals["time"], signals[ci_low], signals[ci_high], alpha=0.3)
        ax.axhline(0, color="gray", linestyle="--", alpha=0.5)
        ax.set_ylabel("Signal")
        ax.legend()
        ax.grid(alpha=0.3)

    axes[2].set_xlabel("Time (UTC)")
    plt.tight_layout()

    plot_path = OUTPUT_DIR / "signals.png"
    plt.savefig(plot_path, dpi=150, bbox_inches="tight")
    print(f"✓ Plot: {plot_path}")
    plt.show()

    print(
        f"\n{'='*60}\nComplete! Processed {len(df)} tweets → {len(signals)} hourly signals\n{'='*60}"
    )


if __name__ == "__main__":
    main()
