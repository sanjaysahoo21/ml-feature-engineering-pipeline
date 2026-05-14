#!/usr/bin/env python3
"""
batch_analysis.py
=================
Batch computation of the same features produced by the Flink streaming pipeline.
Reads raw events from the Kafka user-events topic, computes features using pandas,
and prints a comparison table.

Usage:
    pip install kafka-python pandas tabulate
    python batch_analysis.py [--bootstrap-servers localhost:9092] [--limit 5000]
"""

import argparse
import json
from datetime import datetime, timezone, timedelta
import pandas as pd

def parse_args():
    p = argparse.ArgumentParser(description="Batch feature computation for pipeline analysis")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--limit", type=int, default=5000,
                   help="Max events to consume from user-events topic")
    p.add_argument("--output", default="batch_features.csv",
                   help="CSV output path for computed features")
    return p.parse_args()


def consume_events(bootstrap_servers: str, limit: int) -> list[dict]:
    """Consume raw events from the user-events Kafka topic."""
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("[ERROR] kafka-python not installed. Run: pip install kafka-python")
        raise

    consumer = KafkaConsumer(
        "user-events",
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=None,   # read-only, no committed offsets
    )

    events = []
    for msg in consumer:
        events.append(msg.value)
        if len(events) >= limit:
            break

    consumer.close()
    print(f"[batch] Consumed {len(events)} events from user-events topic.")
    return events


def compute_user_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tumbling 1-hour window per user.
    Computes: click_rate, avg_dwell_time
    (mirrors FlinkFeatureJob.UserFeatureWindowFunction)
    """
    df["hour_window"] = df["timestamp"].dt.floor("1h")
    grouped = df.groupby(["user_id", "hour_window"])

    result = grouped.apply(lambda g: pd.Series({
        "click_rate":     (g["event_type"] == "click").sum() / len(g),
        "avg_dwell_time": g["dwell_time_ms"].mean(),
        "event_count":    len(g),
    })).reset_index()

    return result


def compute_content_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sliding 15-min window / 5-min slide per content.
    Approximated in batch as fixed 15-min buckets for comparison.
    (mirrors FlinkFeatureJob.ContentFeatureWindowFunction)
    """
    df["window_15m"] = df["timestamp"].dt.floor("15min")
    grouped = df.groupby(["content_id", "window_15m"])

    def eng_rate(g):
        views = (g["event_type"] == "view").sum()
        eng   = g["event_type"].isin(["like", "share"]).sum()
        return eng / views if views > 0 else 0.0

    result = grouped.apply(lambda g: pd.Series({
        "engagement_rate": eng_rate(g),
        "view_count":      (g["event_type"] == "view").sum(),
    })).reset_index()

    return result


def compute_category_affinity(df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    """
    Category affinity per user per hour.
    Requires content_id -> category mapping (from content-metadata topic or hard-coded).
    """
    df["category"] = df["content_id"].map(metadata).fillna("unknown")
    df["hour_window"] = df["timestamp"].dt.floor("1h")

    result = (
        df.groupby(["user_id", "hour_window", "category"])
        .size()
        .reset_index(name="category_affinity_score")
    )
    result["feature_name"] = "category_affinity_" + result["category"]
    return result


def print_summary(user_feats: pd.DataFrame, content_feats: pd.DataFrame,
                  category_feats: pd.DataFrame):
    try:
        from tabulate import tabulate
        print("\n=== User Features (Tumbling 1h) ===")
        print(tabulate(user_feats.head(20), headers="keys", tablefmt="grid", floatfmt=".4f"))

        print("\n=== Content Features (15-min bucket) ===")
        print(tabulate(content_feats.head(20), headers="keys", tablefmt="grid", floatfmt=".4f"))

        print("\n=== Category Affinity (Tumbling 1h) ===")
        print(tabulate(category_feats.head(20), headers="keys", tablefmt="grid"))
    except ImportError:
        print(user_feats.head(20).to_string())
        print(content_feats.head(20).to_string())
        print(category_feats.head(20).to_string())


def main():
    args = parse_args()

    # Hard-coded metadata matching DataGeneratorService.initMetadata()
    content_metadata = {
        "content1": "scifi",
        "content2": "news",
        "content3": "sports",
        "content4": "comedy",
    }

    # Consume events
    raw = consume_events(args.bootstrap_servers, args.limit)
    if not raw:
        print("[batch] No events found. Is the producer running?")
        return

    # Build DataFrame
    df = pd.DataFrame(raw)
    df["timestamp"]    = pd.to_datetime(df["timestamp"], utc=True)
    df["dwell_time_ms"] = df["dwell_time_ms"].astype(int)

    print(f"[batch] Event time range: {df['timestamp'].min()} — {df['timestamp'].max()}")
    print(f"[batch] Unique users:   {df['user_id'].nunique()}")
    print(f"[batch] Unique content: {df['content_id'].nunique()}")
    print(f"[batch] Event types:\n{df['event_type'].value_counts()}\n")

    # Compute features
    user_feats     = compute_user_features(df)
    content_feats  = compute_content_features(df)
    category_feats = compute_category_affinity(df, content_metadata)

    print_summary(user_feats, content_feats, category_feats)

    # Save outputs
    user_feats.to_csv("batch_user_features.csv", index=False)
    content_feats.to_csv("batch_content_features.csv", index=False)
    category_feats.to_csv("batch_category_features.csv", index=False)
    print(f"\n[batch] CSVs written: batch_user_features.csv, batch_content_features.csv, batch_category_features.csv")
    print("[batch] Compare these values against the feature-store Kafka topic to validate streaming accuracy.")


if __name__ == "__main__":
    main()
