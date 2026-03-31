"""
src/analysis/sentiment.py
--------------------------
VADER sentiment analysis — purpose-built for short user-generated text.

Each review gets:
  sentiment_score  : compound in [-1.0, +1.0]
  sentiment_label  : 'Positive' | 'Neutral' | 'Negative'
  sentiment_pos    : proportion positive tokens
  sentiment_neg    : proportion negative tokens
  sentiment_neu    : proportion neutral tokens
"""

import pandas as pd
from src.config import POSITIVE_THRESHOLD, NEGATIVE_THRESHOLD


def _label(score: float) -> str:
    if score >= POSITIVE_THRESHOLD:
        return "Positive"
    if score <= NEGATIVE_THRESHOLD:
        return "Negative"
    return "Neutral"


def analyze_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add sentiment columns to a reviews DataFrame.
    Safe on empty DataFrames and null review_text values.
    """
    if df.empty:
        return df

    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    result = df.copy()

    def _score(text):
        t = str(text).strip() if pd.notna(text) else ""
        if not t:
            return {"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0}
        return analyzer.polarity_scores(t)

    scores = result["review_text"].apply(_score)

    result["sentiment_score"] = scores.apply(lambda s: round(s["compound"], 4))
    result["sentiment_pos"]   = scores.apply(lambda s: round(s["pos"],      4))
    result["sentiment_neg"]   = scores.apply(lambda s: round(s["neg"],      4))
    result["sentiment_neu"]   = scores.apply(lambda s: round(s["neu"],      4))
    result["sentiment_label"] = result["sentiment_score"].apply(_label)

    return result


def compute_velocity(df: pd.DataFrame, window_days: int = 30) -> dict:
    """
    Compute sentiment velocity: % change between the most recent window
    and the preceding window of the same length.

    Returns:
        {
          "recent_avg":   0.42,
          "previous_avg": 0.38,
          "delta":        0.04,
          "pct_change":   10.5,
          "direction":    "improving" | "declining" | "stable",
        }
    """
    if df.empty or "sentiment_score" not in df.columns or "review_date" not in df.columns:
        return {"direction": "stable", "pct_change": 0.0, "delta": 0.0,
                "recent_avg": 0.0, "previous_avg": 0.0}

    ts = df.copy()
    ts["review_date"] = pd.to_datetime(ts["review_date"], errors="coerce")
    ts = ts.dropna(subset=["review_date", "sentiment_score"]).sort_values("review_date")

    if ts.empty:
        return {"direction": "stable", "pct_change": 0.0, "delta": 0.0,
                "recent_avg": 0.0, "previous_avg": 0.0}

    latest = ts["review_date"].max()
    cutoff = latest - pd.Timedelta(days=window_days)
    cutoff2 = cutoff - pd.Timedelta(days=window_days)

    recent   = ts[ts["review_date"] >  cutoff]["sentiment_score"]
    previous = ts[(ts["review_date"] > cutoff2) & (ts["review_date"] <= cutoff)]["sentiment_score"]

    if recent.empty or previous.empty:
        return {"direction": "stable", "pct_change": 0.0, "delta": 0.0,
                "recent_avg": float(recent.mean()) if not recent.empty else 0.0,
                "previous_avg": 0.0}

    r_avg = float(recent.mean())
    p_avg = float(previous.mean())
    delta = r_avg - p_avg

    # Use delta on the [-1, +1] scale — NOT percentage of a near-zero baseline.
    # A 0.1 delta on a -1..+1 scale IS meaningful and is capped at ±2.0 max.
    # We express it as "points on a 100-point scale" for display readability.
    display_delta = round(delta * 100, 1)   # e.g. +0.11 → "+11 points"

    if delta > 0.05:
        direction = "improving"
    elif delta < -0.05:
        direction = "declining"
    else:
        direction = "stable"

    return {
        "recent_avg":   round(r_avg, 4),
        "previous_avg": round(p_avg, 4),
        "delta":        round(delta, 4),
        "pct_change":   display_delta,   # now means "score points changed (×100)"
        "direction":    direction,
    }


def get_summary_stats(df: pd.DataFrame) -> dict:
    """Aggregate sentiment stats — used by the metrics row and history log."""
    if df.empty or "sentiment_score" not in df.columns:
        return {
            "total": 0,
            "avg_compound": 0.0,
            "pct_positive": 0.0,
            "pct_neutral":  0.0,
            "pct_negative": 0.0,
            "avg_rating":   0.0,
        }

    labels = df["sentiment_label"].value_counts()
    total  = len(df)

    return {
        "total":        total,
        "avg_compound": round(float(df["sentiment_score"].mean()), 4),
        "pct_positive": round(100 * labels.get("Positive", 0) / total, 1),
        "pct_neutral":  round(100 * labels.get("Neutral",  0) / total, 1),
        "pct_negative": round(100 * labels.get("Negative", 0) / total, 1),
        "avg_rating":   round(float(df["rating"].dropna().mean()), 2) if "rating" in df.columns else 0.0,
    }