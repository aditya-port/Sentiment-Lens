"""
src/analysis/themes.py
-----------------------
Three distinct NLP analyses:

1. Keyword extraction  — TF-IDF per sentiment bucket
2. Aspect sentiment    — Rule-based ABSA for 6 universal aspects
3. Topic clustering    — K-Means on TF-IDF features, labelled by top terms
"""

import re
import string
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.preprocessing import normalize

from src.config import (
    ASPECT_KEYWORDS, N_CLUSTERS, MIN_REVIEWS_FOR_CLUSTERING
)

REVIEW_STOP_WORDS = {
    "place", "came", "come", "go", "went", "got", "get", "like", "just",
    "really", "very", "bit", "little", "quite", "much", "also", "time",
    "nice", "good", "great", "bad", "best", "worst", "definitely", "actually",
    "even", "though", "tried", "try", "ve", "don", "didn", "wasn", "isn",
    "couldn", "wouldn", "hasn", "ll", "re", "doesn", "things", "thing",
    "one", "two", "three", "first", "last", "next", "back", "will", "would",
    "could", "make", "made", "going", "said", "look", "looked", "feel",
    "felt", "got", "use", "used", "way", "lot", "always", "never", "every",
}


def _clean(text: str) -> str:
    text = str(text).lower()
    text = re.sub(r"[%s]" % re.escape(string.punctuation), " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ─────────────────────────────────────────────────────────────────────────────
# 1. Keyword Extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_keywords(texts: list, top_n: int = 15) -> list[tuple[str, float]]:
    """Top-N TF-IDF keywords from a list of texts. Returns [(term, score)]."""
    cleaned = [_clean(t) for t in texts if str(t).strip()]
    if len(cleaned) < 3:
        return []

    try:
        vec = TfidfVectorizer(
            stop_words="english",
            max_features=800,
            ngram_range=(1, 2),
            min_df=2,
            max_df=0.88,
            token_pattern=r"\b[a-z][a-z]{2,}\b",
        )
        matrix = vec.fit_transform(cleaned)
    except ValueError:
        return []

    terms = vec.get_feature_names_out()
    avg   = np.asarray(matrix.mean(axis=0)).flatten()

    pairs = [
        (t, float(s))
        for t, s in zip(terms, avg)
        if t not in REVIEW_STOP_WORDS and s > 0
    ]
    pairs.sort(key=lambda x: x[1], reverse=True)
    return pairs[:top_n]


def get_sentiment_keywords(df: pd.DataFrame, top_n: int = 15) -> dict:
    """
    Extract keywords from each sentiment bucket independently.

    Returns: {"positive": [(kw, score)], "negative": [...], "neutral": [...]}
    """
    out = {}
    for label in ("Positive", "Negative", "Neutral"):
        texts = df[df["sentiment_label"] == label]["review_text"].dropna().tolist()
        out[label.lower()] = extract_keywords(texts, top_n=top_n)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 2. Aspect Sentiment (rule-based ABSA)
# ─────────────────────────────────────────────────────────────────────────────

def get_aspect_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each of 6 universal aspects, find reviews that mention it and
    compute average sentiment + mention count.

    Returns a sorted DataFrame with columns:
        aspect, mention_count, avg_sentiment, pct_positive, sentiment_label
    """
    if df.empty or "sentiment_score" not in df.columns:
        return pd.DataFrame()

    rows = []
    for aspect, keywords in ASPECT_KEYWORDS.items():
        pattern = r"\b(?:" + "|".join(re.escape(k) for k in keywords) + r")\b"
        mask    = df["review_text"].fillna("").str.lower().str.contains(pattern, regex=True)
        subset  = df[mask]

        if len(subset) < 2:
            continue

        avg  = float(subset["sentiment_score"].mean())
        ppos = round(100.0 * (subset["sentiment_label"] == "Positive").sum() / len(subset), 1)
        rows.append({
            "aspect":          aspect,
            "mention_count":   len(subset),
            "avg_sentiment":   round(avg, 3),
            "pct_positive":    ppos,
            "sentiment_label": (
                "Positive" if avg >= 0.05
                else "Negative" if avg <= -0.05
                else "Neutral"
            ),
        })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("mention_count", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Topic Clustering (K-Means on TF-IDF)
# ─────────────────────────────────────────────────────────────────────────────

def cluster_reviews(df: pd.DataFrame, n_clusters: int = N_CLUSTERS) -> pd.DataFrame:
    """
    Cluster reviews into n_clusters topics using K-Means on TF-IDF features.

    Adds two columns to the returned DataFrame:
      - topic_cluster : int cluster ID
      - topic_label   : human-readable label (top 3 TF-IDF terms)

    Returns the input DataFrame unchanged if fewer than MIN_REVIEWS_FOR_CLUSTERING
    non-empty reviews are present.
    """
    if df.empty or "review_text" not in df.columns:
        return df

    texts   = df["review_text"].fillna("").astype(str)
    valid   = texts.str.strip().str.len() > 0
    n_valid = valid.sum()

    if n_valid < MIN_REVIEWS_FOR_CLUSTERING:
        df = df.copy()
        df["topic_cluster"] = 0
        df["topic_label"]   = "General"
        return df

    # Clamp cluster count to number of valid reviews
    k = min(n_clusters, n_valid)

    try:
        vec = TfidfVectorizer(
            stop_words="english",
            max_features=500,
            ngram_range=(1, 2),
            min_df=2,
            token_pattern=r"\b[a-z][a-z]{2,}\b",
        )
        X = vec.fit_transform(texts.where(valid, ""))
        X_norm = normalize(X)

        km = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
        km.fit(X_norm)
    except Exception:
        df = df.copy()
        df["topic_cluster"] = 0
        df["topic_label"]   = "General"
        return df

    labels      = km.labels_
    feature_names = vec.get_feature_names_out()

    # Build human-readable label: top 3 terms per cluster centroid
    cluster_labels = {}
    for cluster_id in range(k):
        centroid  = km.cluster_centers_[cluster_id]
        top_idxs  = centroid.argsort()[::-1][:4]
        top_terms = [
            t for t in feature_names[top_idxs]
            if t not in REVIEW_STOP_WORDS
        ][:3]
        cluster_labels[cluster_id] = " · ".join(top_terms) if top_terms else f"Topic {cluster_id + 1}"

    result = df.copy()
    result["topic_cluster"] = labels
    result["topic_label"]   = result["topic_cluster"].map(cluster_labels)

    return result


def get_cluster_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarise each topic cluster: label, size, avg sentiment, % positive.

    Returns a DataFrame sorted by cluster size descending.
    """
    if df.empty or "topic_cluster" not in df.columns:
        return pd.DataFrame()

    rows = []
    for cid, group in df.groupby("topic_cluster"):
        label   = group["topic_label"].iloc[0]
        avg_s   = group["sentiment_score"].mean() if "sentiment_score" in group.columns else 0.0
        ppos    = 100.0 * (group["sentiment_label"] == "Positive").sum() / len(group) if "sentiment_label" in group.columns else 0.0
        rows.append({
            "cluster_id":    int(cid),
            "topic":         label,
            "review_count":  len(group),
            "avg_sentiment": round(float(avg_s), 3),
            "pct_positive":  round(ppos, 1),
        })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("review_count", ascending=False).reset_index(drop=True)
