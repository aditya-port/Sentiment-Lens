"""
src/analysis/authenticity.py
-----------------------------
Detects potentially inauthentic reviews using heuristic scoring.

Each review receives a suspicion_score in [0, 1].
Scores above SUSPICION_FLAG_THRESHOLD are flagged is_suspicious=True.

This is a heuristic approach — it flags statistical anomalies, not definitive
fakes. Results should be presented as "potentially suspicious" in the UI.

Heuristics:
  1. Very short text (< 15 chars) regardless of rating
  2. Single-word or empty review
  3. Very short text with 5-star rating (classic pattern for paid reviews)
  4. Extreme rating (1 or 5) with < 8 meaningful words
  5. ALL CAPS text (often bot or extreme emotional state)
  6. Excessive exclamation marks (≥ 3)
"""

import re
import pandas as pd
from src.config import SUSPICION_WEIGHTS, SUSPICION_FLAG_THRESHOLD


def score_review(row: dict) -> tuple[float, list[str]]:
    """
    Score a single review for authenticity.

    Returns:
        (score: float in [0, 1], reasons: list of str)
    """
    text   = str(row.get("review_text") or "").strip()
    rating = row.get("rating")

    score   = 0.0
    reasons = []

    # Empty or nearly empty
    if len(text) == 0:
        return (1.0, ["Empty review text"])

    word_count = len(text.split())

    # Single word
    if word_count <= 1:
        score += SUSPICION_WEIGHTS["single_word"]
        reasons.append("Single-word review")

    # Very short text
    if len(text) < 15:
        score += SUSPICION_WEIGHTS["very_short_text"]
        reasons.append("Very short text (< 15 chars)")

    # Short text + 5 stars
    if len(text) < 20 and rating == 5:
        score += SUSPICION_WEIGHTS["short_with_5star"]
        reasons.append("Short text with 5-star rating")

    # Extreme rating + very few words
    if rating in (1, 5) and word_count < 8:
        score += SUSPICION_WEIGHTS["extreme_no_context"]
        reasons.append("Extreme rating without context")

    # All caps (length > 5 to avoid short legitimate caps like "AMAZING")
    if len(text) > 5 and text == text.upper() and re.search(r"[A-Z]", text):
        score += SUSPICION_WEIGHTS["all_caps"]
        reasons.append("All-caps text")

    # Excessive exclamation
    if text.count("!") >= 3:
        score += SUSPICION_WEIGHTS["excessive_punct"]
        reasons.append("Excessive exclamation marks")

    return (min(round(score, 3), 1.0), reasons)


def analyze_authenticity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add is_suspicious, suspicion_score, and suspicion_reasons columns.
    Safe on empty DataFrames.
    """
    if df.empty:
        return df

    result = df.copy()

    scored = result.apply(
        lambda row: score_review(row.to_dict()), axis=1, result_type="expand"
    )
    result["suspicion_score"]   = scored[0]
    result["suspicion_reasons"] = scored[1]
    result["is_suspicious"]     = result["suspicion_score"] >= SUSPICION_FLAG_THRESHOLD

    return result


def get_trust_score(df: pd.DataFrame) -> float:
    """
    Overall trust score for a set of reviews: percentage of non-suspicious reviews.
    Returns a value in [0, 100].
    """
    if df.empty or "is_suspicious" not in df.columns:
        return 100.0
    n_clean = (~df["is_suspicious"]).sum()
    return round(100.0 * n_clean / max(len(df), 1), 1)
