"""
src/analysis/authenticity.py
-----------------------------
Heuristic fake / inauthentic review detection.

Each review receives:
  is_suspicious    : bool  — True if suspicion_score >= SUSPICION_FLAG_THRESHOLD
  suspicion_score  : float — 0.0 … 1.0 (capped)
  suspicion_reasons: list  — human-readable list of triggered signals

Signals and weights are defined in src/config.py under SUSPICION_WEIGHTS.
Results are presented as "potentially suspicious" — they are heuristic
indicators, not proof of inauthenticity.
"""
from __future__ import annotations

import re
import string
import pandas as pd

from src.config import SUSPICION_WEIGHTS, SUSPICION_FLAG_THRESHOLD


# ─────────────────────────────────────────────────────────────────────────────
# Signal detectors
# ─────────────────────────────────────────────────────────────────────────────

def _score_review(text: str, rating) -> tuple[float, list[str]]:
    """
    Score one review text for suspicion signals.

    Returns (score: float 0-1, reasons: list[str])
    """
    score   = 0.0
    reasons = []

    # Normalise
    t       = str(text).strip() if pd.notna(text) else ""
    words   = t.split()
    n_words = len(words)
    n_chars = len(t)

    try:
        r = float(rating)
    except (TypeError, ValueError):
        r = None

    # ── Signal 1: very short text (< 4 words) ────────────────────────────────
    if 0 < n_words < 4:
        w = SUSPICION_WEIGHTS.get("very_short_text", 0.35)
        score += w
        reasons.append(f"very short ({n_words} words)")

    # ── Signal 2: single-word review ─────────────────────────────────────────
    if n_words == 1:
        w = SUSPICION_WEIGHTS.get("single_word", 0.30)
        score += w
        reasons.append("single word review")

    # ── Signal 3: short text with extreme 5-star or 1-star rating ────────────
    if n_words < 6 and r is not None and r == 5.0:
        w = SUSPICION_WEIGHTS.get("short_with_5star", 0.25)
        score += w
        reasons.append("very short 5-star review")

    # ── Signal 4: extreme rating (1 or 5) with no context ────────────────────
    if r is not None and r in (1.0, 5.0) and n_words < 8:
        context_words = {
            "because", "since", "but", "however", "although", "though",
            "except", "despite", "while", "whereas", "reason", "due",
        }
        if not any(w.lower() in context_words for w in words):
            w = SUSPICION_WEIGHTS.get("extreme_no_context", 0.20)
            score += w
            reasons.append("extreme rating with no explanation")

    # ── Signal 5: ALL CAPS (shouting) ────────────────────────────────────────
    letters = [c for c in t if c.isalpha()]
    if len(letters) >= 8:
        upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
        if upper_ratio >= 0.75:
            w = SUSPICION_WEIGHTS.get("all_caps", 0.15)
            score += w
            reasons.append("text is mostly uppercase")

    # ── Signal 6: excessive punctuation (!!!???...) ───────────────────────────
    if n_chars >= 5:
        punct_count = sum(1 for c in t if c in "!?.")
        if punct_count / n_chars >= 0.15:
            w = SUSPICION_WEIGHTS.get("excessive_punct", 0.10)
            score += w
            reasons.append("excessive punctuation")

    # Cap score at 1.0
    return min(score, 1.0), reasons


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def analyze_authenticity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add authenticity columns to a reviews DataFrame.

    Adds:
      is_suspicious    (bool)
      suspicion_score  (float 0–1)
      suspicion_reasons (list[str])

    Safe on empty DataFrames and missing columns.
    """
    if df.empty:
        return df

    result = df.copy()

    # Ensure columns exist before we write to them
    if "is_suspicious"     not in result.columns:
        result["is_suspicious"]    = False
    if "suspicion_score"   not in result.columns:
        result["suspicion_score"]  = 0.0
    if "suspicion_reasons" not in result.columns:
        result["suspicion_reasons"] = [[] for _ in range(len(result))]

    scores  = []
    flags   = []
    reasons = []

    text_col   = "review_text" if "review_text" in result.columns else None
    rating_col = "rating"      if "rating"      in result.columns else None

    for _, row in result.iterrows():
        text   = row[text_col]   if text_col   else ""
        rating = row[rating_col] if rating_col else None

        sc, rs = _score_review(text, rating)
        scores.append(round(sc, 4))
        flags.append(sc >= SUSPICION_FLAG_THRESHOLD)
        reasons.append(rs)

    result["suspicion_score"]   = scores
    result["is_suspicious"]     = flags
    result["suspicion_reasons"] = reasons

    return result


def get_trust_score(df: pd.DataFrame) -> float:
    """
    Return the trust score: % of reviews that are NOT suspicious.
    Returns 100.0 if no reviews or no authenticity data.
    """
    if df.empty or "is_suspicious" not in df.columns:
        return 100.0

    total = len(df)
    if total == 0:
        return 100.0

    suspicious = df["is_suspicious"].sum()
    return round(100.0 * (total - suspicious) / total, 1)
