"""
src/ingestion/product_loader.py
--------------------------------
Parse pasted product reviews from the manual-paste UI.

Supports multiple formats:
  • Plain text  — one review per line
  • Rated       — "4/5: Great product!" or "4.5/5 Amazing value"
  • Star prefix — "★★★ Decent but ok" or "3★ Not bad"
  • Emoji       — "⭐⭐⭐⭐ Love it!"
  • Shorthand   — "5: Perfect" or "2 - Very disappointing"
"""
from __future__ import annotations

import re
import pandas as pd


# ── Rating pattern matchers ────────────────────────────────────────────────────

_PATTERNS = [
    # 4/5: text  or  4.5/5: text
    re.compile(r"^(\d(?:\.\d)?)\s*/\s*5\s*[:\-–]?\s*(.+)$", re.IGNORECASE),
    # 4/5 text  (no colon)
    re.compile(r"^(\d(?:\.\d)?)/5\s+(.+)$", re.IGNORECASE),
    # ★★★ text  or  ⭐⭐⭐⭐ text
    re.compile(r"^([★⭐]{1,5})\s+(.+)$"),
    # 3★ text  or  3⭐ text
    re.compile(r"^(\d)[★⭐]\s+(.+)$"),
    # 3: text  or  3 - text  (digit then separator then text)
    re.compile(r"^([1-5])\s*[:\-–]\s*(.+)$"),
]


def _parse_line(line: str) -> tuple[str, float | None]:
    """
    Parse a single review line.
    Returns (review_text, rating | None).
    """
    line = line.strip()
    if not line:
        return "", None

    for pat in _PATTERNS:
        m = pat.match(line)
        if m:
            raw_rating = m.group(1)
            text       = m.group(2).strip()
            # Convert star string to numeric
            if raw_rating in ("★", "⭐"):
                rating = 1.0
            elif all(c in "★⭐" for c in raw_rating):
                rating = float(len(raw_rating))
            else:
                try:
                    rating = float(raw_rating)
                    if not 1 <= rating <= 5:
                        rating = None
                except ValueError:
                    rating = None
            return text, rating

    return line, None


# ── Public API ─────────────────────────────────────────────────────────────────

def validate_paste(raw_text: str) -> tuple[bool, str]:
    """
    Validate pasted review text before parsing.

    Returns (valid: bool, message: str).
    """
    if not raw_text or not raw_text.strip():
        return False, "Please paste some reviews first."

    lines = [l.strip() for l in raw_text.strip().splitlines() if l.strip()]

    if len(lines) < 1:
        return False, "No reviews detected. Paste at least one review per line."

    if len(lines) > 5000:
        return False, f"Too many lines ({len(lines):,}). Limit is 5,000 reviews per paste."

    # Check that at least some lines look like real text (not just numbers)
    real_lines = [l for l in lines if len(l.split()) >= 2]
    if not real_lines:
        return False, "No readable review text found. Each line should contain at least 2 words."

    return True, f"Found {len(lines)} review line(s)."


def parse_pasted_reviews(raw_text: str, platform: str = "Other") -> pd.DataFrame:
    """
    Parse pasted review text into a DataFrame ready for sentiment analysis.

    Returns a DataFrame with columns:
        review_text, rating, platform, word_count, char_count
    """
    if not raw_text or not raw_text.strip():
        return pd.DataFrame()

    rows = []
    for line in raw_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        text, rating = _parse_line(line)
        text = text.strip()

        # Skip empty or too-short lines after parsing
        if len(text) < 3:
            continue

        rows.append({
            "review_text": text,
            "rating":      rating,
            "platform":    platform,
            "word_count":  len(text.split()),
            "char_count":  len(text),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Add a synthetic review_id so the pipeline doesn't crash on missing IDs
    df.insert(0, "review_id", [f"paste_{i}" for i in range(len(df))])

    return df
