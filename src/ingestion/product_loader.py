"""
src/ingestion/product_loader.py
--------------------------------
Parse pasted product reviews from any platform (Flipkart, Meesho, Amazon, etc.)

Input format options users can paste:
  1. One review per line
  2. Rating: 4/5 on first line, review on next
  3. Auto-detect rating if line starts with a number 1-5
"""
from __future__ import annotations
import re
import uuid
import pandas as pd
from datetime import datetime


def parse_pasted_reviews(raw_text: str, platform: str = "Other") -> pd.DataFrame:
    """
    Parse raw pasted review text into a normalised DataFrame.

    Accepts any of:
      - Plain text, one review per line
      - "4/5: Great product, loved it"
      - "★★★★ Great product"
      - Lines starting with rating digits

    Returns DataFrame with columns matching the reviews schema.
    """
    if not raw_text or not raw_text.strip():
        return pd.DataFrame()

    lines = [l.strip() for l in raw_text.strip().splitlines() if l.strip()]
    records = []

    i = 0
    while i < len(lines):
        line = lines[i]

        # Skip separator lines
        if set(line) <= set("-=_*~"):
            i += 1
            continue

        rating = None
        text   = line

        # Pattern: "4/5: review text" or "4.5/5: text"
        m = re.match(r'^(\d(?:\.\d)?)\s*/\s*5\s*[:\-]?\s*(.*)', line)
        if m:
            rating = float(m.group(1))
            text   = m.group(2).strip()

        # Pattern: "★★★★" or "4 stars" at start
        if not m:
            star_m = re.match(r'^(★+|☆+)\s*(.*)', line)
            if star_m:
                rating = float(len(re.findall(r'★', star_m.group(1))))
                text   = star_m.group(2).strip()

        # Pattern: line is just a number 1-5, next line is the review
        if not m and not star_m if 'm' in dir() or 'star_m' in dir() else True:
            num_m = re.match(r'^([1-5](?:\.\d)?)\s*$', line)
            if num_m and i + 1 < len(lines):
                rating = float(num_m.group(1))
                i += 1
                text = lines[i]

        if not text:
            i += 1
            continue

        records.append({
            "review_id":    f"paste_{uuid.uuid4().hex[:12]}",
            "place_id":     f"product_{platform.lower()}",
            "place_name":   platform,
            "author":       "Reviewer",
            "rating":       rating,
            "review_text":  text,
            "review_date":  None,
            "relative_date":"",
            "owner_response":"",
            "has_owner_response": False,
            "review_url":   "",
            "fetched_at":   datetime.utcnow().isoformat(),
            "word_count":   len(text.split()),
            "char_count":   len(text),
        })
        i += 1

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    return df


def validate_paste(raw_text: str) -> tuple[bool, str]:
    """
    Quick validation of pasted text before processing.
    Returns (valid: bool, message: str)
    """
    if not raw_text or not raw_text.strip():
        return False, "Paste some review text first."

    lines = [l.strip() for l in raw_text.strip().splitlines() if l.strip()]
    # Filter out separators
    real_lines = [l for l in lines if not (set(l) <= set("-=_*~"))]

    if len(real_lines) < 3:
        return False, "Need at least 3 reviews for meaningful analysis."

    # Estimate review count — rough heuristic
    avg_words = sum(len(l.split()) for l in real_lines) / max(len(real_lines), 1)
    if avg_words < 2:
        return False, "Lines look too short. Paste the full review text, not just ratings."

    return True, f"Ready to analyse approximately {len(real_lines)} reviews."