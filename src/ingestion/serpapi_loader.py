"""
src/ingestion/serpapi_loader.py
--------------------------------
Google Maps review fetcher via SerpApi.

Key improvements over v1:
  - Multi-key result parsing  (local_results, place_results, knowledge_graph)
  - Four progressive search strategies with automatic fallback
  - Location-aware query building (city, state, country)
  - Returns top-N candidates so the UI can let the user pick the exact place
  - Detailed, actionable error messages at every failure point
  - Robust review normalisation that never crashes on missing fields

SerpApi credit cost:
  search_candidates() = 1 credit
  fetch_reviews()     = 1 + ceil(n / 10) credits
"""

import re
import uuid
import requests
from datetime import datetime
from typing import Optional
import pandas as pd

from src.config import SERPAPI_BASE, MAX_REVIEWS


# ── Exceptions ────────────────────────────────────────────────────────────────

class SerpApiError(Exception):
    pass

class SerpApiKeyMissingError(SerpApiError):
    pass

class SerpApiNoResultsError(SerpApiError):
    pass

class SerpApiQuotaError(SerpApiError):
    pass


# ── Query builder ─────────────────────────────────────────────────────────────

def build_query(name: str, city: str = "", state: str = "", country: str = "") -> str:
    """
    Combine user inputs into the most specific Google Maps query possible.
    Examples:
        name="Dawn 2 Dusk", city="Mumbai"          → "Dawn 2 Dusk Mumbai"
        name="Starbucks",   city="Delhi", state="Delhi" → "Starbucks Delhi Delhi India"
        name="Eiffel Tower", country="France"       → "Eiffel Tower France"
    """
    parts = [name.strip()]
    if city.strip():
        parts.append(city.strip())
    if state.strip() and state.strip().lower() != city.strip().lower():
        parts.append(state.strip())
    if country.strip():
        parts.append(country.strip())
    return " ".join(parts)


def _query_variants(name: str, city: str, state: str, country: str) -> list[str]:
    """
    Return a ranked list of query strings to try, from most to least specific.
    We try up to 4 strategies before giving up.
    """
    full     = build_query(name, city, state, country)
    no_state = build_query(name, city, "",    country)
    no_ctry  = build_query(name, city, state, "")
    name_loc = build_query(name, city, "",    "")
    name_only = name.strip()

    seen = []
    for q in [full, no_state, no_ctry, name_loc, name_only]:
        if q and q not in seen:
            seen.append(q)
    return seen


# ── Raw SerpApi call ──────────────────────────────────────────────────────────

def _call_serpapi(params: dict) -> dict:
    """Single SerpApi HTTP call with unified error handling."""
    try:
        resp = requests.get(SERPAPI_BASE, params=params, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        raise SerpApiError(
            "SerpApi request timed out after 30 seconds.\n"
            "Check your internet connection and try again."
        )
    except requests.exceptions.ConnectionError:
        raise SerpApiError(
            "Could not reach SerpApi. Check your internet connection."
        )
    except requests.exceptions.HTTPError as e:
        raise SerpApiError(f"SerpApi HTTP error {resp.status_code}: {e}")

    data = resp.json()

    if "error" in data:
        err = str(data["error"])
        if any(k in err.lower() for k in ("api_key", "invalid key", "unauthorized")):
            raise SerpApiKeyMissingError(
                f"Invalid or expired SerpApi key.\n"
                f"Check SERPAPI_KEY in your .env file. Error: {err}"
            )
        if any(k in err.lower() for k in ("quota", "limit", "credit", "plan")):
            raise SerpApiQuotaError(
                f"SerpApi quota exceeded.\n"
                f"Free tier: 100 searches/month. Upgrade at serpapi.com. Error: {err}"
            )
        raise SerpApiError(f"SerpApi error: {err}")

    return data


# ── Result extraction ─────────────────────────────────────────────────────────

def _extract_candidates(data: dict, query: str) -> list[dict]:
    """
    Pull place candidates from every possible result key SerpApi may return.
    SerpApi is inconsistent — sometimes results land in:
      local_results, place_results, knowledge_graph, local_map → results
    We check all of them and merge.
    """
    candidates = []

    # 1. local_results — most common for business searches
    for r in data.get("local_results", []):
        did = r.get("data_id") or r.get("place_id")
        if did:
            candidates.append({
                "data_id":  did,
                "name":     r.get("title") or r.get("name") or query,
                "category": r.get("type") or r.get("category") or "General",
                "address":  r.get("address", ""),
                "rating":   r.get("rating"),
                "reviews":  r.get("reviews"),
                "source":   "local_results",
            })

    # 2. place_results — returned for direct place queries
    pr = data.get("place_results", {})
    if pr:
        did = pr.get("data_id") or pr.get("place_id")
        if did:
            candidates.append({
                "data_id":  did,
                "name":     pr.get("title") or pr.get("name") or query,
                "category": pr.get("type") or pr.get("category") or "General",
                "address":  pr.get("address", ""),
                "rating":   pr.get("rating"),
                "reviews":  pr.get("reviews"),
                "source":   "place_results",
            })

    # 3. knowledge_graph — returned for famous landmarks / well-known places
    kg = data.get("knowledge_graph", {})
    if kg:
        did = kg.get("data_id") or kg.get("place_id") or kg.get("kgmid")
        if did:
            candidates.append({
                "data_id":  did,
                "name":     kg.get("title") or kg.get("name") or query,
                "category": kg.get("type") or "General",
                "address":  kg.get("address", ""),
                "rating":   kg.get("rating"),
                "reviews":  kg.get("reviews"),
                "source":   "knowledge_graph",
            })

    # 4. local_map.results — alternative key used in some SerpApi versions
    for r in (data.get("local_map") or {}).get("results", []):
        did = r.get("data_id") or r.get("place_id")
        if did:
            candidates.append({
                "data_id":  did,
                "name":     r.get("title") or r.get("name") or query,
                "category": r.get("type") or "General",
                "address":  r.get("address", ""),
                "rating":   r.get("rating"),
                "reviews":  r.get("reviews"),
                "source":   "local_map",
            })

    # Deduplicate by data_id (keep first occurrence = highest ranked)
    seen = set()
    unique = []
    for c in candidates:
        if c["data_id"] not in seen:
            seen.add(c["data_id"])
            unique.append(c)

    return unique


# ── Public: search for candidates ─────────────────────────────────────────────

def search_candidates(
    name: str,
    city: str = "",
    state: str = "",
    country: str = "",
    api_key: str = "",
    max_candidates: int = 5,
) -> list[dict]:
    """
    Search Google Maps and return up to max_candidates matching places.

    Uses progressive query strategies — if the full query yields nothing,
    it automatically retries with progressively simpler variants.

    Args:
        name:           Place name as typed by the user
        city:           Optional city
        state:          Optional state / province
        country:        Optional country
        api_key:        SerpApi key
        max_candidates: Max results to return (shown in UI picker)

    Returns:
        List of candidate dicts, each with:
            data_id, name, category, address, rating, reviews, source, matched_query

    Raises:
        SerpApiKeyMissingError  — bad / missing key
        SerpApiQuotaError       — monthly limit hit
        SerpApiNoResultsError   — nothing found after all fallback strategies
        SerpApiError            — other API errors
    """
    if not api_key:
        raise SerpApiKeyMissingError(
            "SERPAPI_KEY is not set.\n"
            "Add it to your .env file. Get a free key at https://serpapi.com"
        )

    variants = _query_variants(name, city, state, country)
    last_error = None
    all_candidates = []

    for query in variants:
        try:
            params = {
                "engine":  "google_maps",
                "q":       query,
                "api_key": api_key,
                "type":    "search",
                "hl":      "en",
                "gl":      _country_code(country),   # geo-target by country
            }
            data = _call_serpapi(params)
            candidates = _extract_candidates(data, query)

            if candidates:
                # Tag which query variant found each result
                for c in candidates:
                    c["matched_query"] = query
                all_candidates.extend(candidates)

                # Deduplicate across variants
                seen = set()
                unique = []
                for c in all_candidates:
                    if c["data_id"] not in seen:
                        seen.add(c["data_id"])
                        unique.append(c)
                all_candidates = unique

                if len(all_candidates) >= max_candidates:
                    break   # Have enough — stop spending credits

        except (SerpApiKeyMissingError, SerpApiQuotaError):
            raise   # Don't retry on auth/quota errors
        except SerpApiError as e:
            last_error = e
            continue  # Try next variant

    if not all_candidates:
        # Build a helpful error with what was tried
        tried = " → ".join(f'"{v}"' for v in variants)
        hint = _build_no_results_hint(name, city, state, country)
        raise SerpApiNoResultsError(
            f"No Google Maps results found after trying:\n{tried}\n\n{hint}"
        )

    return all_candidates[:max_candidates]


def _country_code(country: str) -> str:
    """Map common country names to ISO-3166-1 alpha-2 codes for SerpApi `gl` param."""
    mapping = {
        "india": "in", "united states": "us", "usa": "us", "us": "us",
        "united kingdom": "gb", "uk": "gb", "australia": "au", "canada": "ca",
        "germany": "de", "france": "fr", "japan": "jp", "china": "cn",
        "singapore": "sg", "uae": "ae", "dubai": "ae",
    }
    return mapping.get(country.strip().lower(), "")


def _build_no_results_hint(name, city, state, country) -> str:
    tips = [
        "Make sure the place name is spelled as it appears on Google Maps.",
        "Try the most common short name (e.g. 'Dawn 2 Dusk' not 'Dawn to Dusk Cafe').",
    ]
    if not city and not country:
        tips.append("Add a city or country — many place names are not unique globally.")
    if city:
        tips.append(f"Check that '{city}' is the correct city. Try the full city name.")
    return "\n".join(f"• {t}" for t in tips)


# ── Public: fetch reviews for a chosen candidate ──────────────────────────────

def fetch_reviews_for_place(
    candidate: dict,
    api_key: str,
    max_reviews: int = MAX_REVIEWS,
) -> tuple[pd.DataFrame, dict]:
    """
    Fetch reviews for a place already identified by search_candidates().

    Args:
        candidate:   A dict from search_candidates() — must have 'data_id'
        api_key:     SerpApi key
        max_reviews: Review count cap

    Returns:
        (reviews_df, enriched_meta)
    """
    data_id    = candidate["data_id"]
    place_name = candidate["name"]
    place_url  = f"https://www.google.com/maps/search/?api=1&query={requests.utils.quote(place_name)}"

    all_reviews = []
    next_token  = None
    page_num    = 0

    while len(all_reviews) < max_reviews:
        page_num += 1
        try:
            page = _fetch_review_page(data_id, api_key, next_token)
        except SerpApiError as e:
            if all_reviews:
                # Got some reviews already — partial result is acceptable
                break
            raise SerpApiError(
                f"Failed to fetch reviews for '{place_name}' (page {page_num}).\n"
                f"Details: {e}"
            )

        raw_list = page.get("reviews", [])

        # SerpApi sometimes returns reviews as a dict — handle both
        if isinstance(raw_list, dict):
            raw_list = list(raw_list.values())

        for raw in raw_list:
            all_reviews.append(_normalise_review(raw, data_id, place_name, place_url))
            if len(all_reviews) >= max_reviews:
                break

        next_token = (page.get("serpapi_pagination") or {}).get("next_page_token")
        if not next_token:
            break

    if not all_reviews:
        raise SerpApiNoResultsError(
            f"'{place_name}' was found on Google Maps but returned no review text.\n"
            "This can happen if all reviews are in a non-English language or\n"
            "if the place has very few reviews visible via the API."
        )

    df = pd.DataFrame(all_reviews)
    meta = {**candidate, "place_id": data_id}
    return df, meta


# ── Convenience wrapper (used when candidate is already selected) ──────────────

def fetch_reviews(
    query: str,
    api_key: str,
    max_reviews: int = MAX_REVIEWS,
    city: str = "",
    state: str = "",
    country: str = "",
) -> tuple[pd.DataFrame, dict]:
    """
    One-shot fetch: search → take top result → fetch reviews.
    Used internally when the user skips the candidate-picker step.
    """
    candidates = search_candidates(
        name=query, city=city, state=state, country=country,
        api_key=api_key, max_candidates=1,
    )
    return fetch_reviews_for_place(candidates[0], api_key, max_reviews)


# ── Review page fetch ─────────────────────────────────────────────────────────

def _fetch_review_page(data_id: str, api_key: str, next_token: Optional[str] = None) -> dict:
    params = {
        "engine":  "google_maps_reviews",
        "data_id": data_id,
        "api_key": api_key,
        "hl":      "en",
        "sort_by": "newestFirst",
    }
    if next_token:
        params["next_page_token"] = next_token
    return _call_serpapi(params)


# ── Review normalisation ──────────────────────────────────────────────────────

def _normalise_review(raw: dict, place_id: str, place_name: str, place_url: str) -> dict:
    """Convert raw SerpApi review → standard schema. Never raises."""
    # Text: try multiple field names SerpApi uses inconsistently
    text = (
        raw.get("snippet")
        or raw.get("text")
        or raw.get("review_text")
        or raw.get("body")
        or ""
    )
    text = str(text).strip()

    # Date
    iso_date = raw.get("iso_date") or raw.get("date_utc") or ""
    review_date = None
    if iso_date:
        try:
            review_date = datetime.fromisoformat(
                iso_date.replace("Z", "+00:00")
            ).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            review_date = None

    # Owner response — nested in 'response' or 'owner_response'
    owner_blob = raw.get("response") or raw.get("owner_response") or {}
    if isinstance(owner_blob, str):
        owner_text = owner_blob
    else:
        owner_text = owner_blob.get("snippet") or owner_blob.get("text") or ""

    # Rating — may be int, float, or string like "4.0"
    rating = raw.get("rating")
    if rating is not None:
        try:
            rating = float(rating)
        except (ValueError, TypeError):
            rating = None

    return {
        "review_id":          raw.get("review_id") or f"sr_{uuid.uuid4().hex[:14]}",
        "place_id":           place_id,
        "place_name":         place_name,
        "author":             (raw.get("user") or {}).get("name") or raw.get("author") or "Anonymous",
        "rating":             rating,
        "review_text":        text,
        "review_date":        review_date,
        "relative_date":      raw.get("date") or raw.get("relative_date") or "",
        "owner_response":     str(owner_text).strip(),
        "has_owner_response": bool(str(owner_text).strip()),
        "review_url":         place_url,
        "fetched_at":         datetime.utcnow().isoformat(),
        "word_count":         len(text.split()) if text else 0,
        "char_count":         len(text) if text else 0,
    }
