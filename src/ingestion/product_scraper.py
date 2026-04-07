"""
src/ingestion/product_scraper.py
--------------------------------
Robust product review scraping for the Products page.

Priority:
1) Playwright flow for JS-heavy pages (Flipkart, Meesho)
2) SerpApi product/search fallback
3) Direct requests + BeautifulSoup fallback
"""
from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

import pandas as pd
import requests


def scrape_product_reviews(
    url: str = "",
    product_name: str = "",
    platform: str = "Other",
    api_key: str = "",
    max_reviews: int = 100,
) -> tuple[pd.DataFrame, str]:
    """
    Return (df, method_used). df columns: review_text, rating, date, source.
    """
    max_reviews = max(1, int(max_reviews or 100))
    meta = infer_product_metadata(url=url, platform_hint=platform)
    if product_name and product_name.strip():
        meta["product_name"] = product_name.strip()
    query_name = str(meta.get("product_name", "")).strip()
    target_platform = str(meta.get("platform_key", "other"))

    if url:
        # 1) Browser path first for JS-heavy pages.
        if target_platform in {"flipkart", "meesho"}:
            df, method = _scrape_with_playwright(url, target_platform, max_reviews)
            if not df.empty:
                return _attach_metadata(df, meta, url), method

        # 2) SerpApi product endpoint (if key available).
        if api_key:
            df, method = _try_serpapi_product(url, product_name, api_key, max_reviews)
            if not df.empty:
                return _attach_metadata(df, meta, url), method

        # 3) HTTP fallback.
        if target_platform == "amazon":
            df, method = _scrape_amazon(url, max_reviews)
        elif target_platform == "flipkart":
            df, method = _scrape_flipkart_http(url, max_reviews)
        elif target_platform == "meesho":
            df, method = _scrape_meesho_http(url, max_reviews)
        else:
            df, method = _scrape_generic(url, max_reviews)
        if not df.empty:
            return _attach_metadata(df, meta, url), method

    # 4) Last fallback: web snippets via SerpApi.
    if api_key and query_name and query_name.lower() != "product":
        df, method = _try_serpapi_google_reviews(query_name, meta.get("platform", platform), api_key, max_reviews)
        if not df.empty:
            return _attach_metadata(df, meta, url), method

    return pd.DataFrame(), "none"


def infer_product_metadata(url: str = "", platform_hint: str = "") -> dict:
    platform_key = _detect_platform(url, platform_hint)
    platform = _platform_label(platform_key)
    product_name = _infer_product_name(url, platform_key)
    return {
        "platform_key": platform_key,
        "platform": platform,
        "product_name": product_name or "Product",
    }


def _platform_label(platform_key: str) -> str:
    return {
        "flipkart": "Flipkart",
        "meesho": "Meesho",
        "amazon": "Amazon",
        "other": "Other",
    }.get((platform_key or "").lower(), "Other")


def _attach_metadata(df: pd.DataFrame, meta: dict, url: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df.attrs["detected_platform"] = meta.get("platform", "Other")
    df.attrs["platform_key"] = meta.get("platform_key", "other")
    df.attrs["detected_product_name"] = meta.get("product_name", "Product")
    df.attrs["source_url"] = (url or "").strip()
    return df


def _detect_platform(url: str, platform_hint: str) -> str:
    u = (url or "").lower()
    h = (platform_hint or "").lower()
    text = f"{u} {h}"
    if "flipkart" in text:
        return "flipkart"
    if "meesho" in text:
        return "meesho"
    if "amazon" in text:
        return "amazon"
    return "other"


def _infer_product_name(url: str, platform_key: str) -> str:
    # 1) Try a lightweight title pull from the page HTML.
    title = _fetch_title_from_page(url, platform_key)
    if title:
        return title

    # 2) URL slug fallback.
    try:
        parsed = urlparse(url or "")
        slug = ""
        for part in [p for p in parsed.path.split("/") if p]:
            low = part.lower()
            if low in {"p", "dp", "gp", "product-reviews"}:
                continue
            if re.fullmatch(r"itm[a-z0-9]+", low):
                continue
            if re.fullmatch(r"[a-z]{3,}\d{3,}", low):
                continue
            if len(part) > len(slug):
                slug = part
        slug = unquote(slug).replace("_", " ").replace("-", " ")
        slug = re.sub(r"\s+", " ", slug).strip()
        slug = re.sub(r"\b(?:pid|lid|marketplace|ref|tag)\b.*$", "", slug, flags=re.I).strip()
        return slug[:160] if slug else ""
    except Exception:
        return ""


def _fetch_title_from_page(url: str, platform_key: str) -> str:
    if not url:
        return ""
    try:
        from bs4 import BeautifulSoup
        r = requests.get(url, headers=_headers(), timeout=18)
        if r.status_code >= 400:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        candidates = []
        for sel, attr in [
            ("meta[property='og:title']", "content"),
            ("meta[name='twitter:title']", "content"),
            ("meta[name='title']", "content"),
        ]:
            el = soup.select_one(sel)
            if el and el.get(attr):
                candidates.append(str(el.get(attr)))
        if soup.title and soup.title.get_text(strip=True):
            candidates.append(soup.title.get_text(" ", strip=True))
        for cand in candidates:
            cleaned = _clean_product_title(cand, platform_key)
            if cleaned and len(cleaned) >= 5:
                return cleaned[:180]
    except Exception:
        return ""
    return ""


def _clean_product_title(title: str, platform_key: str) -> str:
    t = _normalize_spaces(title)
    if not t:
        return ""
    t = re.sub(r"\s*[\|\-–]\s*(flipkart|meesho|amazon(?:\.in)?).*$", "", t, flags=re.I)
    t = re.sub(r"^\s*(amazon(?:\.in)?\s*[:\-]\s*)", "", t, flags=re.I)
    t = re.sub(r"\s*buy .*?(online|at best price).*?$", "", t, flags=re.I)
    t = re.sub(r"\s{2,}", " ", t).strip(" -|:")
    if platform_key == "meesho":
        t = re.sub(r"\s*-\s*comfort and style.*$", "", t, flags=re.I)
    return t


def _headers() -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _clean_review_text(text: str) -> str:
    text = _normalize_spaces(text)
    text = re.sub(r"\s*\.\.\.\s*more\s*$", "", text, flags=re.I).strip()
    return text


def _is_review_like(text: str, min_chars: int = 8) -> bool:
    t = _clean_review_text(text)
    if len(t) < min_chars:
        return False
    if re.fullmatch(r"[\W\d\s]+", t):
        return False
    return True


def _records_to_df(rows: list[dict], source: str, max_reviews: int) -> pd.DataFrame:
    out = []
    seen = set()
    for r in rows:
        txt = _clean_review_text(r.get("review_text", ""))
        if not _is_review_like(txt):
            continue
        key = txt.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "review_text": txt,
                "rating": _safe_float(r.get("rating")),
                "date": _normalize_spaces(r.get("date", "")),
                "source": source,
            }
        )
        if len(out) >= max_reviews:
            break
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out)


def _safe_float(value) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        v = float(value)
        if 0.0 <= v <= 5.0:
            return v
        return None
    except Exception:
        return None


def _try_serpapi_product(url: str, name: str, api_key: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from src.config import SERPAPI_BASE
        params = {
            "engine": "google_product",
            "q": name or url,
            "reviews": "1",
            "api_key": api_key,
        }
        r = requests.get(SERPAPI_BASE, params=params, timeout=25)
        r.raise_for_status()
        data = r.json()
        rows = []
        for rv in data.get("reviews", [])[:max_reviews * 2]:
            rows.append(
                {
                    "review_text": rv.get("content") or rv.get("text") or rv.get("snippet") or "",
                    "rating": rv.get("rating") or rv.get("stars"),
                    "date": rv.get("date") or "",
                }
            )
        df = _records_to_df(rows, "serpapi_product", max_reviews)
        if not df.empty:
            return df, "serpapi_product"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _try_serpapi_google_reviews(name: str, platform: str, api_key: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from src.config import SERPAPI_BASE
        query = f"{name} {platform} reviews"
        params = {"engine": "google", "q": query, "num": 20, "api_key": api_key}
        r = requests.get(SERPAPI_BASE, params=params, timeout=25)
        r.raise_for_status()
        data = r.json()
        rows = []
        for item in data.get("organic_results", []):
            snippet = item.get("snippet", "")
            if _is_review_like(snippet, min_chars=15):
                rows.append({"review_text": snippet, "rating": None, "date": ""})
        df = _records_to_df(rows, "serpapi_search", max_reviews)
        if not df.empty:
            return df, "serpapi_search"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _scrape_amazon(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from bs4 import BeautifulSoup

        asin = None
        m = re.search(r"/dp/([A-Z0-9]{10})", url, re.I)
        if m:
            asin = m.group(1)
        domain_match = re.search(r"(amazon\.[a-z.]+)", url, re.I)
        domain = domain_match.group(1) if domain_match else "amazon.in"
        base = url
        if asin:
            base = f"https://www.{domain}/product-reviews/{asin}?sortBy=recent&pageNumber=1"

        rows = []
        for page in range(1, min(8, max_reviews // 8 + 3)):
            page_url = re.sub(r"pageNumber=\d+", f"pageNumber={page}", base)
            r = requests.get(page_url, headers=_headers(), timeout=20)
            if r.status_code >= 400:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            cards = soup.select("[data-hook='review']")
            for card in cards:
                body = card.select_one("[data-hook='review-body'] span, .review-text-content span")
                if not body:
                    continue
                text = body.get_text(" ", strip=True)
                rating = None
                rating_el = card.select_one("[data-hook='review-star-rating'] span, .a-icon-star span")
                if rating_el:
                    rm = re.search(r"([1-5](?:\.\d)?)", rating_el.get_text(" ", strip=True))
                    if rm:
                        rating = _safe_float(rm.group(1))
                date = ""
                date_el = card.select_one("[data-hook='review-date']")
                if date_el:
                    date = date_el.get_text(" ", strip=True)
                rows.append({"review_text": text, "rating": rating, "date": date})
                if len(rows) >= max_reviews * 2:
                    break
            if len(rows) >= max_reviews * 2:
                break
            time.sleep(0.4)
        df = _records_to_df(rows, "amazon_http", max_reviews)
        if not df.empty:
            return df, "amazon_http"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _scrape_flipkart_http(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from bs4 import BeautifulSoup
        r = requests.get(url, headers=_headers(), timeout=20)
        if r.status_code >= 400:
            return pd.DataFrame(), ""
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []
        for el in soup.select("div.t-ZTKy, div.qwjRop, div._11pzQk, div[class*='review'] p"):
            text = el.get_text(" ", strip=True)
            rows.append({"review_text": text, "rating": None, "date": ""})
        df = _records_to_df(rows, "flipkart_http", max_reviews)
        if not df.empty:
            return df, "flipkart_http"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _scrape_meesho_http(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from bs4 import BeautifulSoup
        r = requests.get(url, headers=_headers(), timeout=20)
        if r.status_code >= 400:
            return pd.DataFrame(), ""
        soup = BeautifulSoup(r.text, "html.parser")
        # Meesho often includes product review snippets in JSON-LD.
        rows = _extract_reviews_from_jsonld(soup)
        if not rows:
            for el in soup.select("div[class*='Comment'] span, p[class*='review'], div[class*='review'] p"):
                rows.append({"review_text": el.get_text(" ", strip=True), "rating": None, "date": ""})
        df = _records_to_df(rows, "meesho_http", max_reviews)
        if not df.empty:
            return df, "meesho_http"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _scrape_generic(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from bs4 import BeautifulSoup
        r = requests.get(url, headers=_headers(), timeout=20)
        if r.status_code >= 400:
            return pd.DataFrame(), ""
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []
        for sel in [
            ".review-text", ".review-body", ".review-content",
            "[itemprop='reviewBody']", ".user-review", ".customer-review",
            "div[class*='review'] p", "p[class*='review']",
        ]:
            for el in soup.select(sel):
                rows.append({"review_text": el.get_text(" ", strip=True), "rating": None, "date": ""})
            if rows:
                break
        df = _records_to_df(rows, "generic_http", max_reviews)
        if not df.empty:
            return df, "generic_http"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _extract_reviews_from_jsonld(soup) -> list[dict]:
    rows: list[dict] = []
    for script in soup.select("script[type='application/ld+json']"):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            for rv in item.get("review", []) or []:
                if not isinstance(rv, dict):
                    continue
                rr = rv.get("reviewRating") if isinstance(rv.get("reviewRating"), dict) else {}
                rows.append(
                    {
                        "review_text": rv.get("reviewBody", ""),
                        "rating": rr.get("ratingValue"),
                        "date": rv.get("datePublished", ""),
                    }
                )
    return rows


def _scrape_with_playwright(url: str, platform: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return pd.DataFrame(), ""

    pw = sync_playwright().start()
    browser = None
    context = None
    page = None
    blocked = False

    try:
        # Railway/server-safe path: isolated Chromium context only, no personal profile.
        headless = os.getenv("SL_PLAYWRIGHT_HEADLESS", "1").strip() != "0"
        browser = pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-IN",
            viewport={"width": 1366, "height": 768},
            extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"},
        )
        page = context.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "Object.defineProperty(navigator,'languages',{get:()=>['en-IN','en-US','en']});"
        )
        page.goto(url, timeout=60_000, wait_until="domcontentloaded")

        page.wait_for_timeout(3000)
        text = page.locator("body").inner_text(timeout=15000)
        blocked = _is_access_blocked(text)
        if not blocked:
            if platform == "meesho":
                rows = _extract_meesho_playwright(page, max_reviews)
                df = _records_to_df(rows, "meesho_playwright", max_reviews)
                if not df.empty:
                    return df, "meesho_playwright"
            elif platform == "flipkart":
                rows = _extract_flipkart_playwright(page, url, max_reviews)
                df = _records_to_df(rows, "flipkart_playwright", max_reviews)
                if not df.empty:
                    return df, "flipkart_playwright"
    except Exception:
        pass
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass

    # Local-only fallback for Meesho: isolated temporary Edge guest session.
    # This keeps personal profile/extensions out and is disabled on Railway/Linux.
    if (
        platform == "meesho"
        and os.name == "nt"
        and os.getenv("SL_EDGE_GUEST_FALLBACK", "1").strip() != "0"
    ):
        return _scrape_meesho_edge_guest(url, max_reviews)

    return pd.DataFrame(), ""


def _is_access_blocked(text: str) -> bool:
    t = (text or "").lower()
    return any(
        s in t for s in [
            "access denied",
            "you don't have permission",
            "request blocked",
            "temporarily blocked",
            "captcha",
            "forbidden",
        ]
    )


def _scrape_meesho_edge_guest(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return pd.DataFrame(), ""

    edge_exe = _find_edge_exe()
    if not edge_exe:
        return pd.DataFrame(), ""

    port = _find_free_port()
    user_dir = Path(tempfile.gettempdir()) / f"sentimentlens_edge_guest_{port}"
    edge_proc = None
    pw = None
    browser = None
    context = None
    page = None

    try:
        user_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            edge_exe,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_dir}",
            "--guest",
            "--disable-extensions",
            "--disable-component-extensions-with-background-pages",
            "--disable-sync",
            "--disable-default-apps",
            "--no-first-run",
            "--no-default-browser-check",
            "--inprivate",
            url,
        ]
        edge_proc = subprocess.Popen(cmd)
        pw = sync_playwright().start()

        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
                break
            except Exception:
                time.sleep(1.0)

        if not browser:
            return pd.DataFrame(), ""

        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = _pick_page_by_url(context.pages, url)
        if page is None:
            page = context.new_page()
            page.goto(url, timeout=60_000, wait_until="domcontentloaded")

        page.wait_for_timeout(3000)
        txt = page.locator("body").inner_text(timeout=15000)
        if _is_access_blocked(txt):
            return pd.DataFrame(), ""

        rows = _extract_meesho_playwright(page, max_reviews)
        df = _records_to_df(rows, "meesho_edge_guest", max_reviews)
        if not df.empty:
            return df, "meesho_edge_guest"
    except Exception:
        pass
    finally:
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            if pw:
                pw.stop()
        except Exception:
            pass
        if edge_proc:
            try:
                edge_proc.terminate()
                edge_proc.wait(timeout=8)
            except Exception:
                try:
                    edge_proc.kill()
                except Exception:
                    pass
        try:
            shutil.rmtree(user_dir, ignore_errors=True)
        except Exception:
            pass
    return pd.DataFrame(), ""


def _pick_page_by_url(pages, target_url: str):
    path = urlparse(target_url).path
    for p in pages:
        try:
            if path and path in p.url:
                return p
        except Exception:
            pass
    return pages[0] if pages else None


def _find_edge_exe() -> str:
    candidates = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    return ""


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _extract_meesho_playwright(page, max_reviews: int) -> list[dict]:
    rows: list[dict] = []
    # Initial reviews from JSON-LD/product page.
    try:
        html = page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        rows.extend(_extract_reviews_from_jsonld(soup))
    except Exception:
        pass

    _click_meesho_view_all(page)
    _wait_for_meesho_reviews_ready(page, timeout_ms=14000)

    for _ in range(8):
        parsed = _parse_meesho_text(_extract_meesho_review_text(page))
        if parsed:
            rows.extend(parsed)
        if len(_records_to_df(rows, "tmp", max_reviews)) >= max_reviews:
            break
        if not _click_if_visible(page, ["button:has-text('VIEW MORE')", "text='VIEW MORE'"]):
            _scroll_inner_review_container(page, 900)
        page.wait_for_timeout(1200)
    return rows


def _extract_flipkart_playwright(page, original_url: str, max_reviews: int) -> list[dict]:
    rows: list[dict] = []
    review_url = _build_flipkart_review_url(page, original_url)
    if review_url:
        try:
            page.goto(review_url, timeout=45_000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
        except Exception:
            pass

    _click_if_visible(page, ["text=Latest", "button:has-text('Latest')"])
    page.wait_for_timeout(800)

    for _ in range(12):
        body_text = page.locator("body").inner_text(timeout=15000)
        parsed = _parse_flipkart_text(body_text)
        if parsed:
            rows.extend(parsed)
        if len(_records_to_df(rows, "tmp", max_reviews)) >= max_reviews:
            break
        if not _click_if_visible(page, ["button:has-text('Load More')", "button:has-text('View More')"]):
            try:
                page.mouse.wheel(0, 1400)
            except Exception:
                pass
        page.wait_for_timeout(900)
    return rows


def _click_if_visible(page, selectors: list[str]) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=1200):
                loc.scroll_into_view_if_needed(timeout=1200)
                loc.click(timeout=2200)
                return True
        except Exception:
            continue
    return False


def _scroll_inner_review_container(page, delta: int) -> bool:
    try:
        return bool(
            page.evaluate(
                """(d) => {
                    const els = Array.from(document.querySelectorAll('div,section,article'));
                    for (const el of els) {
                        const cs = getComputedStyle(el);
                        const overflowY = cs.overflowY || '';
                        if (!(overflowY.includes('auto') || overflowY.includes('scroll'))) continue;
                        if ((el.scrollHeight || 0) <= (el.clientHeight || 0) + 120) continue;
                        const txt = (el.innerText || '');
                        if (!txt.includes('Posted on')) continue;
                        el.scrollTop = (el.scrollTop || 0) + d;
                        return true;
                    }
                    return false;
                }""",
                delta,
            )
        )
    except Exception:
        return False


def _extract_meesho_review_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=15000)
    except Exception:
        return ""


def _wait_for_meesho_reviews_ready(page, timeout_ms: int = 12000) -> bool:
    end = time.time() + timeout_ms / 1000.0
    best = 0
    while time.time() < end:
        try:
            text = _extract_meesho_review_text(page)
            cur = len(_parse_meesho_text(text))
            best = max(best, cur)
            if "Posted on" in text and ("VIEW MORE" in text or "Helpful" in text):
                return True
            if cur >= 4:
                return True
        except Exception:
            pass
        page.wait_for_timeout(500)
    return best >= 2


def _click_meesho_view_all(page) -> bool:
    # 1) precise section-aware click
    try:
        ok = bool(
            page.evaluate(
                """() => {
                    const all = Array.from(document.querySelectorAll('div,section,article'));
                    for (const box of all) {
                        const t = (box.innerText || '').toLowerCase();
                        if (!t.includes('product ratings') || !t.includes('view all reviews')) continue;
                        const btn = box.querySelector('button,span,p,a');
                        if (!btn) continue;
                        const bt = (btn.innerText || '').toLowerCase();
                        if (!bt.includes('view all reviews')) continue;
                        box.scrollIntoView({block: 'center', behavior: 'instant'});
                        btn.click();
                        return true;
                    }
                    return false;
                }"""
            )
        )
        if ok:
            page.wait_for_timeout(1200)
            return True
    except Exception:
        pass

    # 2) fallback selector clicks
    return _click_if_visible(
        page,
        [
            "button:has-text('View all reviews')",
            "button:has-text('VIEW ALL REVIEWS')",
            "text='View all reviews'",
            "text='VIEW ALL REVIEWS'",
        ],
    )


def _parse_meesho_text(text: str) -> list[dict]:
    lines = [_normalize_spaces(l) for l in str(text or "").splitlines() if _normalize_spaces(l)]

    def is_author(v: str) -> bool:
        t = v.lower()
        if len(v) > 60:
            return False
        if any(x in t for x in [
            "ratings", "reviews", "helpful", "view more", "view all", "add to cart", "buy now",
            "delivery", "product highlights", "people also viewed", "posted on", "followers", "products",
        ]):
            return False
        if re.search(r"[.!?₹]", v):
            return False
        return bool(re.search(r"[A-Za-z]", v))

    def is_rating(v: str) -> bool:
        return _safe_float(v) is not None

    out = []
    i = 0
    while i + 2 < len(lines):
        if not is_author(lines[i]) or not is_rating(lines[i + 1]) or not lines[i + 2].lower().startswith("posted on"):
            i += 1
            continue
        author = lines[i]
        rating = _safe_float(lines[i + 1])
        date = _normalize_spaces(re.sub(r"^posted on\s*", "", lines[i + 2], flags=re.I))
        j = i + 3
        body = []
        while j < len(lines):
            cur = lines[j]
            low = cur.lower()
            if low.startswith("helpful") or low == "view more":
                break
            if j + 2 < len(lines) and is_author(lines[j]) and is_rating(lines[j + 1]) and lines[j + 2].lower().startswith("posted on"):
                break
            body.append(cur)
            j += 1
        out.append({"review_text": " ".join(body), "rating": rating, "date": date, "author": author})
        i = max(j + 1, i + 1)
    return out


def _parse_flipkart_text(text: str) -> list[dict]:
    lines = [_normalize_spaces(l) for l in str(text or "").splitlines() if _normalize_spaces(l)]
    start = 0
    for idx, line in enumerate(lines):
        if line.lower().startswith("user reviews sorted by"):
            start = idx + 1
            break
    lines = lines[start:]

    def is_rating(v: str) -> bool:
        return _safe_float(v) is not None

    def is_reviewer(v: str) -> bool:
        tl = v.lower()
        if "flipkart customer" in tl or "certified buyer" in tl:
            return True
        if v.startswith(","):
            return True
        if len(v.split()) <= 6 and re.search(r"[A-Za-z]", v) and not re.search(r"[.!?₹]", v):
            return False
        return False

    out = []
    i = 0
    while i < len(lines):
        if not is_rating(lines[i]):
            i += 1
            continue
        rating = _safe_float(lines[i])
        j = i + 1
        while j < len(lines) and lines[j] in {"•", "·", "★"}:
            j += 1
        title = ""
        if j < len(lines) and len(lines[j]) <= 80 and not lines[j].lower().startswith("review for:") and not is_rating(lines[j]):
            title = lines[j]
            j += 1
        while j < len(lines) and lines[j].lower().startswith("review for:"):
            j += 1

        body = []
        date = ""
        while j < len(lines):
            cur = lines[j]
            low = cur.lower()
            if is_rating(cur):
                break
            if low.startswith("helpful for") or low == "verified purchase":
                j += 1
                continue
            if "ago" in low and re.search(r"\d+\s+(day|week|month|year)", low):
                date = cur
                j += 1
                continue
            if re.search(r"[A-Z][a-z]{2,9},\s*\d{4}", cur):
                date = cur
                j += 1
                continue
            if is_reviewer(cur):
                j += 1
                continue
            if re.fullmatch(r"[\(\)\d]+", cur):
                j += 1
                continue
            body.append(cur)
            j += 1
        out.append({"review_text": " ".join(body), "rating": rating, "date": date, "title": title})
        i = max(j, i + 1)
    return out


def _build_flipkart_review_url(page, original_url: str) -> str:
    try:
        u = page.url or original_url
        html = page.content()
        slug_match = re.search(r"flipkart\.com/([^/?]+)/", u, re.I)
        slug = slug_match.group(1) if slug_match else "product"

        item_id = ""
        pid = ""
        lid = ""

        for src in [u, html]:
            if not item_id:
                m = re.search(r"/(itm[a-zA-Z0-9]+)\?", src, re.I)
                if m:
                    item_id = m.group(1)
            if not pid:
                m = re.search(r"[?&]pid=([A-Z0-9]+)", src, re.I)
                if m:
                    pid = m.group(1)
            if not lid:
                m = re.search(r"[?&]lid=([A-Z0-9]+)", src, re.I)
                if m:
                    lid = m.group(1)

        if not item_id or not pid:
            return ""
        review_url = f"https://www.flipkart.com/{slug}/product-reviews/{item_id}?pid={pid}"
        if lid:
            review_url += f"&lid={lid}"
        review_url += "&marketplace=FLIPKART"

        parsed = urlparse(review_url)
        q = parse_qs(parsed.query)
        q.pop("page", None)
        q.pop("sortOrder", None)
        nq = urlencode({k: v[0] for k, v in q.items()}, doseq=False)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, nq, parsed.fragment))
    except Exception:
        return ""
