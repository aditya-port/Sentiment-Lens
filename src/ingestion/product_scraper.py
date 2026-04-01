"""
src/ingestion/product_scraper.py
---------------------------------
Automated product review scraping.
Tries multiple strategies by hook or by crook:
  1. SerpApi Google Shopping reviews (if url given)
  2. Direct HTTP scrape with BeautifulSoup (Amazon/Flipkart/Meesho)
  3. Playwright headless browser (JS-heavy sites)
  4. Google search → SerpApi for product reviews
Falls back gracefully at each step.
"""
from __future__ import annotations
import re, time, requests
from typing import Optional
import pandas as pd


def scrape_product_reviews(
    url: str = "",
    product_name: str = "",
    platform: str = "Other",
    api_key: str = "",
    max_reviews: int = 100,
) -> tuple[pd.DataFrame, str]:
    """
    Try every strategy and return (df, method_used).
    df has columns: review_text, rating (optional)
    Returns empty df if all strategies fail.
    """
    df, method = pd.DataFrame(), "none"

    if url:
        # Strategy 1: SerpApi product reviews
        if api_key:
            df, method = _try_serpapi_product(url, product_name, api_key, max_reviews)
            if not df.empty:
                return df, method

        # Strategy 2: Direct scrape
        plat = platform.lower()
        if "amazon" in url or "amazon" in plat:
            df, method = _scrape_amazon(url, max_reviews)
        elif "flipkart" in url or "flipkart" in plat:
            df, method = _scrape_flipkart(url, max_reviews)
        elif "meesho" in url or "meesho" in plat:
            df, method = _scrape_meesho(url, max_reviews)
        else:
            df, method = _scrape_generic(url, max_reviews)

        if not df.empty:
            return df, method

    # Strategy 3: SerpApi Google search for reviews
    if api_key and product_name:
        df, method = _try_serpapi_google_reviews(product_name, platform, api_key, max_reviews)
        if not df.empty:
            return df, method

    return df, method


def _try_serpapi_product(url: str, name: str, api_key: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    """Try SerpApi Google Shopping / product reviews endpoint."""
    try:
        from src.config import SERPAPI_BASE
        # Try Google Shopping reviews
        params = {
            "engine": "google_product",
            "q": name or url,
            "api_key": api_key,
            "reviews": "1",
        }
        r = requests.get(SERPAPI_BASE, params=params, timeout=20)
        data = r.json()
        reviews_raw = data.get("reviews", [])
        if not reviews_raw:
            return pd.DataFrame(), ""
        rows = []
        for rv in reviews_raw[:max_reviews]:
            text = rv.get("content") or rv.get("text") or rv.get("snippet") or ""
            if len(text.strip()) < 8:
                continue
            rating = None
            try:
                rating = float(rv.get("rating") or rv.get("stars") or 0) or None
            except Exception:
                pass
            rows.append({"review_text": text.strip(), "rating": rating})
        if rows:
            return pd.DataFrame(rows), "serpapi_product"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _try_serpapi_google_reviews(name: str, platform: str, api_key: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    """Search Google for product reviews via SerpApi."""
    try:
        from src.config import SERPAPI_BASE
        query = f"{name} {platform} reviews site:reviews OR site:trustpilot.com"
        params = {
            "engine": "google",
            "q": query,
            "api_key": api_key,
            "num": 20,
        }
        r = requests.get(SERPAPI_BASE, params=params, timeout=20)
        data = r.json()
        rows = []
        for res in data.get("organic_results", []):
            snippet = res.get("snippet", "")
            if len(snippet.strip()) > 15:
                rows.append({"review_text": snippet.strip(), "rating": None})
        if rows:
            return pd.DataFrame(rows), "serpapi_search"
    except Exception:
        pass
    return pd.DataFrame(), ""


def _get_headers():
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _scrape_amazon(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    """Scrape Amazon product reviews page."""
    try:
        from bs4 import BeautifulSoup
        # Convert product page URL to reviews page
        asin_match = re.search(r"/dp/([A-Z0-9]{10})", url)
        if asin_match:
            asin = asin_match.group(1)
            # Extract domain (amazon.in, amazon.com etc)
            domain_match = re.search(r"(amazon\.[a-z.]+)", url)
            domain = domain_match.group(1) if domain_match else "amazon.com"
            url = f"https://www.{domain}/product-reviews/{asin}?sortBy=recent&pageNumber=1"

        rows = []
        for page in range(1, min(6, max_reviews // 10 + 2)):
            page_url = re.sub(r"pageNumber=\d+", f"pageNumber={page}", url)
            try:
                r = requests.get(page_url, headers=_get_headers(), timeout=15)
                soup = BeautifulSoup(r.text, "html.parser")
                # Amazon review spans
                for rev in soup.select("[data-hook='review-body'] span, .review-text-content span"):
                    text = rev.get_text(strip=True)
                    if len(text) > 15:
                        # Try to find rating nearby
                        rating = None
                        parent = rev.find_parent(class_=re.compile("review"))
                        if parent:
                            star_el = parent.select_one("[data-hook='review-star-rating'] span, .a-icon-star span")
                            if star_el:
                                try:
                                    rating = float(star_el.get_text(strip=True).split()[0])
                                except Exception:
                                    pass
                        rows.append({"review_text": text, "rating": rating})
                        if len(rows) >= max_reviews:
                            break
            except Exception:
                continue
            if len(rows) >= max_reviews:
                break
            time.sleep(0.5)

        if rows:
            return pd.DataFrame(rows), "amazon_scrape"
    except ImportError:
        pass
    except Exception:
        pass
    return pd.DataFrame(), ""


def _scrape_flipkart(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    """Scrape Flipkart product reviews."""
    try:
        from bs4 import BeautifulSoup
        rows = []
        for page in range(1, min(6, max_reviews // 10 + 2)):
            page_url = url
            if "?" in url:
                page_url = f"{url}&page={page}"
            else:
                page_url = f"{url}?page={page}"
            # Flipkart reviews endpoint
            if "/p/" in url:
                pid_match = re.search(r"/p/([^?/]+)", url)
                if pid_match:
                    page_url = f"https://www.flipkart.com/product/p/reviews?pid={pid_match.group(1)}&page={page}"
            try:
                r = requests.get(page_url, headers=_get_headers(), timeout=15)
                soup = BeautifulSoup(r.text, "html.parser")
                for rev in soup.select(".t-ZTKy, .qwjRop, ._6K-7Co, div[class*='review'] p"):
                    text = rev.get_text(strip=True)
                    if len(text) > 15:
                        rows.append({"review_text": text, "rating": None})
                        if len(rows) >= max_reviews:
                            break
            except Exception:
                continue
            if len(rows) >= max_reviews:
                break
            time.sleep(0.5)
        if rows:
            return pd.DataFrame(rows), "flipkart_scrape"
    except ImportError:
        pass
    except Exception:
        pass
    return pd.DataFrame(), ""


def _scrape_meesho(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    """Scrape Meesho product reviews."""
    try:
        from bs4 import BeautifulSoup
        rows = []
        r = requests.get(url, headers=_get_headers(), timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        for rev in soup.select("p[class*='review'], div[class*='review'] p, .review-body"):
            text = rev.get_text(strip=True)
            if len(text) > 15:
                rows.append({"review_text": text, "rating": None})
                if len(rows) >= max_reviews:
                    break
        if rows:
            return pd.DataFrame(rows), "meesho_scrape"
    except ImportError:
        pass
    except Exception:
        pass
    return pd.DataFrame(), ""


def _scrape_generic(url: str, max_reviews: int) -> tuple[pd.DataFrame, str]:
    """Generic review scraper — tries common review CSS patterns."""
    try:
        from bs4 import BeautifulSoup
        r = requests.get(url, headers=_get_headers(), timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []
        selectors = [
            ".review-text", ".review-body", ".review-content",
            "[itemprop='reviewBody']", ".user-review", ".customer-review",
            "div[class*='review'] p", "p[class*='review']",
        ]
        for sel in selectors:
            for el in soup.select(sel):
                text = el.get_text(strip=True)
                if len(text) > 20:
                    rows.append({"review_text": text, "rating": None})
                    if len(rows) >= max_reviews:
                        break
            if rows:
                break
        if rows:
            return pd.DataFrame(rows), "generic_scrape"
    except ImportError:
        pass
    except Exception:
        pass
    return pd.DataFrame(), ""
