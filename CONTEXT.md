# SentimentLens Context (Working Memory)

Use this file as the first source of truth before scanning the full codebase.
Update it whenever architecture, core flows, or key decisions change.

## Project Snapshot
- App type: Streamlit SaaS app for review intelligence.
- Main entrypoint: `app.py`.
- Core modules:
  - `src/analysis/` sentiment/authenticity/themes
  - `src/ingestion/` serpapi + product ingestion
  - `src/storage/db.py` PostgreSQL persistence
  - `src/exports/pdf_report.py` report export

## Environment + Config
- Env keys in use:
  - `DB_PATH`, `SERPAPI_KEY`, `GROK_KEY`, `MAX_REVIEWS`, `SITE_URL`
- Config file: `src/config.py`
- Groq endpoint/model used in app:
  - `https://api.groq.com/openai/v1/chat/completions`
  - `llama-3.3-70b-versatile`

## Navigation
- Sidebar pages in `app.py`:
  - Analyze
  - Products
  - Bookmarks
  - Compare
  - History
  - Tools

## Product Flow (Current)
- UI section: `page_products()` in `app.py`.
- URL-only auto-fetch path:
  1. Collect URL + max reviews
  2. Auto-detect platform/product metadata via `infer_product_metadata(...)`
  3. Fetch reviews via `scrape_product_reviews(...)`
  4. Show preview
  5. Run `_run_product_analysis(...)`
- Manual paste path: removed from Products page UI.

## Product Scraper Notes (Current Integrated Version)
- File: `src/ingestion/product_scraper.py`
- Strategy order:
  1. Playwright for JS-heavy platforms (`Flipkart`, `Meesho`)
  2. SerpApi product endpoint fallback
  3. Direct HTTP/BS4 fallback
  4. SerpApi search snippets fallback
- Playwright dependency required: `playwright` (in `requirements.txt`)
- Meesho approach:
  - Uses isolated headless Playwright first (Railway/server-safe).
  - On local Windows only, optional isolated Edge guest CDP fallback (`SL_EDGE_GUEST_FALLBACK=1`).
  - Clicks `View all reviews`, parses body text pattern, clicks `VIEW MORE`.
- Flipkart approach:
  - Builds review URL from product page signals.
  - Parses text stream for rating/title/body/date.

## Product AI Judgement Feature
- Added in `app.py`:
  - `_groq_product_judgement(...)`
- Inputs:
  - Product name, platform, URL
  - Sentiment stats + keywords + aspect scores + sample reviews
- Output shown in Products results:
  - Verdict
  - Confidence
  - Summary
  - Major drawback keywords
  - Positive highlights
  - Buyer recommendations

## Persistence
- Product analysis save path:
  - `get_db().save_product_analysis(...)` in `src/storage/db.py`
- Product result state keys:
  - `prod_df`, `prod_name`, `prod_platform`, `prod_url`

## High-Impact Files (Touch Carefully)
- `app.py` (large, multipage, shared helper functions)
- `src/storage/db.py` (schema + persistence)
- `src/ingestion/product_scraper.py` (network/browser logic)

## Quick Validation Commands
- Syntax:
  - `python -m py_compile app.py src/ingestion/product_scraper.py`
- Product scraper module check:
  - Import `scrape_product_reviews(...)` and test with known Flipkart/Meesho URL.
- App run:
  - `python -m streamlit run app.py`

## Working Rules for Future Edits
- Prefer editing product features without touching analyze/maps pipeline unless needed.
- Keep scraper return schema stable:
  - `review_text`, `rating`, `date`, `source`
- If Meesho/Flipkart selectors break, adjust parser first, then UI flow only if needed.
- Preserve existing DB interfaces unless schema migration is explicitly requested.
