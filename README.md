# 🔍 Sentiment Lens

Universal Google Maps & Reviews Sentiment Analyzer — analyze any place, not just restaurants.

Hotels, shops, museums, hospitals, gyms, airports — if it's on Google Maps, Sentiment Lens analyzes it.

----

## Features

| # | Feature | Description |
|---|---------|-------------|
| 1 | **Universal place search** | Any Google Maps place type via SerpApi |
| 2 | **VADER sentiment scoring** | Compound score [-1, +1] per review, O(1) speed |
| 3 | **Sentiment velocity** | 30-day vs prior 30-day % change with trend signal |
| 4 | **K-Means topic clustering** | Auto-groups reviews into themes — no predefined categories |
| 5 | **Authenticity detection** | Heuristic suspicion scoring; flags potentially fake/paid reviews |
| 6 | **Aspect-based analysis** | 6 universal aspects: Quality, Service, Value, Location, Cleanliness, Wait Time |
| 7 | **Multi-place comparison** | Side-by-side radar + metrics for any analyzed places |
| 8 | **AI executive summary** | Claude-powered business insights (requires Anthropic key) |
| 9 | **Owner response analysis** | Response rate, engagement quality, timeline |
| 10 | **Analysis history** | Full audit log of every place analyzed |

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/yourusername/sentiment-lens.git
cd sentiment-lens
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Create Supabase database

1. Create a project at [supabase.com](https://supabase.com)
2. Go to **SQL Editor → New Query**
3. Paste and run the full contents of `supabase_schema.sql`
4. Verify: `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';`

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key        # Settings → API → service_role
SERPAPI_KEY=your-serpapi-key              # serpapi.com (100 free/month)
ANTHROPIC_API_KEY=your-anthropic-key      # optional — enables AI summary
```

### 4. Run

```bash
streamlit run app.py
```

Open `http://localhost:8501`

---

## Deploy to Railway

1. Push this repo to GitHub
2. New project in [Railway](https://railway.app) → **Deploy from GitHub repo**
3. Set environment variables in **Variables** tab:
   - `SUPABASE_URL`, `SUPABASE_KEY`, `SERPAPI_KEY`, `ANTHROPIC_API_KEY`
4. Railway auto-detects the `Procfile` and deploys

---

## Project Structure

```
sentiment-lens/
├── app.py                          # Streamlit app (entry point)
├── supabase_schema.sql             # Run this in Supabase SQL Editor
├── requirements.txt
├── Procfile                        # Railway deployment
├── railway.toml
├── .env.example
└── src/
    ├── config.py                   # All constants and thresholds
    ├── ingestion/
    │   └── serpapi_loader.py       # Universal Google Maps fetcher
    ├── analysis/
    │   ├── sentiment.py            # VADER + velocity
    │   ├── authenticity.py         # Fake review detection
    │   └── themes.py               # TF-IDF keywords + K-Means + ABSA
    ├── storage/
    │   └── supabase_client.py      # All Supabase operations
    └── visualization/
        └── charts.py               # 13 Plotly chart functions
```

---

## Technical Decisions

**Why VADER?** Purpose-built for short informal user text. Handles negations, intensifiers, and punctuation emphasis. No GPU, no fine-tuning, 100% explainable. ~75% accuracy on review corpora vs ~88% for fine-tuned DistilBERT — a good tradeoff for a portfolio project with a clear upgrade path.

**Why TF-IDF over KeyBERT/YAKE?** Zero extra dependencies beyond scikit-learn, deterministic, and the score (term frequency weighted by inverse document frequency) is self-explanatory in an interview. Applied per sentiment bucket to find what makes positive reviews positive, not just what reviews talk about.

**Why K-Means for clustering?** Interpretable, fast, scikit-learn native. Cluster labels are derived from top TF-IDF centroid terms — a recruiter can see exactly how topics are named. A production upgrade would use BERTopic for better semantic clustering.

**Why Supabase over SQLite?** The project runs on Railway (no persistent disk). Supabase provides a managed Postgres instance with a Python client that mirrors the supabase-js API. The schema uses a view (`place_summaries`) to avoid recomputing aggregates on every compare-page load.

**Suspicion detection heuristics** flag reviews that are statistically anomalous — very short text with extreme ratings, all-caps writing, excessive punctuation. These are indicative patterns, not definitive proof. Results are presented as "potentially suspicious" throughout the UI.

---
