# 🔍 Sentiment Lens

**AI-powered review analytics for any Google Maps place — restaurants, hotels, hospitals, shops, and more.**

Paste a place name → get sentiment scores, topic clusters, fake review detection, trend analysis, and an AI chat assistant — all in seconds.

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app)

---

## ✨ Features

| Feature | What it does |
|---|---|
| 🔍 **Universal Place Search** | Analyze any Google Maps place via SerpApi |
| 🧠 **VADER Sentiment Scoring** | Compound score [-1, +1] per review |
| 📈 **Sentiment Velocity** | 30-day vs prior 30-day trend detection |
| 📦 **Topic Clustering** | K-Means auto-groups reviews into themes |
| 🛡️ **Fake Review Detection** | Heuristic suspicion scoring flags inauthentic reviews |
| 📊 **Aspect Analysis** | Quality · Service · Value · Location · Cleanliness · Wait Time |
| 🤖 **AI Chat** | Groq-powered assistant for natural-language insights |
| 🔖 **Bookmarks** | Save places for quick re-analysis |
| 📄 **PDF Export** | One-page summary report per analysis |
| 🆚 **Multi-Place Compare** | Side-by-side radar + metrics for any analyzed places |
| 📦 **Product Reviews** | Paste reviews from Amazon, Flipkart, Meesho — any platform |
| 👤 **User Accounts** | Email/password auth via Supabase (free + pro plans) |

---

## 🏗️ Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Streamlit |
| Sentiment | VADER (vaderSentiment) |
| Topics | TF-IDF + K-Means (scikit-learn) |
| Database | PostgreSQL via Supabase |
| Auth | Supabase Auth (bcrypt, email confirmation) |
| Reviews | SerpApi (Google Maps) |
| AI Chat | Groq API (llama-3.3-70b-versatile) |
| Deploy | Railway |

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- A [Supabase](https://supabase.com) account (free)
- A [SerpApi](https://serpapi.com) key (100 free searches/month)
- A [Groq](https://console.groq.com) key (free)

---

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/sentiment-lens.git
cd sentiment-lens
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set up Supabase database

1. Go to [supabase.com](https://supabase.com) → **New project**
2. Once created, go to **SQL Editor → New Query**
3. Paste the full contents of `supabase_schema.sql` and click **Run**
4. ✅ You should see all 8 tables created

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your real values:

```env
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_KEY=eyJhbGci...   # SERVICE_ROLE key, NOT anon key
DB_PATH=postgresql://postgres.your-ref:PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres
SERPAPI_KEY=your_serpapi_key
GROK_KEY=your_groq_key
MAX_REVIEWS=200
SITE_URL=http://localhost:8501
```

### 5. Fix Supabase email redirect

1. Supabase Dashboard → **Authentication → URL Configuration**
2. **Site URL** → set to your production URL
3. Update `SITE_URL=` in your `.env` to match

### 6. Run locally

```bash
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501)

---

## 🚂 Deploy to Railway

### Step 1 — Push to GitHub

```bash
git init
git add .
git commit -m "initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/sentiment-lens.git
git push -u origin main
```

> Make sure `.env` is in `.gitignore` — never push real keys to GitHub.

### Step 2 — Create Railway project

1. Go to [railway.app](https://railway.app) → **New Project**
2. Click **Deploy from GitHub repo**
3. Select your `sentiment-lens` repository
4. Railway auto-detects the `Procfile` — no extra config needed

### Step 3 — Add environment variables

In Railway → your service → **Variables** tab, add each of these:

| Variable | Where to get it |
|---|---|
| `SUPABASE_URL` | Supabase → Project Settings → API |
| `SUPABASE_KEY` | Supabase → Project Settings → API → `service_role` key |
| `DB_PATH` | Supabase → Project Settings → Database → URI (Transaction Pooler, port 6543) |
| `SERPAPI_KEY` | [serpapi.com](https://serpapi.com) |
| `GROK_KEY` | [console.groq.com](https://console.groq.com) |
| `MAX_REVIEWS` | `200` |
| `SITE_URL` | Your Railway app URL (after first deploy) |

### Step 4 — Get your Railway URL + update Supabase

1. After deploy: Railway → your service → **Settings → Domains** → copy the URL
2. Paste it into Supabase → **Authentication → URL Configuration → Site URL**
3. Also update `SITE_URL=` in Railway Variables and redeploy

---

## 📁 Project Structure

```
sentiment-lens/
├── app.py                        # Main Streamlit app (entry point)
├── supabase_schema.sql           # Run once in Supabase SQL Editor
├── requirements.txt
├── Procfile                      # Railway start command
├── railway.toml                  # Railway build config
├── .env.example                  # Copy to .env and fill in values
├── .gitignore
└── src/
    ├── config.py                 # Constants, thresholds, colours
    ├── auth/
    │   └── tracker.py            # IP geo, device detection, GPS
    ├── analysis/
    │   ├── sentiment.py          # VADER scoring + velocity
    │   ├── authenticity.py       # Fake review heuristics
    │   └── themes.py             # TF-IDF + K-Means topic clustering
    ├── ingestion/
    │   ├── serpapi_loader.py     # Google Maps review fetcher
    │   └── product_loader.py     # Manual product review parser
    ├── storage/
    │   └── db.py                 # All PostgreSQL operations
    ├── exports/
    │   └── pdf_report.py         # PDF report generation
    └── visualization/
        └── charts.py             # Plotly chart library
```

---

## 🗄️ Database

8 tables — run `supabase_schema.sql` to create them all at once.

| Table | Purpose |
|---|---|
| `users` | Profiles + device/geo tracking. **Passwords never stored here** |
| `places` | Google Maps places with aggregated stats |
| `reviews` | Individual reviews with VADER scores + topic labels |
| `product_analyses` | Product review paste sessions |
| `product_reviews` | Individual product reviews |
| `search_history` | Per-user audit log of every analysis |
| `user_search_queries` | Granular query log with timing + status |
| `bookmarks` | User-saved places |

---

## 🔐 Security

- Passwords are **never stored** in this codebase — Supabase Auth uses bcrypt in its internal schema
- Never commit `.env` to git — it contains your `service_role` key
- RLS is enabled on all tables

---

## 🛠️ Troubleshooting

| Problem | Fix |
|---|---|
| Email links open localhost | Set `SITE_URL` in env + Supabase → Auth → URL Configuration |
| "Email link expired" error | App shows a **Resend confirmation** button automatically |
| DB connection fails | Use Transaction Pooler URL (port **6543**), not direct (5432) |
| Monthly limit reached | Set `plan = 'pro'` for user in Supabase → Table Editor → `users` |
| Railway deploy fails | Check all 7 env variables are set in Railway → Variables |

---

## 📄 License

MIT
