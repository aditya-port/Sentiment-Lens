# Sentiment Lens — Setup & Deployment Guide

## 🚀 Quick Start

### 1. Clone & Install
```bash
git clone <your-repo>
cd sentiment-lens-new
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your real values
```

**Required variables:**
| Variable | What it is |
|---|---|
| `SERPAPI_KEY` | SerpApi key for Google Maps reviews |
| `GROK_KEY` | Groq API key for AI chat |
| `DB_PATH` | Supabase PostgreSQL connection string |
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_KEY` | Supabase **service_role** key (not anon key) |
| `SITE_URL` | **Your production URL** — fixes email confirmation links |

### 3. Run the Supabase Schema
1. Open Supabase Dashboard → SQL Editor → New Query
2. Paste the contents of `supabase_schema.sql`
3. Click Run

### 4. Fix Email Confirmation Links (Critical)
1. In Supabase Dashboard → **Authentication → URL Configuration**
2. Set **Site URL** to your production URL (e.g. `https://yourapp.up.railway.app`)
3. Add the same URL to the **Redirect URLs** list
4. Set `SITE_URL=https://yourapp.up.railway.app` in your `.env`

### 5. Run Locally
```bash
python -m streamlit run app.py
```

---

## 🗄️ Database Architecture

### Security
- **Passwords are NEVER stored** in any table
- Supabase Auth (`auth.users`) handles passwords with bcrypt
- `public.users` stores only the email mirror + profile/tracking data
- The `SUPABASE_KEY` (service_role) in `.env` bypasses RLS — keep it secret

### Tables
| Table | Purpose |
|---|---|
| `users` | Profile + device/geo tracking |
| `places` | Google Maps places analyzed |
| `reviews` | Reviews with sentiment scores |
| `product_analyses` | Product paste sessions |
| `product_reviews` | Individual product reviews |
| `search_history` | Per-user analysis audit log |
| `user_search_queries` | **Granular search tracking** — what users searched, when, timing |
| `bookmarks` | Saved places per user |

### User Tracking
The new `user_search_queries` table records every search with:
- Who searched (user_id)
- What they typed (query_text)
- What type (place / product / compare)
- Result details
- Pipeline duration in milliseconds
- Success/error status

Query the admin view `user_activity_summary` to see per-user analytics.

---

## 🚂 Railway Deployment

Your `railway.toml` and `Procfile` are already configured.

```bash
railway login
railway up
```

Set all environment variables in Railway dashboard under **Variables**.
Make sure `SITE_URL` matches your Railway app URL exactly.

---

## 🔧 Troubleshooting

**Email confirmation link opens localhost?**
→ Set `SITE_URL` in `.env` and update Supabase Dashboard → Auth → URL Configuration

**"Email link is invalid or has expired"?**
→ The app now detects this and shows a "Resend confirmation email" button automatically

**DB connection errors?**
→ Check `DB_PATH` format: `postgresql://postgres.REF:PASSWORD@aws-REGION.pooler.supabase.com:6543/postgres`
→ Use the **Transaction Pooler** URL from Supabase (port 6543), not the direct connection (port 5432)

**"Monthly limit reached"?**
→ Update user's `plan` column to `'pro'` in the `users` table via Supabase Dashboard
