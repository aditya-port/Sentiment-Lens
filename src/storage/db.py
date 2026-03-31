"""
src/storage/db.py — Sentiment Lens Database Layer v2
Direct PostgreSQL via psycopg2 + connection pool.

Tables:
  users                — Profile + usage tracking (NO passwords stored)
  places               — Google Maps places
  reviews              — Reviews with sentiment enrichment
  product_analyses     — Product review paste sessions
  product_reviews      — Individual product reviews
  search_history       — Per-user analysis audit log
  user_search_queries  — Granular search tracking (what users search)
  bookmarks            — User saved places

Security:
  Passwords are NEVER stored here.
  Supabase Auth (auth.users) handles authentication with bcrypt.
  This table stores only the email mirror + profile data.
"""
from __future__ import annotations

import os
import sys
import math
import time
from typing import Optional
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from src.config import DB_PATH

FREE_MONTHLY_LIMIT = 10  # analyses per month on free tier


# ── Exceptions ────────────────────────────────────────────────────────────────
class DBConfigError(Exception):
    pass

class DBQueryError(Exception):
    pass


# ── Type sanitiser ────────────────────────────────────────────────────────────
def _py(val, default=None):
    """Convert numpy/pandas types to Python-native for psycopg2."""
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    try:
        import numpy as np
        if isinstance(val, np.integer):  return int(val)
        if isinstance(val, np.floating):
            v = float(val)
            return default if math.isnan(v) else v
        if isinstance(val, np.bool_):    return bool(val)
        if isinstance(val, np.ndarray):  return val.tolist()
    except ImportError:
        pass
    if isinstance(val, float) and math.isnan(val):
        return default
    return val


# ── Decimal→float cast ────────────────────────────────────────────────────────
_NUMERIC_COLS = {
    "overall_rating", "avg_sentiment", "pct_positive", "pct_negative",
    "trust_score", "rating", "sentiment_score", "sentiment_pos",
    "sentiment_neg", "sentiment_neu", "suspicion_score", "avg_rating",
    "latitude", "longitude", "gps_latitude", "gps_longitude", "gps_accuracy",
    "total_reviews", "positive_count", "negative_count", "suspicious_count",
    "reviews_fetched", "word_count", "char_count", "reviews_count",
    "total_searches", "screen_width", "screen_height", "analyses_this_month",
    "duration_ms", "result_count",
}

def _cast_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    import decimal
    for col in df.columns:
        if col in _NUMERIC_COLS:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            except Exception:
                pass
        elif df[col].dtype == object:
            try:
                first = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first, decimal.Decimal):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            except Exception:
                pass
    return df


# ── Connection pool (ThreadedConnectionPool is safe for Streamlit) ────────────
_pool: Optional[ThreadedConnectionPool] = None

def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        if not DB_PATH:
            raise DBConfigError(
                "DB_PATH is not set.\n"
                "Format: postgresql://user:password@host:port/dbname"
            )
        try:
            _pool = ThreadedConnectionPool(minconn=1, maxconn=10, dsn=DB_PATH)
        except Exception as e:
            raise DBConfigError(f"PostgreSQL connection failed: {e}")
    return _pool

def _conn():
    return _get_pool().getconn()

def _rel(c):
    try:
        _get_pool().putconn(c)
    except Exception:
        pass


# ── DatabaseManager ───────────────────────────────────────────────────────────
class DatabaseManager:

    # ── Schema ────────────────────────────────────────────────────────────────
    def ensure_schema(self):
        """Create all tables and indexes if they don't exist."""
        stmts = [
            'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"',

            # ── users ──────────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS users (
                id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email               TEXT UNIQUE,
                name                TEXT,
                phone               TEXT,
                plan                TEXT DEFAULT 'free',
                analyses_this_month INTEGER DEFAULT 0,
                last_reset_month    TEXT DEFAULT '',
                total_searches      INTEGER DEFAULT 0,
                ip_address      TEXT,
                country         TEXT,
                country_code    TEXT,
                region          TEXT,
                region_name     TEXT,
                city            TEXT,
                zip_code        TEXT,
                latitude        NUMERIC(10,6),
                longitude       NUMERIC(10,6),
                timezone        TEXT,
                isp             TEXT,
                org             TEXT,
                user_agent      TEXT,
                browser_name    TEXT,
                browser_version TEXT,
                os_name         TEXT,
                os_version      TEXT,
                device_type     TEXT DEFAULT 'desktop',
                is_mobile       BOOLEAN DEFAULT FALSE,
                is_bot          BOOLEAN DEFAULT FALSE,
                screen_width    INTEGER,
                screen_height   INTEGER,
                language        TEXT,
                languages       TEXT,
                platform        TEXT,
                timezone_js     TEXT,
                gps_latitude    NUMERIC(10,6),
                gps_longitude   NUMERIC(10,6),
                gps_accuracy    NUMERIC(10,2),
                gps_granted     BOOLEAN DEFAULT FALSE,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                last_seen       TIMESTAMPTZ DEFAULT NOW()
            )""",

            # ── places ────────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS places (
                id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                place_id                    TEXT UNIQUE NOT NULL,
                name                        TEXT NOT NULL,
                category                    TEXT DEFAULT 'General',
                address                     TEXT,
                city                        TEXT,
                country                     TEXT,
                overall_rating              NUMERIC(3,2),
                total_reviews_on_platform   INTEGER,
                reviews_fetched             INTEGER DEFAULT 0,
                avg_sentiment               NUMERIC(6,4),
                pct_positive                NUMERIC(5,2),
                pct_negative                NUMERIC(5,2),
                trust_score                 NUMERIC(5,2),
                last_fetched_at             TIMESTAMPTZ,
                created_at                  TIMESTAMPTZ DEFAULT NOW()
            )""",

            # ── reviews ───────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS reviews (
                id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                review_id           TEXT UNIQUE NOT NULL,
                place_id            TEXT NOT NULL REFERENCES places(place_id) ON DELETE CASCADE,
                place_name          TEXT NOT NULL,
                author              TEXT,
                rating              NUMERIC(2,1),
                review_text         TEXT,
                review_date         DATE,
                relative_date       TEXT,
                owner_response      TEXT,
                has_owner_response  BOOLEAN DEFAULT FALSE,
                review_url          TEXT,
                fetched_at          TIMESTAMPTZ DEFAULT NOW(),
                sentiment_label     TEXT,
                sentiment_score     NUMERIC(6,4),
                sentiment_pos       NUMERIC(6,4),
                sentiment_neg       NUMERIC(6,4),
                sentiment_neu       NUMERIC(6,4),
                word_count          INTEGER,
                char_count          INTEGER,
                is_suspicious       BOOLEAN DEFAULT FALSE,
                suspicion_score     NUMERIC(4,2) DEFAULT 0.0,
                suspicion_reasons   TEXT[] DEFAULT '{}',
                topic_cluster       INTEGER,
                topic_label         TEXT
            )""",

            # ── product_analyses ──────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS product_analyses (
                id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id         UUID REFERENCES users(id) ON DELETE SET NULL,
                product_name    TEXT NOT NULL,
                source          TEXT DEFAULT 'manual',
                platform        TEXT DEFAULT 'Other',
                product_url     TEXT,
                total_reviews   INTEGER DEFAULT 0,
                avg_sentiment   NUMERIC(6,4),
                pct_positive    NUMERIC(5,2),
                pct_negative    NUMERIC(5,2),
                avg_rating      NUMERIC(3,2),
                trust_score     NUMERIC(5,2),
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )""",

            # ── product_reviews ───────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS product_reviews (
                id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                analysis_id     UUID REFERENCES product_analyses(id) ON DELETE CASCADE,
                review_text     TEXT NOT NULL,
                rating          NUMERIC(2,1),
                sentiment_label TEXT,
                sentiment_score NUMERIC(6,4),
                sentiment_pos   NUMERIC(6,4),
                sentiment_neg   NUMERIC(6,4),
                sentiment_neu   NUMERIC(6,4),
                word_count      INTEGER,
                is_suspicious   BOOLEAN DEFAULT FALSE,
                suspicion_score NUMERIC(4,2) DEFAULT 0.0,
                topic_cluster   INTEGER,
                topic_label     TEXT,
                row_order       INTEGER
            )""",

            # ── search_history ────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS search_history (
                id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id         UUID REFERENCES users(id) ON DELETE SET NULL,
                place_name      TEXT NOT NULL,
                place_id        TEXT,
                category        TEXT,
                reviews_count   INTEGER,
                avg_sentiment   NUMERIC(6,4),
                avg_rating      NUMERIC(3,2),
                pct_positive    NUMERIC(5,2),
                trust_score     NUMERIC(5,2),
                searched_at     TIMESTAMPTZ DEFAULT NOW()
            )""",

            # ── user_search_queries (granular tracking) ───────────────────
            """CREATE TABLE IF NOT EXISTS user_search_queries (
                id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id         UUID REFERENCES users(id) ON DELETE SET NULL,
                query_text      TEXT NOT NULL,
                query_type      TEXT DEFAULT 'place',
                result_place_id TEXT,
                result_name     TEXT,
                result_count    INTEGER DEFAULT 0,
                duration_ms     INTEGER,
                session_ip      TEXT,
                status          TEXT DEFAULT 'success',
                error_message   TEXT,
                searched_at     TIMESTAMPTZ DEFAULT NOW()
            )""",

            # ── bookmarks ─────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS bookmarks (
                id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                place_id    TEXT NOT NULL,
                place_name  TEXT,
                note        TEXT,
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (user_id, place_id)
            )""",

            # ── Indexes ───────────────────────────────────────────────────
            "CREATE INDEX IF NOT EXISTS idx_users_email       ON users(LOWER(email))",
            "CREATE INDEX IF NOT EXISTS idx_users_ip          ON users(ip_address)",
            "CREATE INDEX IF NOT EXISTS idx_users_plan        ON users(plan)",
            "CREATE INDEX IF NOT EXISTS idx_users_last_seen   ON users(last_seen DESC)",

            "CREATE INDEX IF NOT EXISTS idx_places_name       ON places(name)",
            "CREATE INDEX IF NOT EXISTS idx_places_category   ON places(category)",
            "CREATE INDEX IF NOT EXISTS idx_places_fetched    ON places(last_fetched_at DESC NULLS LAST)",

            "CREATE INDEX IF NOT EXISTS idx_reviews_place_id  ON reviews(place_id)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_date      ON reviews(review_date DESC NULLS LAST)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_sentiment ON reviews(sentiment_label)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_rating    ON reviews(rating)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_topic     ON reviews(place_id, topic_cluster)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_pl_date   ON reviews(place_id, review_date DESC NULLS LAST)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_pl_sent   ON reviews(place_id, sentiment_label)",
            "CREATE INDEX IF NOT EXISTS idx_reviews_suspicious ON reviews(is_suspicious) WHERE is_suspicious = TRUE",

            "CREATE INDEX IF NOT EXISTS idx_history_date      ON search_history(searched_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_history_user      ON search_history(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_history_user_date ON search_history(user_id, searched_at DESC)",

            "CREATE INDEX IF NOT EXISTS idx_usq_user          ON user_search_queries(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_usq_date          ON user_search_queries(searched_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_usq_user_date     ON user_search_queries(user_id, searched_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_usq_type          ON user_search_queries(query_type)",

            "CREATE INDEX IF NOT EXISTS idx_bookmarks_user    ON bookmarks(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_product_user      ON product_analyses(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_product_reviews   ON product_reviews(analysis_id)",
            "CREATE INDEX IF NOT EXISTS idx_product_rev_order ON product_reviews(analysis_id, row_order)",
        ]
        conn = _conn()
        try:
            with conn.cursor() as cur:
                for s in stmts:
                    cur.execute(s)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise DBQueryError(f"Schema failed: {e}")
        finally:
            _rel(conn)

    # ── Auth helpers ──────────────────────────────────────────────────────────

    def get_user_by_email(self, email: str) -> Optional[dict]:
        """Fetch user profile by email. Returns None if not found."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM users WHERE LOWER(email)=LOWER(%s) LIMIT 1;",
                    (email,))
                row = cur.fetchone()
            return dict(row) if row else None
        finally:
            _rel(conn)

    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        """Fetch user profile by UUID."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE id=%s LIMIT 1;", (user_id,))
                row = cur.fetchone()
            return dict(row) if row else None
        finally:
            _rel(conn)

    def create_user(self, user: dict) -> str:
        """Create a new user profile row. Returns the new UUID."""
        sql = """
        INSERT INTO users (
            email, name, phone,
            ip_address, country, country_code, region, region_name,
            city, zip_code, latitude, longitude, timezone, isp, org,
            user_agent, browser_name, browser_version, os_name, os_version,
            device_type, is_mobile, is_bot,
            screen_width, screen_height, language, languages,
            platform, timezone_js,
            gps_latitude, gps_longitude, gps_accuracy, gps_granted
        ) VALUES (
            %(email)s, %(name)s, %(phone)s,
            %(ip_address)s, %(country)s, %(country_code)s, %(region)s, %(region_name)s,
            %(city)s, %(zip_code)s, %(latitude)s, %(longitude)s, %(timezone)s, %(isp)s, %(org)s,
            %(user_agent)s, %(browser_name)s, %(browser_version)s, %(os_name)s, %(os_version)s,
            %(device_type)s, %(is_mobile)s, %(is_bot)s,
            %(screen_width)s, %(screen_height)s, %(language)s, %(languages)s,
            %(platform)s, %(timezone_js)s,
            %(gps_latitude)s, %(gps_longitude)s, %(gps_accuracy)s, %(gps_granted)s
        )
        ON CONFLICT (email) DO UPDATE SET last_seen = NOW()
        RETURNING id;
        """
        defaults = {
            "email": None, "name": "", "phone": None,
            "ip_address": None, "country": None, "country_code": None,
            "region": None, "region_name": None, "city": None, "zip_code": None,
            "latitude": None, "longitude": None, "timezone": None, "isp": None, "org": None,
            "user_agent": None, "browser_name": None, "browser_version": None,
            "os_name": None, "os_version": None, "device_type": "desktop",
            "is_mobile": False, "is_bot": False,
            "screen_width": None, "screen_height": None, "language": None,
            "languages": None, "platform": None, "timezone_js": None,
            "gps_latitude": None, "gps_longitude": None, "gps_accuracy": None,
            "gps_granted": False,
        }
        params = {k: _py(user.get(k, defaults[k])) for k in defaults}
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                user_id = str(cur.fetchone()[0])
            conn.commit()
            return user_id
        except Exception as e:
            conn.rollback()
            raise DBQueryError(f"create_user failed: {e}")
        finally:
            _rel(conn)

    def upsert_user(self, user: dict) -> str:
        """Create or update user profile. Returns UUID."""
        existing = self.get_user_by_email(user.get("email", ""))
        if existing:
            self.touch_user(str(existing["id"]))
            return str(existing["id"])
        return self.create_user(user)

    def update_user_email(self, user_id: str, email: str):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET email=%s, last_seen=NOW() WHERE id=%s;",
                    (email, user_id))
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            _rel(conn)

    def touch_user(self, user_id: str):
        """Update last_seen and increment total_searches."""
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET last_seen=NOW(), total_searches=total_searches+1 WHERE id=%s;",
                    (user_id,))
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            _rel(conn)

    def get_all_users(self, limit: int = 200) -> pd.DataFrame:
        """Admin: fetch all user profiles. Do NOT expose to regular users."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM users ORDER BY created_at DESC LIMIT %s;",
                    (limit,))
                rows = cur.fetchall()
            return _cast_df(pd.DataFrame([dict(r) for r in rows]))
        finally:
            _rel(conn)

    def update_user_gps(self, user_id, lat, lon, acc):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET gps_latitude=%s, gps_longitude=%s, "
                    "gps_accuracy=%s, gps_granted=TRUE WHERE id=%s;",
                    (_py(lat), _py(lon), _py(acc), user_id))
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            _rel(conn)

    def update_user_screen(self, uid, w, h, lang, tz, plat, langs):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET screen_width=%s, screen_height=%s, language=%s, "
                    "timezone_js=%s, platform=%s, languages=%s, last_seen=NOW() WHERE id=%s;",
                    (_py(w), _py(h), _py(lang), _py(tz), _py(plat), _py(langs), uid))
            conn.commit()
        except Exception:
            conn.rollback()
        finally:
            _rel(conn)

    # ── Usage limits ──────────────────────────────────────────────────────────

    def check_and_increment_usage(self, user_id: str) -> tuple[bool, int, int]:
        """
        Check if user can run an analysis. Increments count if allowed.
        Returns: (allowed: bool, used: int, limit: int)
        Auto-resets monthly count on new calendar month.
        """
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT plan, analyses_this_month, last_reset_month FROM users WHERE id=%s;",
                    (user_id,))
                row = cur.fetchone()
            if not row:
                return True, 0, FREE_MONTHLY_LIMIT

            plan    = row["plan"] or "free"
            used    = int(row["analyses_this_month"] or 0)
            month   = row["last_reset_month"] or ""
            current = datetime.utcnow().strftime("%Y-%m")
            limit   = 999999 if plan in ("pro", "admin") else FREE_MONTHLY_LIMIT

            if month != current:
                used = 0
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET analyses_this_month=0, last_reset_month=%s WHERE id=%s;",
                        (current, user_id))
                conn.commit()

            if used >= limit:
                return False, used, limit

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET analyses_this_month=analyses_this_month+1, "
                    "last_reset_month=%s WHERE id=%s;",
                    (current, user_id))
            conn.commit()
            return True, used + 1, limit
        except Exception:
            conn.rollback()
            return True, 0, FREE_MONTHLY_LIMIT  # fail open
        finally:
            _rel(conn)

    def get_usage(self, user_id: str) -> tuple[int, int]:
        """Returns (used_this_month, limit)."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT plan, analyses_this_month, last_reset_month FROM users WHERE id=%s;",
                    (user_id,))
                row = cur.fetchone()
            if not row:
                return 0, FREE_MONTHLY_LIMIT
            plan  = row["plan"] or "free"
            used  = int(row["analyses_this_month"] or 0)
            month = row["last_reset_month"] or ""
            if month != datetime.utcnow().strftime("%Y-%m"):
                used = 0
            limit = 999999 if plan in ("pro", "admin") else FREE_MONTHLY_LIMIT
            return used, limit
        finally:
            _rel(conn)

    # ── User search query tracking (granular) ─────────────────────────────────

    def log_user_query(
        self,
        user_id: Optional[str],
        query_text: str,
        query_type: str = "place",
        result_place_id: Optional[str] = None,
        result_name: Optional[str] = None,
        result_count: int = 0,
        duration_ms: Optional[int] = None,
        session_ip: Optional[str] = None,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> Optional[str]:
        """
        Log a granular user search query.
        Use this to track WHAT users are searching for in detail.
        Returns the new query row UUID or None on failure.
        """
        sql = """
        INSERT INTO user_search_queries (
            user_id, query_text, query_type, result_place_id, result_name,
            result_count, duration_ms, session_ip, status, error_message
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id;
        """
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    _py(user_id), query_text, query_type,
                    _py(result_place_id), _py(result_name),
                    int(result_count or 0), _py(duration_ms),
                    _py(session_ip), status, _py(error_message),
                ))
                qid = str(cur.fetchone()[0])
            conn.commit()
            return qid
        except Exception:
            conn.rollback()
            return None
        finally:
            _rel(conn)

    def get_user_queries(self, user_id: str, limit: int = 100) -> pd.DataFrame:
        """Fetch a user's search query history."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM user_search_queries WHERE user_id=%s "
                    "ORDER BY searched_at DESC LIMIT %s;",
                    (user_id, limit))
                rows = cur.fetchall()
            df = _cast_df(pd.DataFrame([dict(r) for r in rows]))
            if not df.empty and "searched_at" in df.columns:
                df["searched_at"] = pd.to_datetime(df["searched_at"], errors="coerce")
            return df
        finally:
            _rel(conn)

    def get_all_queries(self, limit: int = 500) -> pd.DataFrame:
        """Admin: fetch all user queries across all users."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT usq.*, u.email, u.name, u.country
                    FROM user_search_queries usq
                    LEFT JOIN users u ON usq.user_id = u.id
                    ORDER BY usq.searched_at DESC LIMIT %s;
                """, (limit,))
                rows = cur.fetchall()
            df = _cast_df(pd.DataFrame([dict(r) for r in rows]))
            if not df.empty and "searched_at" in df.columns:
                df["searched_at"] = pd.to_datetime(df["searched_at"], errors="coerce")
            return df
        finally:
            _rel(conn)

    def get_top_queries(self, days: int = 30, limit: int = 20) -> pd.DataFrame:
        """What are users searching most? Returns ranked query_text list."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT query_text, query_type, COUNT(*) AS search_count,
                           COUNT(DISTINCT user_id) AS unique_users,
                           MAX(searched_at) AS last_searched
                    FROM user_search_queries
                    WHERE searched_at >= NOW() - (%s || ' days')::INTERVAL
                      AND status = 'success'
                    GROUP BY query_text, query_type
                    ORDER BY search_count DESC
                    LIMIT %s;
                """, (days, limit))
                rows = cur.fetchall()
            return _cast_df(pd.DataFrame([dict(r) for r in rows]))
        finally:
            _rel(conn)

    # ── Places ────────────────────────────────────────────────────────────────

    def upsert_place(self, meta: dict):
        meta["last_fetched_at"] = datetime.utcnow().isoformat()
        sql = """
        INSERT INTO places
            (place_id, name, category, address, city, country,
             overall_rating, total_reviews_on_platform, reviews_fetched,
             avg_sentiment, pct_positive, pct_negative, trust_score, last_fetched_at)
        VALUES
            (%(place_id)s, %(name)s, %(category)s, %(address)s, %(city)s, %(country)s,
             %(overall_rating)s, %(total_reviews_on_platform)s, %(reviews_fetched)s,
             %(avg_sentiment)s, %(pct_positive)s, %(pct_negative)s,
             %(trust_score)s, %(last_fetched_at)s)
        ON CONFLICT (place_id) DO UPDATE SET
            name=EXCLUDED.name, category=EXCLUDED.category, address=EXCLUDED.address,
            city=EXCLUDED.city, country=EXCLUDED.country,
            overall_rating=EXCLUDED.overall_rating,
            total_reviews_on_platform=EXCLUDED.total_reviews_on_platform,
            reviews_fetched=EXCLUDED.reviews_fetched,
            avg_sentiment=EXCLUDED.avg_sentiment, pct_positive=EXCLUDED.pct_positive,
            pct_negative=EXCLUDED.pct_negative, trust_score=EXCLUDED.trust_score,
            last_fetched_at=EXCLUDED.last_fetched_at;
        """
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "place_id":                  _py(meta.get("place_id")),
                    "name":                      _py(meta.get("name")),
                    "category":                  _py(meta.get("category", "General")),
                    "address":                   _py(meta.get("address", "")),
                    "city":                      _py(meta.get("city")),
                    "country":                   _py(meta.get("country")),
                    "overall_rating":            _py(meta.get("overall_rating") or meta.get("rating")),
                    "total_reviews_on_platform": _py(meta.get("total_reviews_on_platform") or meta.get("reviews")),
                    "reviews_fetched":           _py(meta.get("reviews_fetched", 0)),
                    "avg_sentiment":             _py(meta.get("avg_sentiment")),
                    "pct_positive":              _py(meta.get("pct_positive")),
                    "pct_negative":              _py(meta.get("pct_negative")),
                    "trust_score":               _py(meta.get("trust_score")),
                    "last_fetched_at":           _py(meta.get("last_fetched_at")),
                })
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise DBQueryError(f"upsert_place failed: {e}")
        finally:
            _rel(conn)

    def get_all_places(self) -> pd.DataFrame:
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM places ORDER BY last_fetched_at DESC NULLS LAST;")
                rows = cur.fetchall()
            return _cast_df(pd.DataFrame([dict(r) for r in rows]))
        finally:
            _rel(conn)

    # ── Reviews ───────────────────────────────────────────────────────────────

    def upsert_reviews(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        sql = """
        INSERT INTO reviews (
            review_id, place_id, place_name, author, rating, review_text,
            review_date, relative_date, owner_response, has_owner_response,
            review_url, fetched_at, sentiment_label, sentiment_score,
            sentiment_pos, sentiment_neg, sentiment_neu,
            word_count, char_count, is_suspicious, suspicion_score,
            suspicion_reasons, topic_cluster, topic_label
        ) VALUES (
            %(review_id)s, %(place_id)s, %(place_name)s, %(author)s, %(rating)s, %(review_text)s,
            %(review_date)s, %(relative_date)s, %(owner_response)s, %(has_owner_response)s,
            %(review_url)s, %(fetched_at)s, %(sentiment_label)s, %(sentiment_score)s,
            %(sentiment_pos)s, %(sentiment_neg)s, %(sentiment_neu)s,
            %(word_count)s, %(char_count)s, %(is_suspicious)s, %(suspicion_score)s,
            %(suspicion_reasons)s, %(topic_cluster)s, %(topic_label)s
        )
        ON CONFLICT (review_id) DO UPDATE SET
            sentiment_label=EXCLUDED.sentiment_label,
            sentiment_score=EXCLUDED.sentiment_score,
            sentiment_pos=EXCLUDED.sentiment_pos, sentiment_neg=EXCLUDED.sentiment_neg,
            sentiment_neu=EXCLUDED.sentiment_neu, is_suspicious=EXCLUDED.is_suspicious,
            suspicion_score=EXCLUDED.suspicion_score,
            suspicion_reasons=EXCLUDED.suspicion_reasons,
            topic_cluster=EXCLUDED.topic_cluster, topic_label=EXCLUDED.topic_label;
        """
        records = []
        for _, row in df.iterrows():
            reasons = row.get("suspicion_reasons", [])
            if not isinstance(reasons, list):
                reasons = []
            rd = _py(row.get("review_date"))
            if rd is not None:
                try:
                    rd = pd.to_datetime(rd).strftime("%Y-%m-%d")
                except Exception:
                    rd = None
            records.append({
                "review_id":          str(_py(row.get("review_id"), f"r_{id(row)}")),
                "place_id":           str(_py(row.get("place_id"), "")),
                "place_name":         str(_py(row.get("place_name"), "")),
                "author":             _py(row.get("author"), "Anonymous"),
                "rating":             _py(row.get("rating")),
                "review_text":        _py(row.get("review_text"), ""),
                "review_date":        rd,
                "relative_date":      _py(row.get("relative_date"), ""),
                "owner_response":     _py(row.get("owner_response"), ""),
                "has_owner_response": bool(_py(row.get("has_owner_response"), False)),
                "review_url":         _py(row.get("review_url"), ""),
                "fetched_at":         _py(row.get("fetched_at"), datetime.utcnow().isoformat()),
                "sentiment_label":    _py(row.get("sentiment_label")),
                "sentiment_score":    _py(row.get("sentiment_score")),
                "sentiment_pos":      _py(row.get("sentiment_pos")),
                "sentiment_neg":      _py(row.get("sentiment_neg")),
                "sentiment_neu":      _py(row.get("sentiment_neu")),
                "word_count":         _py(row.get("word_count")),
                "char_count":         _py(row.get("char_count")),
                "is_suspicious":      bool(_py(row.get("is_suspicious"), False)),
                "suspicion_score":    _py(row.get("suspicion_score"), 0.0),
                "suspicion_reasons":  reasons,
                "topic_cluster":      _py(row.get("topic_cluster")),
                "topic_label":        _py(row.get("topic_label")),
            })
        conn = _conn()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, records, page_size=200)
            conn.commit()
            return len(records)
        except Exception as e:
            conn.rollback()
            raise DBQueryError(f"upsert_reviews failed: {e}")
        finally:
            _rel(conn)

    def get_reviews(self, place_id: str, min_date=None, max_date=None,
                    sentiment=None) -> pd.DataFrame:
        conds, params = ["place_id = %s"], [place_id]
        if min_date:
            conds.append("review_date >= %s")
            params.append(min_date)
        if max_date:
            conds.append("review_date <= %s")
            params.append(max_date)
        if sentiment:
            conds.append("sentiment_label = %s")
            params.append(sentiment)
        sql = (
            f"SELECT * FROM reviews WHERE {' AND '.join(conds)} "
            "ORDER BY review_date DESC NULLS LAST;"
        )
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            df = _cast_df(pd.DataFrame([dict(r) for r in rows]))
            if not df.empty and "review_date" in df.columns:
                df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
            return df
        finally:
            _rel(conn)

    def delete_place_reviews(self, place_id: str):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM reviews WHERE place_id=%s;", (place_id,))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise DBQueryError(f"delete_place_reviews failed: {e}")
        finally:
            _rel(conn)

    # ── Product reviews ───────────────────────────────────────────────────────

    def save_product_analysis(self, user_id: str, product_name: str,
                               platform: str, product_url: str,
                               df: pd.DataFrame) -> str:
        """Save a full product review analysis. Returns analysis_id UUID."""
        from src.analysis.sentiment import get_summary_stats
        from src.analysis.authenticity import get_trust_score
        stats = get_summary_stats(df)
        trust = get_trust_score(df)

        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO product_analyses
                        (user_id, product_name, platform, product_url, total_reviews,
                         avg_sentiment, pct_positive, pct_negative, avg_rating, trust_score)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
                """, (
                    _py(user_id), product_name, platform, product_url or "",
                    int(len(df)),
                    float(stats["avg_compound"]), float(stats["pct_positive"]),
                    float(stats["pct_negative"]),
                    float(stats.get("avg_rating", 0) or 0),
                    float(trust),
                ))
                analysis_id = str(cur.fetchone()[0])

            records = []
            for i, (_, row) in enumerate(df.iterrows()):
                records.append({
                    "analysis_id":     analysis_id,
                    "review_text":     str(_py(row.get("review_text"), "")),
                    "rating":          _py(row.get("rating")),
                    "sentiment_label": _py(row.get("sentiment_label")),
                    "sentiment_score": _py(row.get("sentiment_score")),
                    "sentiment_pos":   _py(row.get("sentiment_pos")),
                    "sentiment_neg":   _py(row.get("sentiment_neg")),
                    "sentiment_neu":   _py(row.get("sentiment_neu")),
                    "word_count":      _py(row.get("word_count")),
                    "is_suspicious":   bool(_py(row.get("is_suspicious"), False)),
                    "suspicion_score": _py(row.get("suspicion_score"), 0.0),
                    "topic_cluster":   _py(row.get("topic_cluster")),
                    "topic_label":     _py(row.get("topic_label")),
                    "row_order":       i,
                })

            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO product_reviews
                        (analysis_id, review_text, rating, sentiment_label, sentiment_score,
                         sentiment_pos, sentiment_neg, sentiment_neu, word_count,
                         is_suspicious, suspicion_score, topic_cluster, topic_label, row_order)
                    VALUES (%(analysis_id)s, %(review_text)s, %(rating)s, %(sentiment_label)s,
                            %(sentiment_score)s, %(sentiment_pos)s, %(sentiment_neg)s,
                            %(sentiment_neu)s, %(word_count)s, %(is_suspicious)s,
                            %(suspicion_score)s, %(topic_cluster)s, %(topic_label)s, %(row_order)s);
                """, records, page_size=200)
            conn.commit()
            return analysis_id
        except Exception as e:
            conn.rollback()
            raise DBQueryError(f"save_product_analysis failed: {e}")
        finally:
            _rel(conn)

    def get_product_analyses(self, user_id: str) -> pd.DataFrame:
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM product_analyses WHERE user_id=%s ORDER BY created_at DESC;",
                    (user_id,))
                rows = cur.fetchall()
            return _cast_df(pd.DataFrame([dict(r) for r in rows]))
        finally:
            _rel(conn)

    # ── Search history ────────────────────────────────────────────────────────

    def log_search(self, entry: dict):
        """Log a completed analysis to search_history."""
        entry.setdefault("searched_at", datetime.utcnow().isoformat())
        entry.setdefault("user_id", None)
        sql = """
        INSERT INTO search_history
            (user_id, place_name, place_id, category, reviews_count, avg_sentiment,
             avg_rating, pct_positive, trust_score, searched_at)
        VALUES
            (%(user_id)s, %(place_name)s, %(place_id)s, %(category)s, %(reviews_count)s,
             %(avg_sentiment)s, %(avg_rating)s, %(pct_positive)s, %(trust_score)s, %(searched_at)s);
        """
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "user_id":       _py(entry.get("user_id")),
                    "place_name":    entry.get("place_name", ""),
                    "place_id":      _py(entry.get("place_id")),
                    "category":      _py(entry.get("category")),
                    "reviews_count": _py(entry.get("reviews_count")),
                    "avg_sentiment": _py(entry.get("avg_sentiment")),
                    "avg_rating":    _py(entry.get("avg_rating")),
                    "pct_positive":  _py(entry.get("pct_positive")),
                    "trust_score":   _py(entry.get("trust_score")),
                    "searched_at":   entry.get("searched_at"),
                })
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise DBQueryError(f"log_search failed: {e}")
        finally:
            _rel(conn)

    def get_history(self, limit: int = 100, user_id: Optional[str] = None) -> pd.DataFrame:
        """Get search history. Pass user_id to filter to one user."""
        if user_id:
            sql = """
                SELECT sh.*, u.name as user_name, u.email as user_email
                FROM search_history sh
                LEFT JOIN users u ON sh.user_id = u.id
                WHERE sh.user_id = %s
                ORDER BY sh.searched_at DESC LIMIT %s;
            """
            params = (user_id, limit)
        else:
            sql = """
                SELECT sh.*, u.name as user_name, u.email as user_email
                FROM search_history sh
                LEFT JOIN users u ON sh.user_id = u.id
                ORDER BY sh.searched_at DESC LIMIT %s;
            """
            params = (limit,)
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            df = _cast_df(pd.DataFrame([dict(r) for r in rows]))
            if not df.empty and "searched_at" in df.columns:
                df["searched_at"] = pd.to_datetime(df["searched_at"], errors="coerce")
            return df
        finally:
            _rel(conn)

    # ── Bookmarks ─────────────────────────────────────────────────────────────

    def toggle_bookmark(self, user_id: str, place_id: str, place_name: str) -> bool:
        """Toggle bookmark. Returns True if now bookmarked, False if removed."""
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM bookmarks WHERE user_id=%s AND place_id=%s;",
                    (user_id, place_id))
                exists = cur.fetchone()
            if exists:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM bookmarks WHERE user_id=%s AND place_id=%s;",
                        (user_id, place_id))
                conn.commit()
                return False
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO bookmarks (user_id, place_id, place_name) VALUES (%s,%s,%s);",
                        (user_id, place_id, place_name))
                conn.commit()
                return True
        except Exception:
            conn.rollback()
            return False
        finally:
            _rel(conn)

    def get_bookmarks(self, user_id: str) -> pd.DataFrame:
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT b.*, p.avg_sentiment, p.pct_positive, p.overall_rating "
                    "FROM bookmarks b LEFT JOIN places p ON b.place_id=p.place_id "
                    "WHERE b.user_id=%s ORDER BY b.created_at DESC;",
                    (user_id,))
                rows = cur.fetchall()
            return _cast_df(pd.DataFrame([dict(r) for r in rows]))
        finally:
            _rel(conn)

    def is_bookmarked(self, user_id: str, place_id: str) -> bool:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM bookmarks WHERE user_id=%s AND place_id=%s;",
                    (user_id, place_id))
                return bool(cur.fetchone())
        finally:
            _rel(conn)

    # ── Place summaries / comparison ──────────────────────────────────────────

    def get_place_summaries(self, place_ids=None) -> pd.DataFrame:
        if place_ids:
            ph  = ",".join(["%s"] * len(place_ids))
            sql = f"""
            SELECT p.place_id, p.name, p.category, p.overall_rating, p.last_fetched_at,
                COUNT(r.id) AS total_reviews,
                ROUND(AVG(r.sentiment_score)::NUMERIC,4) AS avg_sentiment,
                ROUND(AVG(r.rating)::NUMERIC,2) AS avg_rating,
                SUM(CASE WHEN r.sentiment_label='Positive' THEN 1 ELSE 0 END) AS positive_count,
                SUM(CASE WHEN r.sentiment_label='Negative' THEN 1 ELSE 0 END) AS negative_count,
                ROUND(100.0*SUM(CASE WHEN r.is_suspicious=FALSE THEN 1 ELSE 0 END)
                    /NULLIF(COUNT(r.id),0),1) AS trust_score
            FROM places p LEFT JOIN reviews r ON p.place_id=r.place_id
            WHERE p.place_id IN ({ph})
            GROUP BY p.place_id, p.name, p.category, p.overall_rating, p.last_fetched_at;
            """
            params = place_ids
        else:
            sql = """
            SELECT p.place_id, p.name, p.category, p.overall_rating, p.last_fetched_at,
                COUNT(r.id) AS total_reviews,
                ROUND(AVG(r.sentiment_score)::NUMERIC,4) AS avg_sentiment,
                ROUND(AVG(r.rating)::NUMERIC,2) AS avg_rating,
                SUM(CASE WHEN r.sentiment_label='Positive' THEN 1 ELSE 0 END) AS positive_count,
                SUM(CASE WHEN r.sentiment_label='Negative' THEN 1 ELSE 0 END) AS negative_count,
                ROUND(100.0*SUM(CASE WHEN r.is_suspicious=FALSE THEN 1 ELSE 0 END)
                    /NULLIF(COUNT(r.id),0),1) AS trust_score
            FROM places p LEFT JOIN reviews r ON p.place_id=r.place_id
            GROUP BY p.place_id, p.name, p.category, p.overall_rating, p.last_fetched_at;
            """
            params = []
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            return _cast_df(pd.DataFrame([dict(r) for r in rows]))
        finally:
            _rel(conn)

    # ── Session helpers (compatibility shim) ──────────────────────────────────

    def create_session(self, user_id: str, ip: str) -> str:
        return user_id  # No separate sessions table — user_id IS the session key

    def touch_session(self, session_id: str):
        pass  # No-op
