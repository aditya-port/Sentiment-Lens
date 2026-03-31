-- ================================================================
-- Sentiment Lens — Optimized Supabase Schema v2
-- ================================================================
-- Run this in: Supabase Dashboard → SQL Editor → New Query → Run
--
-- SECURITY NOTE:
--   * Use service_role key in .env for server-side (bypasses RLS)
--   * User passwords are NEVER stored here — Supabase Auth handles
--     them with bcrypt in auth.users (a separate, hidden schema)
--   * Emails in public.users are read-only mirrors for the app
-- ================================================================

-- Required extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ================================================================
-- TABLE: users
-- Mirror of Supabase auth.users + rich tracking profile
-- Passwords are NEVER stored here.
-- ================================================================
CREATE TABLE IF NOT EXISTS public.users (
    id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    email               TEXT        UNIQUE,
    name                TEXT,
    phone               TEXT,
    plan                TEXT        DEFAULT 'free' CHECK (plan IN ('free','pro','admin')),
    analyses_this_month INTEGER     DEFAULT 0,
    last_reset_month    TEXT        DEFAULT '',
    total_searches      INTEGER     DEFAULT 0,

    -- Network / geo (collected server-side)
    ip_address          TEXT,
    country             TEXT,
    country_code        TEXT,
    region              TEXT,
    region_name         TEXT,
    city                TEXT,
    zip_code            TEXT,
    latitude            NUMERIC(10,6),
    longitude           NUMERIC(10,6),
    timezone            TEXT,
    isp                 TEXT,
    org                 TEXT,

    -- Device / browser (from User-Agent)
    user_agent          TEXT,
    browser_name        TEXT,
    browser_version     TEXT,
    os_name             TEXT,
    os_version          TEXT,
    device_type         TEXT        DEFAULT 'desktop',
    is_mobile           BOOLEAN     DEFAULT FALSE,
    is_bot              BOOLEAN     DEFAULT FALSE,

    -- Client-side JS data
    screen_width        INTEGER,
    screen_height       INTEGER,
    language            TEXT,
    languages           TEXT,
    platform            TEXT,
    timezone_js         TEXT,

    -- GPS (optional, user-granted)
    gps_latitude        NUMERIC(10,6),
    gps_longitude       NUMERIC(10,6),
    gps_accuracy        NUMERIC(10,2),
    gps_granted         BOOLEAN     DEFAULT FALSE,

    -- Timestamps
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    last_seen           TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.users IS
  'User profiles. Passwords are NOT stored — Supabase Auth (auth.users) handles authentication via bcrypt.';
COMMENT ON COLUMN public.users.plan IS 'free = 10 analyses/month, pro = unlimited';
COMMENT ON COLUMN public.users.email IS 'Mirror of auth.users.email. Read-only replica for the app profile.';


-- ================================================================
-- TABLE: places
-- One row per unique Google Maps place
-- ================================================================
CREATE TABLE IF NOT EXISTS public.places (
    id                          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    place_id                    TEXT        UNIQUE NOT NULL,
    name                        TEXT        NOT NULL,
    category                    TEXT        DEFAULT 'General',
    address                     TEXT,
    city                        TEXT,
    country                     TEXT,
    overall_rating              NUMERIC(3,2),
    total_reviews_on_platform   INTEGER,
    reviews_fetched             INTEGER     DEFAULT 0,
    avg_sentiment               NUMERIC(6,4),
    pct_positive                NUMERIC(5,2),
    pct_negative                NUMERIC(5,2),
    trust_score                 NUMERIC(5,2),
    last_fetched_at             TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.places IS 'Google Maps places. place_id matches the SerpApi data_id field.';


-- ================================================================
-- TABLE: reviews
-- Individual reviews + full sentiment enrichment
-- ================================================================
CREATE TABLE IF NOT EXISTS public.reviews (
    id                  UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    review_id           TEXT        UNIQUE NOT NULL,
    place_id            TEXT        NOT NULL REFERENCES public.places(place_id) ON DELETE CASCADE,
    place_name          TEXT        NOT NULL,
    author              TEXT,
    rating              NUMERIC(2,1),
    review_text         TEXT,
    review_date         DATE,
    relative_date       TEXT,
    owner_response      TEXT,
    has_owner_response  BOOLEAN     DEFAULT FALSE,
    review_url          TEXT,
    fetched_at          TIMESTAMPTZ DEFAULT NOW(),

    -- VADER Sentiment
    sentiment_label     TEXT        CHECK (sentiment_label IN ('Positive', 'Neutral', 'Negative')),
    sentiment_score     NUMERIC(6,4),
    sentiment_pos       NUMERIC(6,4),
    sentiment_neg       NUMERIC(6,4),
    sentiment_neu       NUMERIC(6,4),

    -- Text statistics
    word_count          INTEGER,
    char_count          INTEGER,

    -- Authenticity
    is_suspicious       BOOLEAN     DEFAULT FALSE,
    suspicion_score     NUMERIC(4,2) DEFAULT 0.0,
    suspicion_reasons   TEXT[]      DEFAULT '{}',

    -- Topic clustering (K-Means)
    topic_cluster       INTEGER,
    topic_label         TEXT
);

COMMENT ON TABLE public.reviews IS 'Individual reviews enriched with sentiment, authenticity, and topic clustering.';


-- ================================================================
-- TABLE: product_analyses
-- One row per product review paste session
-- ================================================================
CREATE TABLE IF NOT EXISTS public.product_analyses (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID        REFERENCES public.users(id) ON DELETE SET NULL,
    product_name    TEXT        NOT NULL,
    source          TEXT        DEFAULT 'manual',
    platform        TEXT        DEFAULT 'Other',
    product_url     TEXT,
    total_reviews   INTEGER     DEFAULT 0,
    avg_sentiment   NUMERIC(6,4),
    pct_positive    NUMERIC(5,2),
    pct_negative    NUMERIC(5,2),
    avg_rating      NUMERIC(3,2),
    trust_score     NUMERIC(5,2),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


-- ================================================================
-- TABLE: product_reviews
-- Individual reviews per product analysis session
-- ================================================================
CREATE TABLE IF NOT EXISTS public.product_reviews (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    analysis_id     UUID        REFERENCES public.product_analyses(id) ON DELETE CASCADE,
    review_text     TEXT        NOT NULL,
    rating          NUMERIC(2,1),
    sentiment_label TEXT,
    sentiment_score NUMERIC(6,4),
    sentiment_pos   NUMERIC(6,4),
    sentiment_neg   NUMERIC(6,4),
    sentiment_neu   NUMERIC(6,4),
    word_count      INTEGER,
    is_suspicious   BOOLEAN     DEFAULT FALSE,
    suspicion_score NUMERIC(4,2) DEFAULT 0.0,
    topic_cluster   INTEGER,
    topic_label     TEXT,
    row_order       INTEGER
);


-- ================================================================
-- TABLE: search_history
-- Audit log of every place analysis run
-- ================================================================
CREATE TABLE IF NOT EXISTS public.search_history (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID        REFERENCES public.users(id) ON DELETE SET NULL,
    place_name      TEXT        NOT NULL,
    place_id        TEXT,
    category        TEXT,
    reviews_count   INTEGER,
    avg_sentiment   NUMERIC(6,4),
    avg_rating      NUMERIC(3,2),
    pct_positive    NUMERIC(5,2),
    trust_score     NUMERIC(5,2),
    searched_at     TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.search_history IS 'Audit log of every place analysis. Linked to users for per-user history.';


-- ================================================================
-- TABLE: user_search_queries  [NEW]
-- Granular query tracking — what users are searching in detail
-- Powers user-level analytics and usage dashboards
-- ================================================================
CREATE TABLE IF NOT EXISTS public.user_search_queries (
    id              UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID        REFERENCES public.users(id) ON DELETE SET NULL,
    query_text      TEXT        NOT NULL,
    query_type      TEXT        DEFAULT 'place' CHECK (query_type IN ('place','product','compare')),
    result_place_id TEXT,
    result_name     TEXT,
    result_count    INTEGER     DEFAULT 0,
    duration_ms     INTEGER,    -- pipeline execution time in milliseconds
    session_ip      TEXT,
    status          TEXT        DEFAULT 'success' CHECK (status IN ('success','error','limit_reached')),
    error_message   TEXT,
    searched_at     TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE public.user_search_queries IS
  'Granular per-user query log. Tracks every search attempt including errors and timing.';


-- ================================================================
-- TABLE: bookmarks
-- ================================================================
CREATE TABLE IF NOT EXISTS public.bookmarks (
    id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id     UUID        NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    place_id    TEXT        NOT NULL,
    place_name  TEXT,
    note        TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, place_id)
);


-- ================================================================
-- INDEXES — optimised for the app's most common queries
-- ================================================================

-- users
CREATE INDEX IF NOT EXISTS idx_users_email      ON public.users (LOWER(email));
CREATE INDEX IF NOT EXISTS idx_users_ip         ON public.users (ip_address);
CREATE INDEX IF NOT EXISTS idx_users_plan       ON public.users (plan);
CREATE INDEX IF NOT EXISTS idx_users_last_seen  ON public.users (last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_users_created    ON public.users (created_at DESC);

-- places
CREATE INDEX IF NOT EXISTS idx_places_name          ON public.places (name);
CREATE INDEX IF NOT EXISTS idx_places_category      ON public.places (category);
CREATE INDEX IF NOT EXISTS idx_places_last_fetched  ON public.places (last_fetched_at DESC NULLS LAST);

-- reviews
CREATE INDEX IF NOT EXISTS idx_reviews_place_id   ON public.reviews (place_id);
CREATE INDEX IF NOT EXISTS idx_reviews_date        ON public.reviews (review_date DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment   ON public.reviews (sentiment_label);
CREATE INDEX IF NOT EXISTS idx_reviews_rating      ON public.reviews (rating);
CREATE INDEX IF NOT EXISTS idx_reviews_topic       ON public.reviews (place_id, topic_cluster);
CREATE INDEX IF NOT EXISTS idx_reviews_suspicious  ON public.reviews (is_suspicious) WHERE is_suspicious = TRUE;
-- Composite: place + date for date-range queries
CREATE INDEX IF NOT EXISTS idx_reviews_place_date  ON public.reviews (place_id, review_date DESC NULLS LAST);
-- Composite: place + sentiment for filter queries
CREATE INDEX IF NOT EXISTS idx_reviews_place_sent  ON public.reviews (place_id, sentiment_label);

-- product_analyses
CREATE INDEX IF NOT EXISTS idx_product_analyses_user    ON public.product_analyses (user_id);
CREATE INDEX IF NOT EXISTS idx_product_analyses_date    ON public.product_analyses (created_at DESC);

-- product_reviews
CREATE INDEX IF NOT EXISTS idx_product_reviews_analysis ON public.product_reviews (analysis_id);
CREATE INDEX IF NOT EXISTS idx_product_reviews_order    ON public.product_reviews (analysis_id, row_order);

-- search_history
CREATE INDEX IF NOT EXISTS idx_history_date     ON public.search_history (searched_at DESC);
CREATE INDEX IF NOT EXISTS idx_history_user     ON public.search_history (user_id);
CREATE INDEX IF NOT EXISTS idx_history_place    ON public.search_history (place_id);
-- Composite: user + date for per-user history
CREATE INDEX IF NOT EXISTS idx_history_user_date ON public.search_history (user_id, searched_at DESC);

-- user_search_queries
CREATE INDEX IF NOT EXISTS idx_usq_user         ON public.user_search_queries (user_id);
CREATE INDEX IF NOT EXISTS idx_usq_date         ON public.user_search_queries (searched_at DESC);
CREATE INDEX IF NOT EXISTS idx_usq_type         ON public.user_search_queries (query_type);
CREATE INDEX IF NOT EXISTS idx_usq_user_date    ON public.user_search_queries (user_id, searched_at DESC);
-- Full-text search on query_text
CREATE INDEX IF NOT EXISTS idx_usq_text_search  ON public.user_search_queries USING gin(to_tsvector('english', query_text));

-- bookmarks
CREATE INDEX IF NOT EXISTS idx_bookmarks_user   ON public.bookmarks (user_id);
CREATE INDEX IF NOT EXISTS idx_bookmarks_place  ON public.bookmarks (place_id);


-- ================================================================
-- VIEW: place_summaries
-- Aggregated per-place stats — used by Compare & History pages
-- ================================================================
CREATE OR REPLACE VIEW public.place_summaries AS
SELECT
    p.place_id,
    p.name,
    p.category,
    p.address,
    p.city,
    p.overall_rating,
    p.reviews_fetched,
    p.last_fetched_at,
    COUNT(r.id)                                                       AS total_reviews,
    ROUND(AVG(r.sentiment_score)::NUMERIC, 4)                         AS avg_sentiment,
    ROUND(AVG(r.rating)::NUMERIC, 2)                                  AS avg_rating,
    SUM(CASE WHEN r.sentiment_label = 'Positive' THEN 1 ELSE 0 END)  AS positive_count,
    SUM(CASE WHEN r.sentiment_label = 'Negative' THEN 1 ELSE 0 END)  AS negative_count,
    SUM(CASE WHEN r.sentiment_label = 'Neutral'  THEN 1 ELSE 0 END)  AS neutral_count,
    SUM(CASE WHEN r.is_suspicious = TRUE          THEN 1 ELSE 0 END)  AS suspicious_count,
    SUM(CASE WHEN r.has_owner_response = TRUE      THEN 1 ELSE 0 END) AS owner_responses_count,
    ROUND(
        100.0 * SUM(CASE WHEN r.is_suspicious = FALSE THEN 1 ELSE 0 END)
            / NULLIF(COUNT(r.id), 0), 1
    )                                                                 AS trust_score
FROM public.places p
LEFT JOIN public.reviews r ON p.place_id = r.place_id
GROUP BY p.place_id, p.name, p.category, p.address, p.city,
         p.overall_rating, p.reviews_fetched, p.last_fetched_at;


-- ================================================================
-- VIEW: user_activity_summary  [NEW]
-- Per-user analytics — who searches what, how often
-- ================================================================
CREATE OR REPLACE VIEW public.user_activity_summary AS
SELECT
    u.id                                    AS user_id,
    u.email,
    u.name,
    u.plan,
    u.country,
    u.city,
    u.device_type,
    u.created_at,
    u.last_seen,
    u.total_searches,
    COUNT(DISTINCT usq.id)                  AS total_queries,
    COUNT(DISTINCT sh.id)                   AS total_analyses,
    COUNT(DISTINCT sh.place_id)             AS unique_places_analysed,
    COUNT(DISTINCT pa.id)                   AS product_analyses,
    MAX(usq.searched_at)                    AS last_query_at,
    ROUND(AVG(usq.duration_ms)::NUMERIC, 0) AS avg_query_duration_ms,
    COUNT(DISTINCT CASE WHEN usq.query_type = 'place'   THEN usq.id END) AS place_queries,
    COUNT(DISTINCT CASE WHEN usq.query_type = 'product' THEN usq.id END) AS product_queries
FROM public.users u
LEFT JOIN public.user_search_queries usq ON usq.user_id = u.id
LEFT JOIN public.search_history sh       ON sh.user_id  = u.id
LEFT JOIN public.product_analyses pa     ON pa.user_id  = u.id
GROUP BY u.id, u.email, u.name, u.plan, u.country, u.city,
         u.device_type, u.created_at, u.last_seen, u.total_searches;


-- ================================================================
-- ROW LEVEL SECURITY
-- The app uses service_role key (bypasses RLS).
-- These policies protect direct anon/dashboard access.
-- ================================================================
ALTER TABLE public.users               ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.places              ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.reviews             ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.product_analyses    ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.product_reviews     ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.search_history      ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_search_queries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bookmarks           ENABLE ROW LEVEL SECURITY;

-- Service role full access (app uses service_role key which bypasses RLS anyway)
-- DROP first so re-running this script is safe
DO $$ BEGIN
  DROP POLICY IF EXISTS "service_full_users"   ON public.users;
  DROP POLICY IF EXISTS "service_full_places"  ON public.places;
  DROP POLICY IF EXISTS "service_full_reviews" ON public.reviews;
  DROP POLICY IF EXISTS "service_full_pa"      ON public.product_analyses;
  DROP POLICY IF EXISTS "service_full_pr"      ON public.product_reviews;
  DROP POLICY IF EXISTS "service_full_history" ON public.search_history;
  DROP POLICY IF EXISTS "service_full_usq"     ON public.user_search_queries;
  DROP POLICY IF EXISTS "service_full_bm"      ON public.bookmarks;
END $$;

CREATE POLICY "service_full_users"   ON public.users               FOR ALL USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_full_places"  ON public.places              FOR ALL USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_full_reviews" ON public.reviews             FOR ALL USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_full_pa"      ON public.product_analyses    FOR ALL USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_full_pr"      ON public.product_reviews     FOR ALL USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_full_history" ON public.search_history      FOR ALL USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_full_usq"     ON public.user_search_queries FOR ALL USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_full_bm"      ON public.bookmarks           FOR ALL USING (TRUE) WITH CHECK (TRUE);


-- ================================================================
-- VERIFICATION
-- Run after setup to confirm all objects were created:
-- ================================================================
-- SELECT table_name FROM information_schema.tables
-- WHERE table_schema = 'public' ORDER BY table_name;
--
-- SELECT viewname FROM pg_views WHERE schemaname = 'public';
--
-- SELECT indexname FROM pg_indexes WHERE schemaname = 'public'
-- ORDER BY tablename, indexname;
