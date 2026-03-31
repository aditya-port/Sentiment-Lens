"""
Sentiment Lens — Final SaaS Build
Email + Password auth via Supabase Auth REST API
Run: python -m streamlit run app.py
"""
from __future__ import annotations
import os, sys, json, warnings, hashlib
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
import pandas as pd
import requests as _req
from dotenv import load_dotenv

load_dotenv()

# ── Site URL for email confirmation redirect ───────────────────────────────────
# Set SITE_URL in .env to your production URL (e.g. https://yourapp.up.railway.app)
# This fixes the "email link opens localhost" bug.
_SITE_URL = os.getenv("SITE_URL", "").strip()

st.set_page_config(
    page_title="Sentiment Lens",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════════════
# THEME CSS
# ═══════════════════════════════════════════════════════════════════════════════
_THEME_KEY = "sl_theme"

def _inject_css():
    dark = st.session_state.get(_THEME_KEY, "dark") == "dark"
    BG      = "#060B18" if dark else "#F0F4FF"
    SURFACE = "#0D1426" if dark else "#FFFFFF"
    SURF2   = "#141E35" if dark else "#EEF2FF"
    BORDER  = "rgba(99,102,241,0.15)" if dark else "rgba(99,102,241,0.20)"
    BORDER2 = "rgba(255,255,255,0.06)" if dark else "rgba(0,0,0,0.06)"
    TEXT    = "#E2E8F0" if dark else "#0F172A"
    MUTED   = "#64748B"
    ACCENT  = "#6366F1" if dark else "#4F46E5"
    ACCENT2 = "#818CF8" if dark else "#6366F1"
    GLOW    = "rgba(99,102,241,0.18)" if dark else "rgba(99,102,241,0.12)"
    GLOWS   = "rgba(99,102,241,0.06)"
    GLASS   = "rgba(255,255,255,0.03)" if dark else "rgba(255,255,255,0.70)"
    GREEN   = "#10B981" if dark else "#059669"
    RED     = "#EF4444" if dark else "#DC2626"

    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap');
*{{box-sizing:border-box}}
html,body,[class*="css"]{{font-family:'Inter',sans-serif!important;background:{BG}!important;color:{TEXT}!important}}
.stApp{{background:{BG}!important}}
.main .block-container{{padding:1.5rem 2rem 3rem!important;max-width:1400px!important}}

[data-testid="collapsedControl"]{{display:none!important}}
section[data-testid="stSidebar"]{{
    min-width:268px!important;max-width:268px!important;
    transform:none!important;visibility:visible!important;
    background:{SURFACE}!important;border-right:1px solid {BORDER2}!important;
    box-shadow:4px 0 24px rgba(0,0,0,0.12)!important;
}}
section[data-testid="stSidebar"][aria-expanded="false"]{{min-width:268px!important;margin-left:0!important}}
[data-testid="stSidebar"] *{{color:{TEXT}!important}}
[data-testid="stSidebar"] hr{{border-color:{BORDER2}!important}}

[data-testid="metric-container"]{{
    background:{GLASS}!important;backdrop-filter:blur(12px)!important;
    border:1px solid {BORDER}!important;border-radius:16px!important;
    padding:1rem 1.2rem!important;transition:all .25s!important;
    position:relative!important;overflow:hidden!important;
}}
[data-testid="metric-container"]:hover{{
    border-color:{ACCENT}!important;box-shadow:0 0 24px {GLOW}!important;
    transform:translateY(-2px)!important;
}}
[data-testid="metric-container"]::before{{
    content:""!important;position:absolute!important;top:0!important;
    left:0!important;right:0!important;height:1px!important;
    background:linear-gradient(90deg,transparent,{ACCENT},transparent)!important;opacity:.6!important;
}}
[data-testid="metric-container"] label{{
    font-size:.63rem!important;font-weight:600!important;color:{MUTED}!important;
    text-transform:uppercase!important;letter-spacing:.10em!important;
}}
[data-testid="metric-container"] [data-testid="stMetricValue"]{{
    font-size:1.55rem!important;font-weight:700!important;color:{TEXT}!important;
    font-family:'Space Grotesk',sans-serif!important;letter-spacing:-0.03em!important;
}}
[data-testid="stMetricDelta"]{{font-size:.68rem!important;font-weight:500!important}}

.stTabs [data-baseweb="tab-list"]{{
    gap:2px!important;background:{SURF2}!important;border-radius:12px!important;
    padding:4px!important;border:1px solid {BORDER2}!important;
}}
.stTabs [data-baseweb="tab"]{{
    border-radius:8px!important;padding:7px 14px!important;
    font-size:.82rem!important;font-weight:500!important;
    color:{MUTED}!important;background:transparent!important;transition:all .15s!important;
}}
.stTabs [aria-selected="true"]{{background:{SURFACE}!important;color:{ACCENT}!important;
    font-weight:600!important;box-shadow:0 2px 8px rgba(0,0,0,.15)!important;}}

div.stButton>button[kind="primary"]{{
    background:linear-gradient(135deg,{ACCENT},{ACCENT2})!important;
    color:#fff!important;border:none!important;border-radius:10px!important;
    font-weight:600!important;padding:.55rem 1.1rem!important;width:100%!important;
    transition:all .2s!important;box-shadow:0 4px 15px {GLOW}!important;
}}
div.stButton>button[kind="primary"]:hover{{
    transform:translateY(-1px)!important;box-shadow:0 8px 25px {GLOW}!important;
}}
div.stButton>button:not([kind="primary"]){{
    border-radius:10px!important;border:1px solid {BORDER2}!important;
    background:{GLASS}!important;color:{TEXT}!important;font-weight:500!important;
    transition:all .15s!important;
}}
div.stButton>button:not([kind="primary"]):hover{{
    border-color:{ACCENT}!important;color:{ACCENT}!important;
}}

[data-testid="stTextInput"] input,[data-testid="stSelectbox"] select{{
    background:{SURF2}!important;border:1px solid {BORDER2}!important;
    border-radius:10px!important;color:{TEXT}!important;transition:all .15s!important;
}}
[data-testid="stTextInput"] input:focus{{
    border-color:{ACCENT}!important;box-shadow:0 0 0 3px {GLOWS}!important;
}}

.sl-place-header{{
    background:{GLASS}!important;backdrop-filter:blur(16px)!important;
    border:1px solid {BORDER}!important;border-radius:18px!important;
    padding:1.4rem 1.8rem!important;margin-bottom:1.4rem!important;
    box-shadow:0 8px 32px rgba(0,0,0,.10),inset 0 1px 0 rgba(255,255,255,.06)!important;
    position:relative!important;overflow:hidden!important;
}}
.sl-place-header::before{{
    content:""!important;position:absolute!important;top:-80px!important;right:-80px!important;
    width:200px!important;height:200px!important;
    background:radial-gradient(circle,{GLOW} 0%,transparent 70%)!important;pointer-events:none!important;
}}
.sl-place-name{{
    font-size:1.55rem!important;font-weight:700!important;margin:0!important;
    font-family:'Space Grotesk',sans-serif!important;letter-spacing:-0.03em!important;
    background:linear-gradient(135deg,{TEXT},{ACCENT2})!important;
    -webkit-background-clip:text!important;-webkit-text-fill-color:transparent!important;background-clip:text!important;
}}
.sl-place-meta{{font-size:.82rem!important;color:{MUTED}!important;margin:.35rem 0 0!important}}

.sl-card{{
    background:{GLASS}!important;backdrop-filter:blur(8px)!important;
    border:1px solid {BORDER2}!important;border-radius:12px!important;
    padding:.9rem 1rem!important;margin:.35rem 0!important;transition:all .2s!important;
}}
.sl-card:hover{{border-color:{BORDER}!important;box-shadow:0 4px 16px {GLOWS}!important}}
.sl-card-title{{font-size:.68rem!important;font-weight:700!important;color:{MUTED}!important;
    text-transform:uppercase!important;letter-spacing:.08em!important;margin-bottom:4px!important}}
.sl-card-body{{font-size:.88rem!important;line-height:1.65!important;color:{TEXT}!important}}
.sl-section{{font-size:.65rem!important;font-weight:700!important;color:{ACCENT}!important;
    text-transform:uppercase!important;letter-spacing:.12em!important;margin-bottom:.5rem!important}}
.sl-divider{{height:1px!important;background:linear-gradient(90deg,transparent,{BORDER2},transparent)!important;margin:.9rem 0!important}}
.sl-info{{background:rgba(59,130,246,.06)!important;border:1px solid rgba(59,130,246,.18)!important;
    border-radius:10px!important;padding:.75rem 1rem!important;font-size:.85rem!important;color:#93C5FD!important}}
.sl-warning{{background:rgba(245,158,11,.06)!important;border:1px solid rgba(245,158,11,.18)!important;
    border-radius:10px!important;padding:.75rem 1rem!important;font-size:.85rem!important;color:#FCD34D!important}}

.badge{{display:inline-flex!important;align-items:center!important;gap:5px!important;
    padding:3px 10px!important;border-radius:20px!important;font-size:.71rem!important;font-weight:600!important}}
.badge-up{{background:rgba(16,185,129,.12)!important;color:{GREEN}!important;border:1px solid rgba(16,185,129,.2)!important}}
.badge-down{{background:rgba(239,68,68,.12)!important;color:{RED}!important;border:1px solid rgba(239,68,68,.2)!important}}
.badge-flat{{background:rgba(100,116,139,.12)!important;color:{MUTED}!important;border:1px solid rgba(100,116,139,.2)!important}}
.badge-cat{{background:rgba(99,102,241,.12)!important;color:{ACCENT2}!important;
    border:1px solid rgba(99,102,241,.2)!important;padding:2px 10px!important;
    border-radius:20px!important;font-size:.71rem!important;font-weight:600!important}}
.badge-free{{background:rgba(16,185,129,.10)!important;color:{GREEN}!important;
    border:1px solid rgba(16,185,129,.2)!important;padding:2px 8px!important;
    border-radius:20px!important;font-size:.68rem!important;font-weight:600!important}}
.badge-pro{{background:rgba(99,102,241,.15)!important;color:{ACCENT2}!important;
    border:1px solid rgba(99,102,241,.25)!important;padding:2px 8px!important;
    border-radius:20px!important;font-size:.68rem!important;font-weight:600!important}}

/* ── Auth pages ── */
.sl-auth-wrap{{max-width:440px!important;margin:3rem auto 0!important}}
.sl-auth-card{{
    background:{SURFACE}!important;border:1px solid {BORDER}!important;
    border-radius:20px!important;padding:2.5rem 2rem!important;
    box-shadow:0 20px 60px rgba(0,0,0,.2)!important;
}}
.sl-auth-logo{{text-align:center!important;font-size:2.6rem!important;margin-bottom:.4rem!important}}
.sl-auth-title{{
    text-align:center!important;font-size:1.45rem!important;font-weight:700!important;
    font-family:'Space Grotesk',sans-serif!important;letter-spacing:-0.03em!important;
    background:linear-gradient(135deg,{TEXT},{ACCENT2})!important;
    -webkit-background-clip:text!important;-webkit-text-fill-color:transparent!important;background-clip:text!important;
}}
.sl-auth-sub{{text-align:center!important;font-size:.83rem!important;color:{MUTED}!important;margin-bottom:1.6rem!important;margin-top:.35rem!important}}

/* ── Landing page ── */
.sl-hero{{text-align:center!important;padding:4rem 2rem 2rem!important}}
.sl-hero-title{{
    font-size:3rem!important;font-weight:700!important;
    font-family:'Space Grotesk',sans-serif!important;letter-spacing:-0.04em!important;
    background:linear-gradient(135deg,{TEXT} 0%,{ACCENT2} 60%)!important;
    -webkit-background-clip:text!important;-webkit-text-fill-color:transparent!important;background-clip:text!important;
    line-height:1.1!important;margin-bottom:.8rem!important;
}}
.sl-hero-sub{{font-size:1.1rem!important;color:{MUTED}!important;max-width:600px!important;margin:0 auto 2rem!important}}
.sl-feature-card{{
    background:{GLASS}!important;border:1px solid {BORDER}!important;
    border-radius:16px!important;padding:1.5rem!important;text-align:center!important;
    transition:all .25s!important;height:100%!important;
}}
.sl-feature-card:hover{{
    border-color:{ACCENT}!important;transform:translateY(-4px)!important;
    box-shadow:0 12px 40px {GLOW}!important;
}}
.sl-feature-icon{{font-size:2rem!important;margin-bottom:.8rem!important;display:block!important}}
.sl-feature-title{{font-size:1rem!important;font-weight:600!important;margin-bottom:.5rem!important}}
.sl-feature-desc{{font-size:.84rem!important;color:{MUTED}!important;line-height:1.6!important}}

/* ── Usage bar ── */
.sl-usage-bar-bg{{
    background:{SURF2}!important;border-radius:20px!important;height:6px!important;
    margin:.4rem 0!important;overflow:hidden!important;
}}
.sl-usage-bar-fill{{
    background:linear-gradient(90deg,{ACCENT},{ACCENT2})!important;
    height:6px!important;border-radius:20px!important;transition:width .4s!important;
}}

.sl-footer{{
    text-align:center!important;font-size:.68rem!important;color:{MUTED}!important;
    margin-top:3rem!important;padding-top:1.2rem!important;border-top:1px solid {BORDER2}!important;
}}
[data-testid="stDataFrame"]{{border-radius:12px!important;overflow:hidden!important;border:1px solid {BORDER2}!important}}
::-webkit-scrollbar{{width:4px!important;height:4px!important}}
::-webkit-scrollbar-thumb{{background:{BORDER}!important;border-radius:2px!important}}
#MainMenu,header,footer{{visibility:hidden!important}}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════════════════
def _init():
    for k, v in {
        _THEME_KEY:     "dark",
        "auth_mode":    "login",   # "login" | "register"
        "logged_in":    False,
        "user_id":      None,
        "user_email":   "",
        "user_name":    "",
        "user_plan":    "free",
        "page":         "Analyze",
        "place_id":     None,
        "place_meta":   {},
        "df":           pd.DataFrame(),
        "candidates":   [],
        "search_done":  False,
        "max_reviews":  80,
        "gps_asked":    False,
        "ai_messages":  [],
        "prod_df":      pd.DataFrame(),   # product analysis result
        "prod_name":    "",
    }.items():
        if k not in st.session_state:
            st.session_state[k] = v
_init()


# ═══════════════════════════════════════════════════════════════════════════════
# DB + ENV helpers
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_resource(show_spinner=False)
def _get_db():
    from src.storage.db import DatabaseManager, DBConfigError
    try:
        db = DatabaseManager()
        db.ensure_schema()
        return db
    except DBConfigError as e:
        return str(e)

def get_db():
    db = _get_db()
    if isinstance(db, str):
        st.error(f"**Database not configured** — {db}")
        st.stop()
    return db

def _serpapi_key(): return os.getenv("SERPAPI_KEY","").strip()
def _groq_key():    return os.getenv("GROK_KEY","").strip()
def _supa_url():    return os.getenv("SUPABASE_URL","").strip()
def _supa_key():    return os.getenv("SUPABASE_KEY","").strip()


# ═══════════════════════════════════════════════════════════════════════════════
# SUPABASE AUTH — email + password via REST API
# ═══════════════════════════════════════════════════════════════════════════════

def _hash_password(pw: str) -> str:
    """SHA-256 hash stored in our users table (Supabase Auth handles real auth)."""
    return hashlib.sha256(pw.encode()).hexdigest()


def auth_resend_confirmation(email: str) -> tuple[bool, str]:
    """Resend signup confirmation email via Supabase Auth REST API."""
    supa = _supa_url()
    if not supa:
        return False, "SUPABASE_URL is not set."
    url = f"{supa}/auth/v1/resend"
    body = {"type": "signup", "email": email}
    if _SITE_URL:
        body["options"] = {"emailRedirectTo": _SITE_URL}
    resp = _req.post(url,
        headers={"apikey": _supa_key(), "Content-Type": "application/json"},
        json=body, timeout=15)
    if resp.status_code == 200:
        return True, "\u2705 Confirmation email resent! Check your inbox (and spam)."
    err = resp.json().get("msg") or resp.json().get("error_description") or "Resend failed."
    return False, err


def auth_register(email: str, password: str, name: str) -> tuple[bool, str]:
    """Register via Supabase Auth REST API, then create our user profile row."""
    supa = _supa_url()
    if not supa:
        return False, "SUPABASE_URL is not set in your .env file."
    url = f"{supa}/auth/v1/signup"
    # Pass emailRedirectTo so confirmation links use production URL, not localhost
    signup_body: dict = {"email": email, "password": password}
    if _SITE_URL:
        signup_body["options"] = {"emailRedirectTo": _SITE_URL}
    resp = _req.post(url,
        headers={"apikey": _supa_key(), "Content-Type": "application/json"},
        json=signup_body,
        timeout=15)

    if resp.status_code not in (200, 201):
        err = resp.json().get("msg") or resp.json().get("error_description") or "Registration failed."
        return False, err

    data = resp.json()
    supa_uid = (data.get("user") or data).get("id", "")

    # Create profile row in our users table
    try:
        from src.auth.tracker import collect_all
        profile = collect_all(name, "")
    except Exception:
        profile = {"name": name}

    profile["email"]   = email
    profile["name"]    = name

    db = get_db()
    try:
        # Check if email already in our table (duplicate signup)
        existing = db.get_user_by_email(email)
        if not existing:
            db.create_user(profile)
    except Exception:
        pass

    return True, "Account created! Check your email to confirm, then log in."


def auth_login(email: str, password: str) -> tuple[bool, str, dict]:
    """Login via Supabase Auth REST API."""
    supa = _supa_url()
    if not supa:
        return False, "SUPABASE_URL is not set in your .env file.", {}
    url = f"{supa}/auth/v1/token?grant_type=password"
    resp = _req.post(url,
        headers={"apikey": _supa_key(), "Content-Type": "application/json"},
        json={"email": email, "password": password},
        timeout=15)

    if resp.status_code != 200:
        err = resp.json().get("error_description") or resp.json().get("msg") or "Invalid email or password."
        return False, err, {}

    # Fetch or create our user profile
    db = get_db()
    try:
        profile = db.get_user_by_email(email)
        if not profile:
            try:
                from src.auth.tracker import collect_all
                p = collect_all("", "")
            except Exception:
                p = {}
            p["email"] = email
            uid = db.create_user(p)
            profile = {"id": uid, "email": email, "name": email.split("@")[0], "plan":"free"}
    except Exception:
        profile = {"email": email, "name": email.split("@")[0], "plan":"free"}

    return True, "Logged in successfully.", profile


def _restore_from_query_params():
    """Restore session from URL ?uid= param (persists across Streamlit reruns)."""
    if st.session_state.get("logged_in"):
        return
    try:
        uid = st.query_params.get("uid", "")
        if not uid:
            return
        db = get_db()
        from src.storage.db import DatabaseManager
        if isinstance(db, DatabaseManager):
            users = db.get_all_users(500)
            if not users.empty and "id" in users.columns:
                match = users[users["id"].astype(str) == str(uid)]
                if not match.empty:
                    row = match.iloc[0]
                    st.session_state.update({
                        "logged_in":  True,
                        "user_id":    str(uid),
                        "user_email": str(row.get("email","") or ""),
                        "user_name":  str(row.get("name","User") or "User"),
                        "user_plan":  str(row.get("plan","free") or "free"),
                    })
    except Exception:
        pass


def _do_logout():
    try:
        st.query_params.clear()
    except Exception:
        pass
    for k in list(st.session_state.keys()):
        del st.session_state[k]
    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# LANDING PAGE
# ═══════════════════════════════════════════════════════════════════════════════
def page_landing():
    _restore_from_query_params()
    if st.session_state.get("logged_in"):
        st.rerun()

    st.markdown("""
    <div class="sl-hero">
        <div class="sl-hero-title">Understand what customers<br>really think</div>
        <div class="sl-hero-sub">
            Sentiment Lens analyses Google Maps reviews, product reviews, and any text
            with AI-powered insights — in seconds.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # CTA buttons
    _, c1, c2, _ = st.columns([1.5, 1, 1, 1.5])
    with c1:
        if st.button("Get started free →", type="primary", use_container_width=True):
            st.session_state["auth_mode"] = "register"
            st.rerun()
    with c2:
        if st.button("Sign in", use_container_width=True):
            st.session_state["auth_mode"] = "login"
            st.rerun()

    st.markdown("---")

    # Feature grid
    features = [
        ("🔍","Google Maps Reviews","Analyse any restaurant, hotel, hospital, or attraction worldwide."),
        ("📦","Product Reviews","Paste reviews from Flipkart, Meesho, Amazon — any platform."),
        ("🧠","AI Sentiment Scoring","VADER scores every review -1 → +1 with Positive / Neutral / Negative labels."),
        ("📊","Aspect Analysis","See exactly what customers say about Quality, Service, Value, and more."),
        ("🤖","AI Chat","Ask Groq-powered AI questions about any analysis in natural language."),
        ("📄","PDF Reports","Export a professional one-page summary report for any analysis."),
        ("🛡️","Authenticity Check","Heuristic scoring flags potentially fake or paid reviews."),
        ("🔖","Bookmarks","Save your favourite places for quick re-analysis."),
    ]
    for row_start in range(0, len(features), 4):
        cols = st.columns(4)
        for col, (icon, title, desc) in zip(cols, features[row_start:row_start+4]):
            with col:
                st.markdown(f"""
                <div class="sl-feature-card">
                    <span class="sl-feature-icon">{icon}</span>
                    <div class="sl-feature-title">{title}</div>
                    <div class="sl-feature-desc">{desc}</div>
                </div>
                """, unsafe_allow_html=True)
        if row_start + 4 < len(features):
            st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)

    st.markdown("""
    <div class="sl-footer">
        Sentiment Lens &nbsp;·&nbsp; Free: 10 analyses/month &nbsp;·&nbsp;
        VADER · TF-IDF · K-Means · Groq · SerpApi
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH PAGES
# ═══════════════════════════════════════════════════════════════════════════════
def page_auth():
    _restore_from_query_params()
    if st.session_state.get("logged_in"):
        st.rerun()

    # ── Detect Supabase OTP error in URL hash (e.g. expired email link) ──────
    # This fires when user clicks a confirmation link that is invalid or expired.
    try:
        from streamlit_js_eval import streamlit_js_eval
        url_hash = streamlit_js_eval(js_expressions="window.location.hash", key="_otp_hash_check")
        if url_hash and "error_code=otp_expired" in str(url_hash):
            st.warning(
                "⏰ **That confirmation link has expired.**\n\n"
                "Enter your email below and click **Resend confirmation email** to get a fresh link."
            )
            resend_email = st.text_input("Email address", key="_resend_email")
            if st.button("📧 Resend confirmation email", type="primary", use_container_width=True):
                if resend_email.strip():
                    ok, msg = auth_resend_confirmation(resend_email.strip())
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)
                else:
                    st.error("Please enter your email address.")
            st.markdown("---")
    except Exception:
        pass

    mode = st.session_state.get("auth_mode","login")

    st.markdown(f"""
    <div class="sl-auth-wrap">
        <div class="sl-auth-card">
            <div class="sl-auth-logo">🔍</div>
            <div class="sl-auth-title">Sentiment Lens</div>
            <div class="sl-auth-sub">{"Create your account" if mode=="register" else "Sign in to your account"}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)

        if mode == "register":
            name  = st.text_input("Name")
            email = st.text_input("Email")
            pw    = st.text_input("Password", type="password")
            pw2   = st.text_input("Confirm password", type="password")

            if st.button("Create account", type="primary", use_container_width=True):
                if not all([name.strip(), email.strip(), pw]):
                    st.error("All fields are required.")
                elif pw != pw2:
                    st.error("Passwords don't match.")
                elif len(pw) < 6:
                    st.error("Password must be at least 6 characters.")
                else:
                    ok, msg = auth_register(email.strip(), pw, name.strip())
                    if ok:
                        st.success(msg)
                        st.session_state["auth_mode"] = "login"
                        st.rerun()
                    else:
                        st.error(msg)

            st.markdown("<div style='height:.4rem'></div>", unsafe_allow_html=True)
            if st.button("Already have an account? Sign in", use_container_width=True):
                st.session_state["auth_mode"] = "login"
                st.rerun()

        else:  # login
            email = st.text_input("Email")
            pw    = st.text_input("Password", type="password")

            if st.button("Sign in", type="primary", use_container_width=True):
                if not email.strip() or not pw:
                    st.error("Enter your email and password.")
                else:
                    ok, msg, profile = auth_login(email.strip(), pw)
                    if ok:
                        uid = str(profile.get("id",""))
                        st.session_state.update({
                            "logged_in":  True,
                            "user_id":    uid,
                            "user_email": profile.get("email",""),
                            "user_name":  profile.get("name","") or email.split("@")[0],
                            "user_plan":  profile.get("plan","free"),
                        })
                        if uid:
                            try: st.query_params["uid"] = uid
                            except Exception: pass
                        st.rerun()
                    else:
                        st.error(msg)
                        # If error is "Email not confirmed", offer resend button
                        if "not confirmed" in msg.lower() or "email" in msg.lower():
                            if st.button("📧 Resend confirmation email", key="_resend_from_login"):
                                ok2, msg2 = auth_resend_confirmation(email.strip())
                                if ok2:
                                    st.success(msg2)
                                else:
                                    st.error(msg2)

            st.markdown("<div style='height:.4rem'></div>", unsafe_allow_html=True)
            if st.button("Don't have an account? Create one", use_container_width=True):
                st.session_state["auth_mode"] = "register"
                st.rerun()

        if st.button("← Back to home", use_container_width=True):
            st.session_state["auth_mode"] = "home"
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
def render_sidebar():
    dark = st.session_state.get(_THEME_KEY,"dark") == "dark"
    uid  = st.session_state.get("user_id","")

    with st.sidebar:
        # Logo + theme
        c1, c2 = st.columns([4,1])
        with c1:
            plan = st.session_state.get("user_plan","free")
            plan_badge = f'<span class="badge-{"pro" if plan=="pro" else "free"}">{plan.upper()}</span>'
            st.markdown(
                f"<div style='font-size:.98rem;font-weight:700;font-family:Space Grotesk,sans-serif'>"
                f"🔍 Sentiment Lens &nbsp;{plan_badge}</div>"
                f"<div style='font-size:.7rem;opacity:.45;margin-top:2px'>{st.session_state.get('user_email','')}</div>",
                unsafe_allow_html=True)
        with c2:
            if st.button("☀️" if dark else "🌙", key="theme_btn"):
                st.session_state[_THEME_KEY] = "light" if dark else "dark"
                st.rerun()

        # Usage meter
        if uid:
            try:
                used, limit = get_db().get_usage(uid)
                pct = min(100, int(used / max(limit,1) * 100))
                warn_col = "#EF4444" if pct >= 80 else "#6366F1"
                lim_str = "∞" if limit > 9000 else str(limit)
                st.markdown(
                    f"<div style='font-size:.68rem;color:#64748B;margin-top:.5rem'>"
                    f"Analyses: <b style='color:{warn_col}'>{used}</b> / {lim_str} this month</div>"
                    f"<div class='sl-usage-bar-bg'><div class='sl-usage-bar-fill' style='width:{pct}%'></div></div>",
                    unsafe_allow_html=True)
            except Exception:
                pass

        st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)

        nav = st.radio("nav",
            ["🔍 Analyze","📦 Products","🔖 Bookmarks","📊 Compare","📜 History"],
            label_visibility="collapsed")
        st.session_state["page"] = nav.split(" ",1)[1]
        st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)

        if st.session_state["page"] == "Analyze":
            st.markdown('<div class="sl-section">Search</div>', unsafe_allow_html=True)
            place_name  = st.text_input("Place name")
            city_input  = st.text_input("City")
            state_input = st.text_input("State")
            cntry_input = st.text_input("Country")
            max_r       = st.slider("Reviews to fetch", 20, 200, 80, step=10)

            if st.button("Search Places", type="primary", use_container_width=True):
                if not place_name.strip():
                    st.error("Enter a place name.")
                elif not _serpapi_key():
                    st.error("SERPAPI_KEY missing from .env")
                else:
                    _do_search(place_name, city_input, state_input, cntry_input, max_r)

            st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
            st.markdown('<div class="sl-section">Previously Analyzed</div>', unsafe_allow_html=True)
            try:
                places_df = get_db().get_all_places()
                if not places_df.empty:
                    sel = st.selectbox("Switch", ["—"] + places_df["name"].tolist(),
                                       label_visibility="collapsed")
                    if sel != "—":
                        row = places_df[places_df["name"] == sel].iloc[0]
                        if row["place_id"] != st.session_state.get("place_id"):
                            st.session_state.update({
                                "place_id":   row["place_id"],
                                "place_meta": row.to_dict(),
                                "df":         _load_reviews_cached(row["place_id"]),
                                "search_done":False, "candidates":[],
                                "ai_messages":[],
                            })
                            st.rerun()
                else:
                    st.caption("No places analysed yet.")
            except Exception:
                pass

        st.markdown("<div style='flex:1'></div>", unsafe_allow_html=True)
        st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)

        # Theme toggle + logout at bottom
        if st.button("⟵  Log out", use_container_width=True):
            _do_logout()

        st.markdown(
            "<div style='font-size:.62rem;opacity:.32;line-height:1.8;margin-top:.5rem'>"
            "VADER · TF-IDF · K-Means<br>Supabase · SerpApi · Groq</div>",
            unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH + PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def _load_reviews_cached(place_id: str) -> pd.DataFrame:
    return get_db().get_reviews(place_id)

def _vel_badge(direction, pts):
    abs_pts = abs(float(pts))
    if direction == "improving":
        return f'<span class="badge badge-up">↑ +{abs_pts:.1f} pts</span>'
    elif direction == "declining":
        return f'<span class="badge badge-down">↓ -{abs_pts:.1f} pts</span>'
    return f'<span class="badge badge-flat">→ stable</span>'

def _do_search(place_name, city, state, country, max_r):
    from src.ingestion.serpapi_loader import (
        search_candidates, SerpApiError,
        SerpApiKeyMissingError, SerpApiNoResultsError, SerpApiQuotaError)
    with st.spinner("Searching Google Maps…"):
        try:
            cands = search_candidates(
                name=place_name.strip(), city=city.strip(),
                state=state.strip(), country=country.strip(),
                api_key=_serpapi_key(), max_candidates=5)
            st.session_state.update({"candidates":cands,"max_reviews":max_r,"search_done":True})
            st.rerun()
        except (SerpApiKeyMissingError, SerpApiNoResultsError, SerpApiQuotaError, SerpApiError) as e:
            st.error(str(e))

def run_pipeline(candidate: dict, max_reviews: int = 80) -> tuple[bool, str]:
    """Full analysis pipeline with usage limit check."""
    uid = st.session_state.get("user_id")

    # Check usage limit
    if uid:
        allowed, used, limit = get_db().check_and_increment_usage(uid)
        if not allowed:
            lim = str(limit) if limit < 9000 else "∞"
            return False, (
                f"Monthly limit reached ({used}/{lim} analyses). "
                "Upgrade to Pro for unlimited analyses."
            )

    from src.ingestion.serpapi_loader import (
        fetch_reviews_for_place, SerpApiError, SerpApiNoResultsError, SerpApiQuotaError)
    from src.analysis.sentiment    import analyze_sentiment, get_summary_stats
    from src.analysis.authenticity import analyze_authenticity, get_trust_score
    from src.analysis.themes       import cluster_reviews

    try:
        with st.spinner(f"📥 Fetching reviews for **{candidate['name']}**…"):
            df, meta = fetch_reviews_for_place(candidate, _serpapi_key(), max_reviews)
    except (SerpApiNoResultsError, SerpApiQuotaError, SerpApiError) as e:
        # Track failed search attempts
        try:
            from src.auth.tracker import get_ip_from_headers
            get_db().log_user_query(
                user_id=uid,
                query_text=str(candidate.get("name", "")),
                query_type="place",
                session_ip=get_ip_from_headers(),
                status="error",
                error_message=str(e),
            )
        except Exception:
            pass
        return False, str(e)
    except Exception as e:
        return False, f"Fetch error: {e}"

    try:
        with st.spinner("🧠 Analysing sentiment…"):
            df = analyze_sentiment(df)
        with st.spinner("🔎 Checking authenticity…"):
            df = analyze_authenticity(df)
        with st.spinner("📊 Clustering topics…"):
            df = cluster_reviews(df)

        stats = get_summary_stats(df)
        trust = get_trust_score(df)
        db    = get_db()

        with st.spinner("💾 Saving…"):
            db.upsert_place({**meta,
                "reviews_fetched": int(len(df)),
                "avg_sentiment":   float(stats["avg_compound"]),
                "pct_positive":    float(stats["pct_positive"]),
                "pct_negative":    float(stats["pct_negative"]),
                "trust_score":     float(trust),
            })
            db.delete_place_reviews(meta["place_id"])
            db.upsert_reviews(df)
            db.log_search({
                "place_name":    str(meta["name"]),
                "place_id":      str(meta["place_id"]),
                "category":      str(meta.get("category","")),
                "reviews_count": int(len(df)),
                "avg_sentiment": float(stats["avg_compound"]),
                "avg_rating":    float(stats["avg_rating"]),
                "pct_positive":  float(stats["pct_positive"]),
                "trust_score":   float(trust),
                "user_id":       uid,
            })
            # Granular query tracking — records what user searched and result
            try:
                from src.auth.tracker import get_ip_from_headers
                db.log_user_query(
                    user_id=uid,
                    query_text=str(candidate.get("name", "")),
                    query_type="place",
                    result_place_id=str(meta["place_id"]),
                    result_name=str(meta["name"]),
                    result_count=int(len(df)),
                    session_ip=get_ip_from_headers(),
                    status="success",
                )
            except Exception:
                pass
            if uid: db.touch_user(uid)

        _load_reviews_cached.clear()
        st.session_state.update({
            "place_id": meta["place_id"], "place_meta": meta, "df": df,
            "search_done": False, "candidates": [], "ai_messages": [],
        })
        return True, f"Analysed **{meta['name']}** — {len(df)} reviews"
    except Exception as e:
        return False, f"Analysis error: {e}"


# ═══════════════════════════════════════════════════════════════════════════════
# GROQ AI
# ═══════════════════════════════════════════════════════════════════════════════
GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

def _groq(messages: list, max_tokens: int = 350) -> str | None:
    key = _groq_key()
    if not key: return None
    try:
        r = _req.post(GROQ_URL,
            headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},
            json={"model":GROQ_MODEL,"max_tokens":max_tokens,"messages":messages},
            timeout=30)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"[Error: {e}]"

def _ai_context(df: pd.DataFrame, meta: dict) -> str:
    if df.empty: return "No place analysed yet."
    try:
        from src.analysis.sentiment import get_summary_stats, compute_velocity
        from src.analysis.themes    import get_sentiment_keywords, get_aspect_sentiment
        s  = get_summary_stats(df)
        v  = compute_velocity(df)
        kw = get_sentiment_keywords(df, top_n=5)
        praise = ", ".join([k for k,_ in kw.get("positive",[])[:5]]) or "n/a"
        comps  = ", ".join([k for k,_ in kw.get("negative",[])[:5]]) or "n/a"
        asp_df = get_aspect_sentiment(df)
        asp = ""
        if not asp_df.empty:
            asp = " | Aspects: " + " / ".join(
                f"{r['aspect']}={r['sentiment_label']}({float(r['pct_positive']):.0f}%pos)"
                for _,r in asp_df.iterrows())
        return (
            f"Place:{meta.get('name','?')} | Cat:{meta.get('category','')} | "
            f"Reviews:{s['total']} | Rating:{float(s['avg_rating']):.1f}/5 | "
            f"Positive:{float(s['pct_positive']):.0f}% | Negative:{float(s['pct_negative']):.0f}% | "
            f"Score:{float(s['avg_compound']):.3f} | Trend:{v['direction']}({float(v.get('delta',0)):+.3f}) | "
            f"Praise:{praise} | Complaints:{comps}{asp}"
        )
    except Exception:
        return f"Place:{meta.get('name','?')} | Reviews:{len(df)}"

def _groq_summary(name, stats, kw, aspect_df) -> str | None:
    praise = ", ".join([k for k,_ in kw.get("positive",[])[:5]])
    comp   = ", ".join([k for k,_ in kw.get("negative",[])[:5]])
    asp    = ""
    if aspect_df is not None and not aspect_df.empty:
        asp = "; ".join(f"{r['aspect']}({float(r['pct_positive']):.0f}%pos)"
                        for _,r in aspect_df.iterrows())
    return _groq([{"role":"user","content":(
        f"Business analyst: write a 3-paragraph executive summary.\n"
        f"Place:{name} | Rating:{float(stats.get('avg_rating',0)):.1f}/5 | "
        f"Reviews:{stats['total']} | "
        f"Positive:{float(stats['pct_positive']):.0f}% | Negative:{float(stats['pct_negative']):.0f}% | "
        f"Score:{float(stats['avg_compound']):.3f}\n"
        f"Praise:{praise or 'n/a'} | Complaints:{comp or 'n/a'} | Aspects:{asp or 'n/a'}\n"
        "Para 1:reputation. Para 2:strengths. Para 3:improvements. "
        "Use specific numbers. Never say 'it is difficult to determine'. 3 paragraphs, no bullets."
    )}], max_tokens=600)


# ═══════════════════════════════════════════════════════════════════════════════
# AUTO INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════
def auto_insights(df, stats, velocity, trust):
    ins = []
    ppos = float(stats["pct_positive"])
    pneg = float(stats["pct_negative"])
    delta = float(velocity.get("delta",0))
    d = velocity.get("direction","stable")
    pts = abs(float(velocity.get("pct_change",0)))

    if ppos >= 75:
        ins.append({"icon":"✅","type":"Strength",
            "text":f"Strong positive sentiment at {ppos:.0f}% of {stats['total']} reviews — genuine customer approval."})
    elif pneg >= 30:
        ins.append({"icon":"⚠️","type":"Concern",
            "text":f"High negative rate ({pneg:.0f}%) across {stats['total']} reviews signals recurring issues."})
    if d == "declining" and abs(delta) > 0.05:
        ins.append({"icon":"📉","type":"Trend Alert",
            "text":f"Sentiment score dropped {pts:.1f} points vs prior 30 days. Check recent reviews for cause."})
    elif d == "improving" and abs(delta) > 0.05:
        ins.append({"icon":"📈","type":"Positive Trend",
            "text":f"Sentiment improved {pts:.1f} points vs prior 30 days — momentum is building."})
    if float(trust) < 80:
        sus = int(df["is_suspicious"].sum()) if "is_suspicious" in df.columns else 0
        ins.append({"icon":"🚩","type":"Authenticity Risk",
            "text":f"{sus} of {stats['total']} reviews ({100-float(trust):.0f}%) flagged as potentially inauthentic."})
    elif float(trust) >= 95:
        ins.append({"icon":"🛡️","type":"High Trust",
            "text":f"{float(trust):.0f}% authenticity — strong, reliable signal corpus."})
    if "has_owner_response" in df.columns:
        rate = 100.0 * df["has_owner_response"].sum() / max(len(df),1)
        if rate < 10 and pneg > 20:
            ins.append({"icon":"💬","type":"Engagement Gap",
                "text":f"Only {rate:.0f}% owner response rate despite {pneg:.0f}% negative reviews. Responding publicly helps."})
        elif rate > 40:
            ins.append({"icon":"👍","type":"Active Owner",
                "text":f"Owner responds to {rate:.0f}% of reviews — strong engagement that builds trust."})
    avg_r = float(stats.get("avg_rating",0) or 0)
    avg_s = float(stats.get("avg_compound",0) or 0)
    if avg_r >= 4.0 and avg_s < 0.1:
        ins.append({"icon":"🔀","type":"Rating/Text Gap",
            "text":"High star ratings but low text sentiment — customers rate generously but write cautiously."})
    return ins


# ═══════════════════════════════════════════════════════════════════════════════
# CANDIDATE PICKER
# ═══════════════════════════════════════════════════════════════════════════════
def render_candidate_picker():
    candidates  = st.session_state.get("candidates",[])
    max_reviews = st.session_state.get("max_reviews",80)
    st.markdown("### 📍 Select the correct place")
    st.markdown(f"Found **{len(candidates)}** result(s). Click **Analyze** next to the correct one.")
    for i, c in enumerate(candidates):
        name     = str(c.get("name") or "Unknown")
        category = c.get("category") or ""
        if isinstance(category, list): category = ", ".join(str(x) for x in category)
        address  = str(c.get("address") or "No address")
        rating   = c.get("rating")
        reviews  = c.get("reviews")
        query    = str(c.get("matched_query") or "")
        cat_html = f"<span class='badge-cat'>{category.strip()}</span>" if category.strip() else ""
        meta_p   = []
        if rating:  meta_p.append(f"⭐ {rating}")
        if reviews: meta_p.append(f"{int(reviews):,} Google reviews")
        meta_h = (f"<div style='font-size:.79rem;color:#6366F1;margin-top:4px'>{'  ·  '.join(meta_p)}</div>"
                  if meta_p else "")
        c1, c2 = st.columns([5,1])
        with c1:
            st.markdown(
                f"<div class='sl-card'>"
                f"<div style='display:flex;align-items:center;gap:10px;flex-wrap:wrap'>"
                f"<span style='font-size:.98rem;font-weight:600'>{name}</span>{cat_html}</div>"
                f"<div style='font-size:.81rem;opacity:.55;margin-top:4px'>📍 {address}</div>"
                f"{meta_h}"
                f"<div style='font-size:.68rem;opacity:.35;margin-top:4px'>Matched: <code>{query}</code></div>"
                f"</div>", unsafe_allow_html=True)
        with c2:
            st.markdown("<div style='margin-top:.9rem'></div>", unsafe_allow_html=True)
            if st.button("Analyze", key=f"pick_{i}", type="primary"):
                ok, msg = run_pipeline(c, max_reviews)
                if ok:
                    st.success(msg); st.rerun()
                else:
                    st.error(msg)
    st.markdown("---")
    if st.button("← Back to search"):
        st.session_state.update({"search_done":False,"candidates":[]})
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# AI CHAT PANEL
# ═══════════════════════════════════════════════════════════════════════════════
SUGGESTIONS = [
    "What are the top 3 things customers love?",
    "What's the biggest recurring complaint?",
    "Is the quality trending up or down recently?",
]

def render_ai_chat(df, meta):
    st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
    st.markdown("### 🤖 Ask AI about this place")
    if not _groq_key():
        st.markdown('<div class="sl-info">Add <code>GROK_KEY</code> to .env to enable AI chat.</div>',
                    unsafe_allow_html=True)
        return
    ctx = _ai_context(df, meta)
    sys_msg = (
        "You are a sharp Google Maps review analyst. "
        "Use exact numbers from the data provided. "
        "Never say 'it is difficult to determine' or 'further analysis needed'. "
        "Give a concrete, specific, data-backed answer in exactly 3-4 sentences. "
        f"Data: {ctx}"
    )
    s1,s2,s3 = st.columns(3)
    for col, q in zip([s1,s2,s3], SUGGESTIONS):
        with col:
            if st.button(q, key=f"sugg_{hash(q)}", use_container_width=True):
                _send_ai(q, sys_msg)
    for msg in st.session_state.get("ai_messages",[]):
        is_ai = msg["role"]=="assistant"
        col   = "#6366F1" if is_ai else "transparent"
        icon  = "🤖" if is_ai else "👤"
        lbl   = "AI" if is_ai else "You"
        st.markdown(
            f"<div class='sl-card' style='border-left:3px solid {col};margin:.3rem 0'>"
            f"<span style='font-size:.68rem;opacity:.45'>{icon} {lbl}</span><br>"
            f"<span style='font-size:.86rem'>{msg['content']}</span></div>",
            unsafe_allow_html=True)
    user_q = st.chat_input("Ask anything about this place's reviews…", key="ai_inp")
    if user_q: _send_ai(user_q, sys_msg)

def _send_ai(question, system):
    msgs = st.session_state.get("ai_messages",[])
    msgs.append({"role":"user","content":question})
    history = [{"role":"system","content":system}]
    history += [{"role":m["role"],"content":m["content"]} for m in msgs[-6:]]
    with st.spinner("Thinking…"):
        ans = _groq(history, max_tokens=300)
    if ans: msgs.append({"role":"assistant","content":ans})
    st.session_state["ai_messages"] = msgs
    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYZE PAGE
# ═══════════════════════════════════════════════════════════════════════════════
def page_analyze():
    if st.session_state.get("search_done") and st.session_state.get("candidates"):
        render_candidate_picker()
        return

    if not st.session_state.get("place_id"):
        st.markdown("## 🔍 Google Maps Analyzer")
        st.markdown("""<div class="sl-info">
            Enter a place name in the sidebar and click <b>Search Places</b>.<br>
            Works for restaurants, hotels, hospitals, shops, museums — any Google Maps place.
        </div>""", unsafe_allow_html=True)
        return

    df   = st.session_state["df"]
    meta = st.session_state["place_meta"]
    if df.empty:
        df = _load_reviews_cached(st.session_state["place_id"])
        st.session_state["df"] = df
    if df.empty:
        st.warning("No reviews found. Try re-analysing."); return

    for col in ["rating","sentiment_score","sentiment_pos","sentiment_neg",
                "sentiment_neu","suspicion_score","word_count","char_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    from src.analysis.sentiment    import get_summary_stats, compute_velocity
    from src.analysis.authenticity import get_trust_score
    from src.analysis.themes       import get_sentiment_keywords, get_aspect_sentiment, get_cluster_summary
    from src.visualization.charts  import (
        sentiment_gauge, sentiment_donut, rating_distribution,
        sentiment_over_time, monthly_volume, keyword_bars,
        aspect_radar, aspect_bar, rating_vs_sentiment,
        topic_cluster_chart, suspicion_chart, owner_response_chart,
        review_length_chart, sentiment_by_weekday, rating_over_time,
    )

    stats    = get_summary_stats(df)
    velocity = compute_velocity(df)
    trust    = get_trust_score(df)
    name     = str(meta.get("name") or "Unknown")
    category = meta.get("category") or ""
    if isinstance(category, list): category = ", ".join(str(x) for x in category)
    address  = str(meta.get("address") or "")
    rating_v = meta.get("overall_rating") or meta.get("rating")

    cat_html = f"<span class='badge-cat'>{category.strip()}</span>" if category.strip() else ""
    vel_html = _vel_badge(velocity["direction"], velocity["pct_change"])

    # Bookmark button
    uid = st.session_state.get("user_id","")
    pid = str(meta.get("place_id",""))
    is_bm = False
    if uid and pid:
        try: is_bm = get_db().is_bookmarked(uid, pid)
        except Exception: pass

    bm_col, head_col = st.columns([0.08, 0.92])
    with bm_col:
        bm_icon = "🔖" if is_bm else "📌"
        if st.button(bm_icon, key="bm_btn", help="Toggle bookmark"):
            try:
                result = get_db().toggle_bookmark(uid, pid, name)
                st.toast("Bookmarked ✓" if result else "Bookmark removed")
                st.rerun()
            except Exception: pass
    with head_col:
        st.markdown(
            f"<div class='sl-place-header'>"
            f"<div style='display:flex;align-items:center;gap:12px;flex-wrap:wrap'>"
            f"<span class='sl-place-name'>{name}</span>{cat_html}{vel_html}</div>"
            f"<div class='sl-place-meta'>{'⭐ '+str(rating_v)+'  ·  ' if rating_v else ''}"
            f"{address}{'  ·  '+str(len(df))+' reviews' if not df.empty else ''}</div>"
            f"</div>", unsafe_allow_html=True)

    # KPIs
    m1,m2,m3,m4,m5,m6 = st.columns(6)
    m1.metric("Reviews",   f"{stats['total']:,}")
    m2.metric("Avg Rating",f'{float(stats["avg_rating"]):.2f} ★' if stats["avg_rating"] else "—")
    m3.metric("Positive",  f'{float(stats["pct_positive"]):.1f}%',
              delta=f'+{float(stats["pct_positive"])-50:.1f}%', delta_color="normal")
    m4.metric("Negative",  f'{float(stats["pct_negative"]):.1f}%',
              delta=f'{50-float(stats["pct_negative"]):.1f}%', delta_color="inverse")
    m5.metric("Sentiment", f'{float(stats["avg_compound"]):+.3f}')
    m6.metric("Trust",     f'{float(trust):.0f}%',
              delta="authentic" if float(trust)>=90 else "check",
              delta_color="normal" if float(trust)>=90 else "off")
    st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)

    t_ov,t_in,t_tr,t_rv,t_th = st.tabs(
        ["📊 Overview","🤖 Insights","📈 Trends","💬 Reviews","🔑 Themes"])

    # OVERVIEW ────────────────────────────────────────────────────────────────
    with t_ov:
        c1,c2 = st.columns([1.2,0.8])
        with c1: st.plotly_chart(sentiment_donut(df), width='stretch')
        with c2:
            st.markdown("<div style='padding-top:.5rem'></div>", unsafe_allow_html=True)
            st.plotly_chart(sentiment_gauge(float(stats["avg_compound"])), width='stretch')
        st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)
        c3,c4 = st.columns(2)
        with c3: st.plotly_chart(rating_distribution(df), width='stretch')
        with c4: st.plotly_chart(rating_vs_sentiment(df), width='stretch')
        st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)
        c5,c6 = st.columns(2)
        with c5: st.plotly_chart(review_length_chart(df), width='stretch')
        with c6:
            if "has_owner_response" in df.columns:
                rate = 100.0*df["has_owner_response"].sum()/max(len(df),1)
                r1,r2 = st.columns(2)
                r1.metric("Response Rate", f"{rate:.0f}%")
                r2.metric("Responses", int(df["has_owner_response"].sum()))
                st.plotly_chart(owner_response_chart(df), width='stretch')

        # PDF export
        st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
        kw_pdf   = get_sentiment_keywords(df, top_n=8)
        asp_pdf  = get_aspect_sentiment(df)
        if st.button("📄 Export PDF Report", use_container_width=True):
            with st.spinner("Generating PDF…"):
                try:
                    from src.export.pdf_report import generate_pdf_report
                    pdf = generate_pdf_report(
                        name, stats, velocity, float(trust), asp_pdf,
                        kw_pdf.get("positive",[]), kw_pdf.get("negative",[]), "place")
                    st.download_button(
                        "⬇️ Download PDF Report",
                        data=pdf,
                        file_name=f"{name.replace(' ','_')}_report.pdf",
                        mime="application/pdf",
                    )
                except Exception as e:
                    st.error(f"PDF error: {e}")

    # INSIGHTS ────────────────────────────────────────────────────────────────
    with t_in:
        kw2     = get_sentiment_keywords(df, top_n=8)
        aspect2 = get_aspect_sentiment(df)
        ins     = auto_insights(df, stats, velocity, trust)

        if ins:
            st.markdown('<div class="sl-section">Automated Insights</div>', unsafe_allow_html=True)
            for i in ins:
                st.markdown(
                    f"<div class='sl-card'><div class='sl-card-title'>{i['icon']} {i['type']}</div>"
                    f"<div class='sl-card-body'>{i['text']}</div></div>",
                    unsafe_allow_html=True)

        if "sentiment_score" in df.columns:
            st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
            import plotly.graph_objects as go
            from src.visualization.charts import _layout, _ax, COLOR_PRIMARY
            fig_h = go.Figure(go.Histogram(
                x=df["sentiment_score"].astype(float), nbinsx=30,
                marker_color=COLOR_PRIMARY, opacity=0.72,
                hovertemplate="Score: %{x:.2f}<br>Count: %{y}<extra></extra>"))
            fig_h.update_layout(**_layout(
                title=dict(text="Score Distribution", font=dict(size=13), x=0),
                xaxis=_ax(title="VADER Score"), yaxis=_ax(title="Reviews"),
                height=210, showlegend=False))
            st.plotly_chart(fig_h, width='stretch')

        st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sl-section">AI Executive Summary</div>', unsafe_allow_html=True)
        if _groq_key():
            if st.button("Generate AI Summary", type="primary"):
                with st.spinner("Generating…"):
                    s = _groq_summary(name, stats, kw2, aspect2)
                if s:
                    st.markdown(
                        f"<div class='sl-card'><div class='sl-card-title'>🤖 AI Summary</div>"
                        f"<div class='sl-card-body'>{s.replace(chr(10),'<br>')}</div></div>",
                        unsafe_allow_html=True)
        else:
            st.markdown('<div class="sl-info">Add <code>GROK_KEY</code> to .env for AI summaries.</div>',
                        unsafe_allow_html=True)

        if not aspect2.empty:
            st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
            st.markdown('<div class="sl-section">Aspect Summary</div>', unsafe_allow_html=True)
            from src.config import COLOR_POSITIVE as CP, COLOR_NEGATIVE as CN, COLOR_NEUTRAL as CNT
            for _, row in aspect2.iterrows():
                icon = "✅" if row["sentiment_label"]=="Positive" else "❌" if row["sentiment_label"]=="Negative" else "➖"
                col  = CP if row["sentiment_label"]=="Positive" else CN if row["sentiment_label"]=="Negative" else CNT
                st.markdown(
                    f"<div class='sl-card' style='display:flex;justify-content:space-between;padding:.6rem 1rem'>"
                    f"<span>{icon} <b>{row['aspect']}</b></span>"
                    f"<span style='font-size:.79rem;opacity:.7'>{row['mention_count']} mentions &nbsp;·&nbsp;"
                    f" {float(row['pct_positive']):.0f}% positive &nbsp;·&nbsp;"
                    f" <span style='color:{col};font-weight:600'>{float(row['avg_sentiment']):+.3f}</span></span>"
                    f"</div>", unsafe_allow_html=True)

        render_ai_chat(df, meta)

    # TRENDS ──────────────────────────────────────────────────────────────────
    with t_tr:
        v = velocity
        vc,vd = st.columns([1,3])
        with vc:
            st.metric("30-Day Change", f'{float(v.get("recent_avg",0)):+.3f}',
                      delta=f'{float(v.get("pct_change",0)):+.1f} pts vs prior',
                      delta_color="normal" if v["direction"]!="declining" else "inverse",
                      help="Score points changed on [-100,+100] scale vs prior 30-day window.")
        with vd:
            st.markdown(
                f"<div class='sl-card'><div class='sl-card-title'>Velocity</div>"
                f"<div class='sl-card-body'>Recent 30d: <b>{float(v.get('recent_avg',0)):+.3f}</b>"
                f" &nbsp;|&nbsp; Prior 30d: <b>{float(v.get('previous_avg',0)):+.3f}</b>"
                f" &nbsp;|&nbsp; Delta: <b>{float(v.get('delta',0)):+.3f}</b>"
                f" &nbsp;|&nbsp; Trend: <b>{v['direction'].capitalize()}</b></div></div>",
                unsafe_allow_html=True)
        st.plotly_chart(sentiment_over_time(df), width='stretch')
        c1,c2 = st.columns(2)
        with c1: st.plotly_chart(monthly_volume(df),      width='stretch')
        with c2: st.plotly_chart(rating_over_time(df),    width='stretch')
        st.plotly_chart(sentiment_by_weekday(df), width='stretch')

    # REVIEWS ─────────────────────────────────────────────────────────────────
    with t_rv:
        fc1,fc2,fc3,fc4 = st.columns([2,1,1,1])
        with fc1: sfilt  = st.multiselect("Sentiment",["Positive","Neutral","Negative"],default=["Positive","Neutral","Negative"])
        with fc2: rfilt  = st.multiselect("Rating",[5,4,3,2,1],default=[5,4,3,2,1])
        with fc3: sufilt = st.selectbox("Authenticity",["All","Clean only","Suspicious only"])
        with fc4: sort   = st.selectbox("Sort",["Newest","Oldest","Rating ↓","Rating ↑","Sentiment ↓","Sentiment ↑"])
        fdf = df.copy()
        if sfilt and "sentiment_label" in fdf.columns:
            fdf = fdf[fdf["sentiment_label"].isin(sfilt)]
        if rfilt and "rating" in fdf.columns:
            try: fdf = fdf[fdf["rating"].dropna().astype(int).isin(rfilt)]
            except Exception: pass
        if sufilt == "Clean only"        and "is_suspicious" in fdf.columns: fdf = fdf[~fdf["is_suspicious"]]
        elif sufilt == "Suspicious only" and "is_suspicious" in fdf.columns: fdf = fdf[fdf["is_suspicious"]]
        smap = {"Newest":("review_date",False),"Oldest":("review_date",True),
                "Rating ↓":("rating",False),"Rating ↑":("rating",True),
                "Sentiment ↓":("sentiment_score",False),"Sentiment ↑":("sentiment_score",True)}
        sc,sa = smap[sort]
        if sc in fdf.columns: fdf = fdf.sort_values(sc, ascending=sa)
        st.markdown(f"**{len(fdf)}** of **{len(df)}** reviews shown")
        show = ["review_date","rating","sentiment_label","sentiment_score",
                "is_suspicious","review_text","author","has_owner_response"]
        disp = fdf[[c for c in show if c in fdf.columns]].rename(columns={
            "review_date":"Date","rating":"★","sentiment_label":"Sentiment",
            "sentiment_score":"Score","is_suspicious":"🚩","review_text":"Review",
            "author":"Author","has_owner_response":"Reply"})
        def _hl(val):
            c = {"Positive":"rgba(16,185,129,.10)","Negative":"rgba(239,68,68,.10)"}.get(val,"")
            return f"background-color:{c}" if c else ""
        try:
            st.dataframe(disp.style.map(_hl,subset=["Sentiment"]) if "Sentiment" in disp.columns else disp,
                         width='stretch', height=420)
        except Exception:
            st.dataframe(disp, width='stretch', height=420)
        if "is_suspicious" in df.columns and df["is_suspicious"].any():
            with st.expander(f"🔎 {int(df['is_suspicious'].sum())} flagged reviews"):
                st.plotly_chart(suspicion_chart(df), width='stretch')
                st.dataframe(df[df["is_suspicious"]][["author","rating","review_text","suspicion_score","suspicion_reasons"]],
                             width='stretch')
        st.download_button("⬇️ Export CSV",
            fdf.to_csv(index=False).encode(),
            f"{name.replace(' ','_')}_reviews.csv","text/csv")

    # THEMES ──────────────────────────────────────────────────────────────────
    with t_th:
        cluster_df = get_cluster_summary(df)
        st.plotly_chart(topic_cluster_chart(cluster_df), width='stretch')
        if not cluster_df.empty:
            from src.config import COLOR_POSITIVE as CP, COLOR_NEGATIVE as CN, COLOR_NEUTRAL as CNT
            for _, row in cluster_df.iterrows():
                col = CP if float(row["avg_sentiment"])>=0.05 else CN if float(row["avg_sentiment"])<=-0.05 else CNT
                st.markdown(
                    f"<div class='sl-card'>"
                    f"<div style='display:flex;justify-content:space-between;align-items:center'>"
                    f"<span class='sl-card-title'>{row['topic']}</span>"
                    f"<span style='font-size:.74rem;color:{col};font-weight:600'>"
                    f"{float(row['pct_positive']):.0f}% positive · {float(row['avg_sentiment']):+.3f}</span></div>"
                    f"<div style='font-size:.81rem;opacity:.5;margin-top:3px'>{row['review_count']} reviews</div>"
                    f"</div>", unsafe_allow_html=True)
        st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
        with st.spinner("Extracting keywords…"):
            kw = get_sentiment_keywords(df, top_n=14)
        kc1,kc2 = st.columns(2)
        with kc1: st.plotly_chart(keyword_bars(kw.get("positive",[]),color="#10B981",title="What Customers Praise"), width='stretch')
        with kc2: st.plotly_chart(keyword_bars(kw.get("negative",[]),color="#EF4444",title="What Customers Criticise"), width='stretch')
        st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
        aspect_df = get_aspect_sentiment(df)
        if not aspect_df.empty:
            ac1,ac2 = st.columns(2)
            with ac1: st.plotly_chart(aspect_radar(aspect_df), width='stretch')
            with ac2: st.plotly_chart(aspect_bar(aspect_df),   width='stretch')


# ═══════════════════════════════════════════════════════════════════════════════
# PRODUCT REVIEWS PAGE
# ═══════════════════════════════════════════════════════════════════════════════
def page_products():
    st.markdown("## 📦 Product Review Analyzer")
    st.markdown("""<div class="sl-info">
        Paste product reviews from <b>Flipkart, Meesho, Amazon, Nykaa</b> — any platform.<br>
        One review per line. Optionally start with a rating: <code>4/5: Great product!</code>
    </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    c1,c2,c3 = st.columns([2,1,1])
    with c1: product_name = st.text_input("Product name")
    with c2: platform     = st.selectbox("Platform", ["Flipkart","Meesho","Amazon","Nykaa","Myntra","Other"])
    with c3: product_url  = st.text_input("Product URL (optional)")

    raw_text = st.text_area(
        "Paste reviews here — one per line",
        height=220,
        placeholder=(
            "Excellent product, loved the quality!\n"
            "5/5: Amazing value for money\n"
            "★★★ Decent but could be better\n"
            "2/5: Poor packaging, took 2 weeks to arrive\n"
            "Very happy with the purchase!"
        )
    )

    if st.button("Analyse Reviews", type="primary", use_container_width=True):
        from src.ingestion.product_loader import parse_pasted_reviews, validate_paste

        # Usage limit check
        uid = st.session_state.get("user_id","")
        if uid:
            allowed, used, limit = get_db().check_and_increment_usage(uid)
            if not allowed:
                st.error(f"Monthly limit reached ({used}/{limit}). Upgrade to Pro for unlimited.")
                return

        valid, msg = validate_paste(raw_text)
        if not valid:
            st.error(msg)
            return
        if not product_name.strip():
            st.error("Enter a product name.")
            return

        with st.spinner("Parsing and analysing reviews…"):
            df = parse_pasted_reviews(raw_text, platform)
            if df.empty:
                st.error("Could not parse any reviews. Check the format.")
                return

            from src.analysis.sentiment    import analyze_sentiment
            from src.analysis.authenticity import analyze_authenticity
            from src.analysis.themes       import cluster_reviews

            df = analyze_sentiment(df)
            df = analyze_authenticity(df)
            df = cluster_reviews(df)

            # Save to DB
            try:
                analysis_id = get_db().save_product_analysis(
                    uid, product_name.strip(), platform, product_url.strip(), df)
            except Exception:
                analysis_id = None

            st.session_state["prod_df"]   = df
            st.session_state["prod_name"] = product_name.strip()

        st.success(f"Analysed {len(df)} reviews for **{product_name}**")

    # Show results
    df   = st.session_state.get("prod_df", pd.DataFrame())
    pname = st.session_state.get("prod_name","")

    if df.empty or not pname:
        return

    for col in ["rating","sentiment_score","suspicion_score","word_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    from src.analysis.sentiment    import get_summary_stats
    from src.analysis.authenticity import get_trust_score
    from src.analysis.themes       import get_sentiment_keywords, get_aspect_sentiment, get_cluster_summary
    from src.visualization.charts  import (
        sentiment_donut, rating_distribution, keyword_bars,
        aspect_radar, topic_cluster_chart, sentiment_gauge, review_length_chart
    )

    stats = get_summary_stats(df)
    trust = get_trust_score(df)

    st.markdown("---")
    st.markdown(f"### Results: {pname} ({platform})")

    m1,m2,m3,m4,m5 = st.columns(5)
    m1.metric("Reviews",  f"{stats['total']:,}")
    m2.metric("Positive", f'{float(stats["pct_positive"]):.1f}%')
    m3.metric("Negative", f'{float(stats["pct_negative"]):.1f}%')
    m4.metric("Sentiment",f'{float(stats["avg_compound"]):+.3f}')
    m5.metric("Trust",    f'{float(trust):.0f}%')

    c1,c2 = st.columns(2)
    with c1: st.plotly_chart(sentiment_donut(df),  width='stretch')
    with c2: st.plotly_chart(sentiment_gauge(float(stats["avg_compound"])), width='stretch')

    if "rating" in df.columns and df["rating"].notna().any():
        c3,c4 = st.columns(2)
        with c3: st.plotly_chart(rating_distribution(df), width='stretch')
        with c4: st.plotly_chart(review_length_chart(df), width='stretch')

    with st.spinner("Extracting keywords…"):
        kw = get_sentiment_keywords(df, top_n=10)
    kc1,kc2 = st.columns(2)
    with kc1: st.plotly_chart(keyword_bars(kw.get("positive",[]),color="#10B981",title="Praised aspects"), width='stretch')
    with kc2: st.plotly_chart(keyword_bars(kw.get("negative",[]),color="#EF4444",title="Criticised aspects"), width='stretch')

    asp = get_aspect_sentiment(df)
    if not asp.empty:
        st.plotly_chart(aspect_radar(asp), width='stretch')

    cdf = get_cluster_summary(df)
    if not cdf.empty:
        st.plotly_chart(topic_cluster_chart(cdf), width='stretch')

    # PDF export
    st.markdown('<div class="sl-divider"></div>', unsafe_allow_html=True)
    kw_pdf  = get_sentiment_keywords(df, top_n=8)
    asp_pdf = get_aspect_sentiment(df)
    if st.button("📄 Export PDF Report", key="prod_pdf", use_container_width=True):
        from src.export.pdf_report import generate_pdf_report
        pdf = generate_pdf_report(
            pname, stats, {}, float(trust), asp_pdf,
            kw_pdf.get("positive",[]), kw_pdf.get("negative",[]), "product")
        st.download_button("⬇️ Download PDF",
            data=pdf, file_name=f"{pname.replace(' ','_')}_report.pdf",
            mime="application/pdf")

    st.download_button("⬇️ Export Reviews CSV",
        df.to_csv(index=False).encode(),
        f"{pname.replace(' ','_')}_reviews.csv","text/csv")


# ═══════════════════════════════════════════════════════════════════════════════
# BOOKMARKS PAGE
# ═══════════════════════════════════════════════════════════════════════════════
def page_bookmarks():
    st.markdown("## 🔖 Bookmarks")
    uid = st.session_state.get("user_id","")
    if not uid: st.info("Log in to see bookmarks."); return

    bm = get_db().get_bookmarks(uid)
    if bm.empty:
        st.info("No bookmarks yet. Click the 📌 button on any place to bookmark it.")
        return

    for _,row in bm.iterrows():
        c1,c2 = st.columns([5,1])
        with c1:
            sent_str = f"{float(row.get('avg_sentiment',0)):+.3f}" if row.get("avg_sentiment") is not None else "—"
            star_str = f"⭐ {float(row.get('overall_rating',0)):.1f}" if row.get("overall_rating") else ""
            st.markdown(
                f"<div class='sl-card'>"
                f"<span style='font-size:.95rem;font-weight:600'>{row.get('place_name','')}</span><br>"
                f"<span style='font-size:.79rem;opacity:.55'>{star_str}  ·  Sentiment {sent_str}</span>"
                f"</div>", unsafe_allow_html=True)
        with c2:
            st.markdown("<div style='margin-top:.9rem'></div>", unsafe_allow_html=True)
            if st.button("Load", key=f"bm_load_{row.get('place_id','')}", use_container_width=True):
                pid = str(row.get("place_id",""))
                places = get_db().get_all_places()
                if not places.empty:
                    match = places[places["place_id"] == pid]
                    if not match.empty:
                        st.session_state.update({
                            "place_id":   pid,
                            "place_meta": match.iloc[0].to_dict(),
                            "df":         _load_reviews_cached(pid),
                            "page":       "Analyze",
                        })
                        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# COMPARE / HISTORY
# ═══════════════════════════════════════════════════════════════════════════════
def page_compare():
    st.markdown("## 📊 Compare Places")
    all_p = get_db().get_all_places()
    if all_p.empty or len(all_p) < 2: st.info("Analyse at least 2 places first."); return
    sel = st.multiselect("Select 2–5 places", all_p["name"].tolist(), max_selections=5)
    if len(sel) < 2: st.caption("Select 2 or more."); return
    sel_rows = all_p[all_p["name"].isin(sel)]
    try:   summ = get_db().get_place_summaries(sel_rows["place_id"].tolist())
    except Exception: summ = sel_rows.copy()
    if summ.empty: st.warning("Could not load data."); return
    from src.visualization.charts import comparison_radar, sentiment_donut
    dc = [c for c in ["name","avg_rating","avg_sentiment","total_reviews","positive_count","negative_count","trust_score"] if c in summ.columns]
    st.dataframe(summ[dc].rename(columns={"name":"Place","avg_rating":"Avg ★","avg_sentiment":"Sentiment",
        "total_reviews":"Reviews","positive_count":"Positive","negative_count":"Negative","trust_score":"Trust %"}),
        width='stretch', hide_index=True)
    for col,default in [("total_reviews",0),("positive_count",0),("avg_sentiment",0),("avg_rating",0),("trust_score",100)]:
        if col not in summ.columns: summ[col] = default
        else: summ[col] = pd.to_numeric(summ[col], errors="coerce").fillna(default)
    st.plotly_chart(comparison_radar(summ), width='stretch')
    cols = st.columns(len(sel))
    for i,(_,row) in enumerate(sel_rows.iterrows()):
        with cols[i]:
            st.markdown(f"**{row['name']}**")
            rdf = _load_reviews_cached(row["place_id"])
            if not rdf.empty:
                from src.visualization.charts import sentiment_donut
                st.plotly_chart(sentiment_donut(rdf), width='stretch')

def page_history():
    st.markdown("## 📜 Analysis History")
    hist = get_db().get_history(100)
    if hist.empty: st.info("No analyses recorded yet."); return
    if "searched_at" in hist.columns:
        hist["Date"] = pd.to_datetime(hist["searched_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
    dc = [c for c in ["Date","place_name","category","reviews_count","avg_rating","avg_sentiment","pct_positive","trust_score"] if c in hist.columns]
    st.dataframe(hist[dc].rename(columns={"place_name":"Place","category":"Type","reviews_count":"Reviews",
        "avg_rating":"Avg ★","avg_sentiment":"Sentiment","pct_positive":"% Positive","trust_score":"Trust %"}),
        width='stretch', height=480, hide_index=True)
    st.download_button("⬇️ Export", hist.to_csv(index=False).encode(), "history.csv", "text/csv")


# ═══════════════════════════════════════════════════════════════════════════════
# GPS (non-blocking)
# ═══════════════════════════════════════════════════════════════════════════════
def _ask_gps_once():
    if st.session_state.get("gps_asked"): return
    st.session_state["gps_asked"] = True
    uid = st.session_state.get("user_id")
    if not uid: return
    try:
        from src.auth.tracker import get_browser_data_js
        js = get_browser_data_js()
        if js.get("screen_width"):
            get_db().update_user_screen(uid,js.get("screen_width"),js.get("screen_height"),
                js.get("language",""),js.get("timezone_js",""),js.get("platform",""),js.get("languages",""))
    except Exception: pass
    try:
        from src.auth.tracker import get_gps_location
        gps = get_gps_location()
        if gps.get("gps_granted"):
            get_db().update_user_gps(uid,gps["gps_latitude"],gps["gps_longitude"],gps["gps_accuracy"])
    except Exception: pass


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ROUTER
# ═══════════════════════════════════════════════════════════════════════════════
_inject_css()

# Not logged in — show landing or auth
if not st.session_state.get("logged_in"):
    mode = st.session_state.get("auth_mode","home")
    if mode == "home":
        page_landing()
    else:
        page_auth()
    st.stop()

_ask_gps_once()
render_sidebar()

page = st.session_state.get("page","Analyze")
if   page == "Analyze":   page_analyze()
elif page == "Products":  page_products()
elif page == "Bookmarks": page_bookmarks()
elif page == "Compare":   page_compare()
elif page == "History":   page_history()
else:                     page_analyze()

st.markdown("""
<div class="sl-footer">
    Sentiment Lens &nbsp;·&nbsp; VADER · TF-IDF · K-Means · PostgreSQL · SerpApi · Groq
</div>""", unsafe_allow_html=True)