"""
src/auth/tracker.py
--------------------
Collects all available user/browser data and returns a flat dict
ready to be passed to DatabaseManager.create_user().

Data sources:
  1. st.context.headers  — User-Agent, IP (X-Forwarded-For), Accept-Language
  2. ip-api.com          — country, region, city, ISP, timezone (free, no key)
  3. user-agents library — parses UA into browser/OS/device fields
  4. streamlit-js-eval   — screen size, platform, timezone, navigator.languages
  5. GPS (optional)      — browser geolocation via streamlit-js-eval
"""

import requests
import streamlit as st
from datetime import datetime


def get_ip_from_headers() -> str:
    """Extract client IP from Streamlit request headers."""
    try:
        headers = st.context.headers
        # Cloudflare / Railway / reverse proxies set X-Forwarded-For
        ip = (
            headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or headers.get("X-Real-Ip", "")
            or headers.get("Cf-Connecting-Ip", "")
            or ""
        )
        # Filter out loopback / local
        if ip in ("127.0.0.1", "::1", "localhost", ""):
            return ""
        return ip
    except Exception:
        return ""


def get_user_agent() -> str:
    try:
        return st.context.headers.get("User-Agent", "")
    except Exception:
        return ""


def parse_user_agent(ua_string: str) -> dict:
    """Parse User-Agent string into structured fields."""
    result = {
        "user_agent":      ua_string,
        "browser_name":    None,
        "browser_version": None,
        "os_name":         None,
        "os_version":      None,
        "device_type":     "desktop",
        "is_mobile":       False,
        "is_bot":          False,
    }
    if not ua_string:
        return result
    try:
        from user_agents import parse
        ua = parse(ua_string)
        result["browser_name"]    = ua.browser.family or None
        result["browser_version"] = ua.browser.version_string or None
        result["os_name"]         = ua.os.family or None
        result["os_version"]      = ua.os.version_string or None
        result["is_mobile"]       = ua.is_mobile
        result["is_bot"]          = ua.is_bot
        if ua.is_tablet:
            result["device_type"] = "tablet"
        elif ua.is_mobile:
            result["device_type"] = "mobile"
        else:
            result["device_type"] = "desktop"
    except Exception:
        pass
    return result


def geolocate_ip(ip: str) -> dict:
    """
    Call ip-api.com (free, no key, 45 req/min limit) to get geo data.
    Returns empty dict on failure so the app still works.
    """
    empty = {
        "country": None, "country_code": None, "region": None,
        "region_name": None, "city": None, "zip_code": None,
        "latitude": None, "longitude": None, "timezone": None,
        "isp": None, "org": None,
    }
    if not ip:
        return empty
    try:
        resp = requests.get(
            f"http://ip-api.com/json/{ip}",
            params={"fields": "status,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org"},
            timeout=5,
        )
        data = resp.json()
        if data.get("status") != "success":
            return empty
        return {
            "country":      data.get("country"),
            "country_code": data.get("countryCode"),
            "region":       data.get("region"),
            "region_name":  data.get("regionName"),
            "city":         data.get("city"),
            "zip_code":     data.get("zip"),
            "latitude":     data.get("lat"),
            "longitude":    data.get("lon"),
            "timezone":     data.get("timezone"),
            "isp":          data.get("isp"),
            "org":          data.get("org"),
        }
    except Exception:
        return empty


def get_browser_data_js() -> dict:
    """
    Run JavaScript in the browser to collect screen / locale data.
    Uses streamlit-js-eval. Returns empty dict if package unavailable.
    """
    result = {
        "screen_width":  None,
        "screen_height": None,
        "language":      None,
        "languages":     None,
        "platform":      None,
        "timezone_js":   None,
    }
    try:
        from streamlit_js_eval import streamlit_js_eval
        sw   = streamlit_js_eval(js_expressions="screen.width",                        key="_sw")
        sh   = streamlit_js_eval(js_expressions="screen.height",                       key="_sh")
        lang = streamlit_js_eval(js_expressions="navigator.language",                  key="_lang")
        langs= streamlit_js_eval(js_expressions="navigator.languages.join(',')",       key="_langs")
        plat = streamlit_js_eval(js_expressions="navigator.platform",                  key="_plat")
        tz   = streamlit_js_eval(js_expressions="Intl.DateTimeFormat().resolvedOptions().timeZone", key="_tz")

        result["screen_width"]  = int(sw)  if sw  else None
        result["screen_height"] = int(sh)  if sh  else None
        result["language"]      = str(lang)  if lang  else None
        result["languages"]     = str(langs) if langs else None
        result["platform"]      = str(plat)  if plat  else None
        result["timezone_js"]   = str(tz)    if tz    else None
    except Exception:
        pass
    return result


def get_gps_location() -> dict:
    """
    Ask browser for GPS coordinates.
    Returns dict with gps_latitude, gps_longitude, gps_accuracy, gps_granted.
    gps_granted=False means the user denied or the call failed.
    """
    empty = {"gps_latitude": None, "gps_longitude": None,
             "gps_accuracy": None, "gps_granted": False}
    try:
        from streamlit_js_eval import get_geolocation
        loc = get_geolocation()
        if loc and isinstance(loc, dict):
            coords = loc.get("coords", {})
            return {
                "gps_latitude":  coords.get("latitude"),
                "gps_longitude": coords.get("longitude"),
                "gps_accuracy":  coords.get("accuracy"),
                "gps_granted":   True,
            }
    except Exception:
        pass
    return empty


def collect_all(name: str, phone: str = "") -> dict:
    """
    Master collector — gathers everything and returns one flat dict
    ready for DatabaseManager.create_user().
    """
    ip      = get_ip_from_headers()
    ua_str  = get_user_agent()
    ua_data = parse_user_agent(ua_str)
    geo     = geolocate_ip(ip)
    js_data = get_browser_data_js()

    return {
        "name":  name.strip(),
        "phone": phone.strip() or None,
        "ip_address": ip or None,
        **geo,
        **ua_data,
        **js_data,
        # GPS filled in separately after user grants permission
        "gps_latitude":  None,
        "gps_longitude": None,
        "gps_accuracy":  None,
        "gps_granted":   False,
    }