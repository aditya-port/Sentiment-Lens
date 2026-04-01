"""
src/config.py — all constants and env-var bindings.
"""
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH      = os.getenv("DB_PATH", "")
SERPAPI_KEY  = os.getenv("SERPAPI_KEY", "")
GROQ_KEY     = os.getenv("GROK_KEY", "")
MAX_REVIEWS  = int(os.getenv("MAX_REVIEWS", "500"))   # raised from 200→500

SERPAPI_BASE          = "https://serpapi.com/search"
GROQ_MODEL            = "llama-3.3-70b-versatile"
GROQ_API_URL          = "https://api.groq.com/openai/v1/chat/completions"
AI_SUMMARY_MAX_TOKENS = 600

POSITIVE_THRESHOLD = 0.05
NEGATIVE_THRESHOLD = -0.05

# Minimum review text length to be considered a "real" review
MIN_REVIEW_CHARS = 10   # reviews shorter than this are ignored (neutrals with no text)

SUSPICION_WEIGHTS = {
    "very_short_text":    0.35,
    "short_with_5star":   0.25,
    "extreme_no_context": 0.20,
    "all_caps":           0.15,
    "excessive_punct":    0.10,
    "single_word":        0.30,
}
SUSPICION_FLAG_THRESHOLD = 0.35

FREE_MONTHLY_LIMIT = 20   # raised from 10→20

N_CLUSTERS                 = 5
MIN_REVIEWS_FOR_CLUSTERING = 15

COLOR_POSITIVE = "#10B981"
COLOR_NEGATIVE = "#EF4444"
COLOR_NEUTRAL  = "#6B7280"
COLOR_PRIMARY  = "#6366F1"
COLOR_ACCENT   = "#F59E0B"
COLOR_INFO     = "#3B82F6"
COLOR_WARNING  = "#F97316"

SENTIMENT_COLORS = {
    "Positive": COLOR_POSITIVE,
    "Neutral":  COLOR_NEUTRAL,
    "Negative": COLOR_NEGATIVE,
}

ASPECT_KEYWORDS = {
    "Quality":     ["quality","excellent","poor","great","terrible","amazing","awful",
                    "outstanding","disappointing","superb","mediocre","exceptional","perfect","flawed"],
    "Service":     ["service","staff","employee","helpful","rude","friendly","attentive",
                    "professional","unhelpful","courteous","dismissive","responsive","polite","manager"],
    "Value":       ["price","value","money","worth","expensive","cheap","affordable",
                    "reasonable","overpriced","pricey","bargain","costly","fair","steep"],
    "Location":    ["location","parking","accessible","convenient","area","neighbourhood",
                    "neighborhood","nearby","distance","central","remote","situated","proximity"],
    "Cleanliness": ["clean","dirty","hygiene","sanitary","tidy","messy","spotless",
                    "filthy","neat","grimy","immaculate","dusty","fresh","maintained"],
    "Wait Time":   ["wait","waiting","slow","fast","quick","busy","crowded","line",
                    "queue","immediate","lengthy","prompt","efficient","rush","delay"],
}

CACHE_TTL_REVIEWS = 300
CACHE_TTL_PLACES  = 120
