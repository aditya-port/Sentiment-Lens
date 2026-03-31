"""
src/visualization/charts.py — Final Production Build
NO **_BASE spreads in update_layout — uses _layout() helper exclusively.
This eliminates ALL duplicate keyword argument errors permanently.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from src.config import (
    COLOR_POSITIVE, COLOR_NEGATIVE, COLOR_NEUTRAL,
    COLOR_PRIMARY, COLOR_ACCENT, COLOR_INFO, SENTIMENT_COLORS,
)

_FONT = "Inter, -apple-system, BlinkMacSystemFont, sans-serif"
_BG   = "rgba(0,0,0,0)"
_GRID = "rgba(128,128,128,0.10)"

# ── Core helpers ────────────────────────────────────────────────────────────

def _ax(**kw) -> dict:
    d = dict(gridcolor=_GRID, zeroline=False, showgrid=True, linecolor=_GRID)
    d.update(kw)
    return d

def _layout(**kw) -> dict:
    """
    Single source of layout defaults. Pass overrides via kwargs.
    NEVER spread into update_layout — always call as **_layout(...).
    This guarantees no duplicate keyword argument errors.
    """
    base = dict(
        font=dict(family=_FONT, size=12),
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        transition=dict(duration=350, easing="cubic-in-out"),
        margin=kw.pop("margin", dict(l=10, r=10, t=44, b=10)),
        legend=kw.pop("legend", dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1, font=dict(size=11), title_text="",
        )),
    )
    base.update(kw)
    return base

def _month_fmt(period_series: pd.Series) -> list:
    """Convert Period/string series → ['Jan 2026', 'Feb 2026', ...]"""
    try:
        return list(pd.PeriodIndex(period_series).strftime("%b %Y"))
    except Exception:
        return [str(x) for x in period_series]

def _empty(msg: str = "Not enough data.", h: int = 240) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=msg, xref="paper", yref="paper", x=0.5, y=0.5,
        showarrow=False, font=dict(size=13, color="#6B7280"),
    )
    fig.update_layout(**_layout(height=h))
    return fig


# ── 1. Sentiment Gauge ──────────────────────────────────────────────────────
def sentiment_gauge(avg_score: float) -> go.Figure:
    score = float(avg_score) if avg_score is not None else 0.0
    bar_color = (COLOR_POSITIVE if score >= 0.05
                 else COLOR_NEGATIVE if score <= -0.05
                 else COLOR_NEUTRAL)

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number=dict(valueformat=".3f", font=dict(size=28, family=_FONT, color=bar_color)),
        gauge=dict(
            axis=dict(
                range=[-1, 1],
                tickvals=[-1, 0, 1],
                ticktext=["-1", "0", "+1"],
                tickwidth=1,
                tickcolor="rgba(128,128,128,0.4)",
                tickfont=dict(size=9, family=_FONT),
            ),
            bar=dict(color=bar_color, thickness=0.18),
            bgcolor=_BG,
            borderwidth=0,
            steps=[
                dict(range=[-1.0, -0.05], color="rgba(239,68,68,0.08)"),
                dict(range=[-0.05, 0.05], color="rgba(107,114,128,0.08)"),
                dict(range=[0.05,  1.0],  color="rgba(16,185,129,0.08)"),
            ],
            threshold=dict(line=dict(color=bar_color, width=2), thickness=0.70, value=score),
        ),
        title=dict(text="Sentiment Score", font=dict(size=11, family=_FONT, color="#9CA3AF")),
        domain=dict(x=[0.05, 0.95], y=[0.10, 0.90]),
    ))
    fig.update_layout(
        paper_bgcolor=_BG, plot_bgcolor=_BG,
        font=dict(family=_FONT),
        transition=dict(duration=350, easing="cubic-in-out"),
        margin=dict(l=24, r=24, t=32, b=16),
        height=180,
    )
    return fig


# ── 2. Sentiment Donut ──────────────────────────────────────────────────────
def sentiment_donut(df: pd.DataFrame) -> go.Figure:
    if df.empty or "sentiment_label" not in df.columns:
        return _empty("Load a place to see sentiment distribution.")
    counts = df["sentiment_label"].value_counts().reset_index()
    counts.columns = ["label", "count"]
    fig = go.Figure(go.Pie(
        labels=counts["label"],
        values=counts["count"],
        hole=0.60,
        marker=dict(
            colors=[SENTIMENT_COLORS.get(l, COLOR_PRIMARY) for l in counts["label"]],
            line=dict(color=_BG, width=3),
        ),
        textfont=dict(size=12, family=_FONT),
        hovertemplate="%{label}<br>%{value} reviews · %{percent}<extra></extra>",
        sort=False,
    ))
    fig.update_layout(**_layout(
        title=dict(text="Sentiment Distribution", font=dict(size=13), x=0),
        height=270,
    ))
    return fig


# ── 3. Rating Distribution ──────────────────────────────────────────────────
def rating_distribution(df: pd.DataFrame) -> go.Figure:
    if df.empty or "rating" not in df.columns:
        return _empty()
    _sc = {1:"#EF4444", 2:"#F97316", 3:COLOR_NEUTRAL, 4:"#34D399", 5:COLOR_POSITIVE}
    counts = (
        pd.to_numeric(df["rating"], errors="coerce").dropna()
        .astype(int).value_counts()
        .reindex([5, 4, 3, 2, 1], fill_value=0).reset_index()
    )
    counts.columns = ["rating", "count"]
    counts["stars"] = counts["rating"].apply(lambda r: "★" * r)
    fig = go.Figure(go.Bar(
        x=counts["count"], y=counts["stars"], orientation="h",
        marker_color=[_sc[r] for r in counts["rating"]],
        text=counts["count"], textposition="outside", textfont=dict(size=11),
        hovertemplate="%{y}: %{x} reviews<extra></extra>",
    ))
    fig.update_layout(**_layout(
        title=dict(text="Rating Distribution", font=dict(size=13), x=0),
        xaxis=_ax(title="", showgrid=False),
        yaxis=dict(title=""),
        height=230,
        showlegend=False,
    ))
    return fig


# ── 4. Sentiment Over Time ──────────────────────────────────────────────────
def sentiment_over_time(df: pd.DataFrame, window: int = 7) -> go.Figure:
    if df.empty or "sentiment_score" not in df.columns or "review_date" not in df.columns:
        return _empty("Date data required for trend chart.")
    ts = df.copy()
    ts["review_date"]     = pd.to_datetime(ts["review_date"], errors="coerce")
    ts["sentiment_score"] = pd.to_numeric(ts["sentiment_score"], errors="coerce").astype(float)
    ts = ts.dropna(subset=["review_date", "sentiment_score"])
    if ts.empty:
        return _empty()
    daily = (
        ts.groupby(ts["review_date"].dt.date)["sentiment_score"]
        .mean().reset_index().sort_values("review_date")
        .rename(columns={"review_date": "date", "sentiment_score": "score"})
    )
    smoothed = daily["score"].rolling(window=window, min_periods=1, center=True).mean()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily["date"], y=daily["score"], mode="markers",
        marker=dict(size=4, color=COLOR_PRIMARY, opacity=0.22),
        name="Daily", hovertemplate="Date: %{x|%d %b %Y}<br>Score: %{y:.3f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=daily["date"], y=smoothed, mode="lines",
        line=dict(color=COLOR_PRIMARY, width=2.5, shape="spline"),
        name=f"{window}-day avg", fill="tozeroy",
        fillcolor="rgba(99,102,241,0.05)",
        hovertemplate="Date: %{x|%d %b %Y}<br>Avg: %{y:.3f}<extra></extra>",
    ))
    fig.add_hline(y=0.05,  line_dash="dot", line_color=COLOR_POSITIVE, line_width=1)
    fig.add_hline(y=-0.05, line_dash="dot", line_color=COLOR_NEGATIVE, line_width=1)
    fig.update_layout(**_layout(
        title=dict(text="Sentiment Score Over Time", font=dict(size=13), x=0),
        xaxis=_ax(title=""),
        yaxis=_ax(title="Score", range=[-1.05, 1.05]),
        height=290,
    ))
    return fig


# ── 5. Monthly Volume ───────────────────────────────────────────────────────
def monthly_volume(df: pd.DataFrame) -> go.Figure:
    if df.empty or "review_date" not in df.columns:
        return _empty()
    ts = df.copy()
    ts["review_date"] = pd.to_datetime(ts["review_date"], errors="coerce")
    ts = ts.dropna(subset=["review_date"])
    if ts.empty:
        return _empty()
    grp = ts.groupby(ts["review_date"].dt.to_period("M")).size().reset_index(name="count")
    grp["label"] = _month_fmt(grp["review_date"])
    fig = go.Figure(go.Bar(
        x=grp["label"], y=grp["count"],
        marker=dict(color=COLOR_PRIMARY, opacity=0.78,
                    line=dict(color="rgba(99,102,241,0.4)", width=1)),
        hovertemplate="%{x}: %{y} reviews<extra></extra>",
    ))
    fig.update_layout(**_layout(
        title=dict(text="Monthly Review Volume", font=dict(size=13), x=0),
        xaxis=_ax(title="", tickangle=-30),
        yaxis=_ax(title=""),
        height=230,
        showlegend=False,
    ))
    return fig


# ── 6. Keyword Bars ─────────────────────────────────────────────────────────
def keyword_bars(keywords: list, color: str = COLOR_PRIMARY, title: str = "Keywords") -> go.Figure:
    if not keywords:
        return _empty("Not enough reviews for keyword extraction.")
    terms  = [k[0] for k in keywords]
    scores = [float(k[1]) for k in keywords]
    fig = go.Figure(go.Bar(
        x=scores, y=terms, orientation="h",
        marker=dict(color=color, opacity=0.80, line=dict(width=0)),
        hovertemplate="%{y}: %{x:.4f}<extra></extra>",
    ))
    fig.update_layout(**_layout(
        title=dict(text=title, font=dict(size=13), x=0),
        xaxis=_ax(title="TF-IDF score", showgrid=False),
        yaxis=dict(autorange="reversed"),
        height=max(240, 26 * len(keywords)),
        showlegend=False,
    ))
    return fig


# ── 7. Aspect Radar ─────────────────────────────────────────────────────────
def aspect_radar(aspect_df: pd.DataFrame) -> go.Figure:
    if aspect_df is None or aspect_df.empty:
        return _empty()
    aspects = aspect_df["aspect"].tolist()
    ppos    = [float(x) for x in aspect_df["pct_positive"].tolist()]
    fig = go.Figure(go.Scatterpolar(
        r=ppos + [ppos[0]], theta=aspects + [aspects[0]],
        fill="toself", fillcolor="rgba(99,102,241,0.10)",
        line=dict(color=COLOR_PRIMARY, width=2),
        hovertemplate="%{theta}: %{r:.1f}% positive<extra></extra>",
    ))
    fig.update_layout(**_layout(
        polar=dict(
            bgcolor=_BG,
            radialaxis=dict(visible=True, range=[0, 100], ticksuffix="%",
                            tickfont=dict(size=9), gridcolor=_GRID),
            angularaxis=dict(tickfont=dict(size=11, family=_FONT), gridcolor=_GRID),
        ),
        title=dict(text="Aspect Sentiment (% Positive)", font=dict(size=13), x=0),
        height=300,
        showlegend=False,
    ))
    return fig


# ── 8. Aspect Bar ───────────────────────────────────────────────────────────
def aspect_bar(aspect_df: pd.DataFrame) -> go.Figure:
    if aspect_df is None or aspect_df.empty:
        return _empty()
    adf = aspect_df.copy()
    for c in ["mention_count", "avg_sentiment", "pct_positive"]:
        if c in adf.columns:
            adf[c] = pd.to_numeric(adf[c], errors="coerce").astype(float)
    colors = [SENTIMENT_COLORS.get(l, COLOR_PRIMARY) for l in adf["sentiment_label"]]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=adf["aspect"], y=adf["mention_count"], name="Mentions",
        marker=dict(color=colors, opacity=0.72),
        hovertemplate="%{x}: %{y} mentions<extra></extra>",
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=adf["aspect"], y=adf["pct_positive"], name="% Positive",
        mode="lines+markers", line=dict(color=COLOR_ACCENT, width=2), marker=dict(size=7),
        hovertemplate="%{x}: %{y:.1f}% positive<extra></extra>",
    ), secondary_y=True)
    fig.update_layout(**_layout(
        title=dict(text="Aspect Mentions & Positivity", font=dict(size=13), x=0),
        height=290,
    ))
    fig.update_yaxes(title_text="Mentions",   secondary_y=False, gridcolor=_GRID, zeroline=False)
    fig.update_yaxes(title_text="% Positive", secondary_y=True,  range=[0, 110], gridcolor=_GRID, zeroline=False)
    return fig


# ── 9. Rating vs Sentiment — Negative·Neutral·Positive left-to-right ────────
def rating_vs_sentiment(df: pd.DataFrame) -> go.Figure:
    if df.empty or "rating" not in df.columns or "sentiment_score" not in df.columns:
        return _empty()
    sub = df.dropna(subset=["rating", "sentiment_score"]).copy()
    if sub.empty:
        return _empty()
    sub["rating"]          = pd.to_numeric(sub["rating"],          errors="coerce").astype(float)
    sub["sentiment_score"] = pd.to_numeric(sub["sentiment_score"], errors="coerce").astype(float)
    sub = sub.dropna(subset=["rating", "sentiment_score"])
    if sub.empty:
        return _empty()
    sub["jitter"] = sub["rating"] + np.random.uniform(-0.18, 0.18, len(sub))

    fig = px.scatter(
        sub, x="jitter", y="sentiment_score",
        color="sentiment_label",
        color_discrete_map=SENTIMENT_COLORS,
        category_orders={"sentiment_label": ["Negative", "Neutral", "Positive"]},
        opacity=0.50,
        hover_data={"rating": True, "sentiment_score": ":.3f", "jitter": False, "sentiment_label": False},
        labels={"jitter": "Star Rating", "sentiment_score": "VADER Score", "sentiment_label": ""},
    )
    coeffs = np.polyfit(sub["rating"], sub["sentiment_score"], 1)
    fig.add_trace(go.Scatter(
        x=np.linspace(1, 5, 50), y=np.polyval(coeffs, np.linspace(1, 5, 50)),
        mode="lines", line=dict(dash="dot", color="#6B7280", width=1.5),
        showlegend=False,
    ))
    # Use separate update_layout calls to avoid any possible duplicate key
    fig.update_layout(**_layout(
        title=dict(text="Star Rating vs. Sentiment Score", font=dict(size=13), x=0),
        xaxis=_ax(title="Star Rating", tickvals=[1, 2, 3, 4, 5]),
        yaxis=_ax(title="VADER Score", range=[-1.08, 1.08]),
        height=290,
        margin=dict(l=10, r=10, t=44, b=44),
    ))
    # Override legend position — separate call avoids duplicate key
    fig.update_layout(legend=dict(
        orientation="h", yanchor="top", y=-0.15,
        xanchor="center", x=0.5, font=dict(size=11), title_text="",
    ))
    return fig


# ── 10. Topic Cluster Chart ─────────────────────────────────────────────────
def topic_cluster_chart(cluster_df: pd.DataFrame) -> go.Figure:
    if cluster_df is None or cluster_df.empty:
        return _empty("Need at least 15 reviews for topic clustering.")
    cdf = cluster_df.copy()
    for c in ["review_count", "avg_sentiment", "pct_positive"]:
        if c in cdf.columns:
            cdf[c] = pd.to_numeric(cdf[c], errors="coerce").astype(float)

    def _c(s):
        return COLOR_POSITIVE if s >= 0.05 else COLOR_NEGATIVE if s <= -0.05 else COLOR_NEUTRAL

    colors = [_c(float(s)) for s in cdf["avg_sentiment"]]
    fig = go.Figure(go.Bar(
        x=cdf["review_count"], y=cdf["topic"], orientation="h",
        marker=dict(color=colors, opacity=0.80),
        text=[f'{int(r)} reviews · {float(s):+.2f}' for r, s in zip(cdf["review_count"], cdf["avg_sentiment"])],
        textposition="outside", textfont=dict(size=10),
        hovertemplate="Topic: %{y}<br>Reviews: %{x}<extra></extra>",
    ))
    fig.update_layout(**_layout(
        title=dict(text="Review Topics (K-Means Clusters)", font=dict(size=13), x=0),
        xaxis=_ax(title="Review Count", showgrid=False),
        yaxis=dict(autorange="reversed"),
        height=max(240, 48 * len(cdf)),
        showlegend=False,
    ))
    return fig


# ── 11. Suspicion Histogram ─────────────────────────────────────────────────
def suspicion_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty or "suspicion_score" not in df.columns:
        return _empty()
    from src.config import SUSPICION_FLAG_THRESHOLD
    fig = go.Figure(go.Histogram(
        x=pd.to_numeric(df["suspicion_score"], errors="coerce").dropna(),
        nbinsx=20, marker_color=COLOR_INFO, opacity=0.72,
        hovertemplate="Score: %{x:.2f}<br>Count: %{y}<extra></extra>",
    ))
    fig.add_vline(x=SUSPICION_FLAG_THRESHOLD, line_dash="dash", line_color=COLOR_NEGATIVE,
                  annotation_text="Flag threshold", annotation_font_size=10)
    fig.update_layout(**_layout(
        title=dict(text="Authenticity Score Distribution", font=dict(size=13), x=0),
        xaxis=_ax(title="Suspicion Score"),
        yaxis=_ax(title="Reviews"),
        height=230,
        showlegend=False,
    ))
    return fig


# ── 12. Owner Response Chart ────────────────────────────────────────────────
def owner_response_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty or "has_owner_response" not in df.columns or "review_date" not in df.columns:
        return _empty()
    ts = df.copy()
    ts["review_date"] = pd.to_datetime(ts["review_date"], errors="coerce")
    ts = ts.dropna(subset=["review_date"])
    if ts.empty:
        return _empty()
    ts["period"] = ts["review_date"].dt.to_period("M")
    grp = ts.groupby(["period", "has_owner_response"]).size().unstack(fill_value=0)
    labels = _month_fmt(pd.Series(grp.index))
    responded   = grp.get(True,  pd.Series(0, index=grp.index)).tolist()
    no_response = grp.get(False, pd.Series(0, index=grp.index)).tolist()
    fig = go.Figure()
    fig.add_trace(go.Bar(x=labels, y=responded,   name="Responded",   marker_color=COLOR_POSITIVE, opacity=0.80))
    fig.add_trace(go.Bar(x=labels, y=no_response, name="No Response", marker_color=COLOR_NEUTRAL,  opacity=0.45))
    fig.update_layout(**_layout(
        barmode="stack",
        title=dict(text="Owner Response by Month", font=dict(size=13), x=0),
        xaxis=_ax(title="", tickangle=-30),
        yaxis=_ax(title="Reviews"),
        height=240,
    ))
    return fig


# ── 13. Comparison Radar ────────────────────────────────────────────────────
def comparison_radar(summary_df: pd.DataFrame) -> go.Figure:
    if summary_df is None or summary_df.empty:
        return _empty("Select at least 2 places to compare.")
    dimensions = ["Sentiment", "Avg Rating", "Trust", "% Positive", "Volume"]
    palette    = [COLOR_PRIMARY, COLOR_POSITIVE, COLOR_ACCENT, COLOR_INFO, "#A78BFA"]
    fig = go.Figure()
    max_rev = max(pd.to_numeric(summary_df.get("total_reviews", pd.Series([1])), errors="coerce").max(), 1)
    for i, (_, row) in enumerate(summary_df.iterrows()):
        total = max(float(pd.to_numeric(row.get("total_reviews", 1), errors="coerce") or 1), 1)
        ppos  = 100.0 * float(pd.to_numeric(row.get("positive_count", 0), errors="coerce") or 0) / total
        vol   = 100.0 * total / max_rev
        avg_r = 100.0 * float(pd.to_numeric(row.get("avg_rating", 0),    errors="coerce") or 0) / 5.0
        sent  = 50 + 50 * float(pd.to_numeric(row.get("avg_sentiment", 0), errors="coerce") or 0)
        trust = float(pd.to_numeric(row.get("trust_score", 100), errors="coerce") or 100)
        vals  = [sent, avg_r, trust, ppos, vol]
        col   = palette[i % len(palette)]
        r, g, b = int(col[1:3], 16), int(col[3:5], 16), int(col[5:7], 16)
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=dimensions + [dimensions[0]],
            fill="toself", fillcolor=f"rgba({r},{g},{b},0.10)",
            line=dict(color=col, width=2),
            name=str(row.get("name", f"Place {i+1}")),
        ))
    fig.update_layout(**_layout(
        polar=dict(
            bgcolor=_BG,
            radialaxis=dict(range=[0, 100], visible=True, tickfont=dict(size=9), gridcolor=_GRID),
            angularaxis=dict(tickfont=dict(size=11, family=_FONT), gridcolor=_GRID),
        ),
        title=dict(text="Multi-Place Comparison", font=dict(size=13), x=0),
        height=340,
    ))
    return fig


# ── 14. Review Length Box Plot ──────────────────────────────────────────────
def review_length_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty or "word_count" not in df.columns or "sentiment_label" not in df.columns:
        return _empty()
    order = ["Negative", "Neutral", "Positive"]
    fig = go.Figure()
    for label in order:
        if label not in SENTIMENT_COLORS:
            continue
        vals = pd.to_numeric(
            df[df["sentiment_label"] == label]["word_count"], errors="coerce"
        ).dropna()
        if vals.empty:
            continue
        fig.add_trace(go.Box(
            y=vals, name=label,
            marker_color=SENTIMENT_COLORS[label],
            boxmean=True,
            hovertemplate=(
                f"<b>{label}</b><br>"
                "Max: %{upperfence}<br>"
                "Avg words: %{mean:.0f}<br>"
                "Min: %{lowerfence}<extra></extra>"
            ),
            hoveron="boxes",
        ))
    fig.update_layout(**_layout(
        title=dict(text="Review Length by Sentiment (Words)", font=dict(size=13), x=0),
        yaxis=_ax(title="Word Count"),
        xaxis=dict(title=""),
        height=240,
        showlegend=False,
    ))
    return fig


# ── 15. Sentiment by Day of Week ────────────────────────────────────────────
def sentiment_by_weekday(df: pd.DataFrame) -> go.Figure:
    if df.empty or "review_date" not in df.columns or "sentiment_score" not in df.columns:
        return _empty("Need dated reviews for weekday analysis.")
    ts = df.copy()
    ts["review_date"]     = pd.to_datetime(ts["review_date"], errors="coerce")
    ts["sentiment_score"] = pd.to_numeric(ts["sentiment_score"], errors="coerce").astype(float)
    ts = ts.dropna(subset=["review_date", "sentiment_score"])
    if len(ts) < 5:
        return _empty("Need at least 5 dated reviews.")

    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    ts["weekday"] = pd.Categorical(ts["review_date"].dt.day_name(), categories=order, ordered=True)
    grp = ts.groupby("weekday", observed=True).agg(
        avg_score=("sentiment_score", "mean"),
        count=("sentiment_score", "count"),
    ).reset_index()

    def _c(s):
        return COLOR_POSITIVE if s >= 0.05 else COLOR_NEGATIVE if s <= -0.05 else COLOR_NEUTRAL

    fig = go.Figure(go.Bar(
        x=grp["weekday"],
        y=grp["avg_score"].round(3),
        marker_color=[_c(float(s)) for s in grp["avg_score"]],
        opacity=0.82,
        text=[f'{float(s):+.3f}' for s in grp["avg_score"]],
        textposition="outside",
        textfont=dict(size=10),
        customdata=grp["count"],
        hovertemplate="%{x}<br>Avg Score: %{y:.3f}<br>Reviews: %{customdata}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dot", line_color="#6B7280", line_width=1)
    fig.update_layout(**_layout(
        title=dict(text="Avg Sentiment by Day of Week", font=dict(size=13), x=0),
        xaxis=_ax(title=""),
        yaxis=_ax(title="Avg Score", range=[-1.1, 1.1]),
        height=240,
        showlegend=False,
    ))
    return fig


# ── 16. Rating Over Time ─────────────────────────────────────────────────────
def rating_over_time(df: pd.DataFrame) -> go.Figure:
    if df.empty or "review_date" not in df.columns or "rating" not in df.columns:
        return _empty()
    ts = df.copy()
    ts["review_date"] = pd.to_datetime(ts["review_date"], errors="coerce")
    ts["rating"]      = pd.to_numeric(ts["rating"], errors="coerce").astype(float)
    ts = ts.dropna(subset=["review_date", "rating"])
    if ts.empty:
        return _empty()
    grp = ts.groupby(ts["review_date"].dt.to_period("M"))["rating"].mean().reset_index()
    grp["label"] = _month_fmt(grp["review_date"])
    fig = go.Figure(go.Scatter(
        x=grp["label"], y=grp["rating"].round(2),
        mode="lines+markers",
        line=dict(color=COLOR_ACCENT, width=2.5, shape="spline"),
        marker=dict(size=7, color=COLOR_ACCENT),
        fill="tozeroy", fillcolor="rgba(245,158,11,0.05)",
        hovertemplate="%{x}: %{y:.2f} ★<extra></extra>",
    ))
    fig.add_hline(y=4.0, line_dash="dot", line_color="#6B7280", line_width=1,
                  annotation_text="4.0 baseline", annotation_font_size=9)
    fig.update_layout(**_layout(
        title=dict(text="Avg Rating Over Time", font=dict(size=13), x=0),
        xaxis=_ax(title="", tickangle=-30),
        yaxis=_ax(title="", range=[1, 5.2]),
        height=230,
        showlegend=False,
    ))
    return fig