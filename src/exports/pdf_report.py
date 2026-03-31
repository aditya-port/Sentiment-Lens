"""
src/export/pdf_report.py
-------------------------
Generate a clean PDF summary report for a place or product analysis.
Uses reportlab — zero external dependencies beyond pip install reportlab.
"""
from __future__ import annotations
import io
from datetime import datetime
import pandas as pd


def generate_pdf_report(
    name: str,
    stats: dict,
    velocity: dict,
    trust: float,
    aspect_df: pd.DataFrame | None,
    kw_positive: list,
    kw_negative: list,
    source_type: str = "place",    # "place" or "product"
) -> bytes:
    """
    Generate a PDF report and return as bytes (for st.download_button).
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles   import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units    import cm
        from reportlab.lib          import colors
        from reportlab.platypus     import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
        )

        buffer = io.BytesIO()
        doc    = SimpleDocTemplate(buffer, pagesize=A4,
                                   leftMargin=2*cm, rightMargin=2*cm,
                                   topMargin=2*cm, bottomMargin=2*cm)

        styles = getSampleStyleSheet()
        INDIGO  = colors.HexColor("#6366F1")
        GREEN   = colors.HexColor("#10B981")
        RED     = colors.HexColor("#EF4444")
        GRAY    = colors.HexColor("#6B7280")
        DARK    = colors.HexColor("#0F172A")
        LIGHT   = colors.HexColor("#F8FAFF")

        title_style = ParagraphStyle("Title", parent=styles["Title"],
                                     fontSize=22, textColor=DARK, spaceAfter=4)
        h2_style    = ParagraphStyle("H2", parent=styles["Heading2"],
                                     fontSize=13, textColor=INDIGO, spaceAfter=6,
                                     spaceBefore=14)
        body_style  = ParagraphStyle("Body", parent=styles["Normal"],
                                     fontSize=10, textColor=DARK, spaceAfter=4,
                                     leading=14)
        muted_style = ParagraphStyle("Muted", parent=styles["Normal"],
                                     fontSize=9, textColor=GRAY)

        story = []

        # Header
        story.append(Paragraph("Sentiment Lens", muted_style))
        story.append(Paragraph(name, title_style))
        story.append(Paragraph(
            f"{source_type.capitalize()} Analysis Report &nbsp;·&nbsp; "
            f"Generated {datetime.utcnow().strftime('%d %b %Y %H:%M')} UTC",
            muted_style))
        story.append(HRFlowable(width="100%", thickness=1, color=INDIGO, spaceAfter=10))

        # Key metrics table
        story.append(Paragraph("Summary Metrics", h2_style))
        avg_r = stats.get("avg_rating", 0) or 0
        metrics_data = [
            ["Metric", "Value"],
            ["Total Reviews",  str(int(stats.get("total", 0)))],
            ["Avg Rating",     f'{float(avg_r):.2f} / 5.0' if avg_r else "N/A"],
            ["Positive",       f'{float(stats.get("pct_positive",0)):.1f}%'],
            ["Neutral",        f'{float(stats.get("pct_neutral",0)):.1f}%'],
            ["Negative",       f'{float(stats.get("pct_negative",0)):.1f}%'],
            ["Sentiment Score", f'{float(stats.get("avg_compound",0)):+.3f}'],
            ["Trust Score",    f'{float(trust):.1f}%'],
            ["30-Day Trend",   velocity.get("direction","stable").capitalize()],
        ]
        t = Table(metrics_data, colWidths=[7*cm, 9*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",  (0,0), (-1,0), INDIGO),
            ("TEXTCOLOR",   (0,0), (-1,0), colors.white),
            ("FONTSIZE",    (0,0), (-1,0), 10),
            ("FONTNAME",    (0,0), (-1,0), "Helvetica-Bold"),
            ("BACKGROUND",  (0,1), (-1,-1), LIGHT),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT, colors.white]),
            ("FONTSIZE",    (0,1), (-1,-1), 10),
            ("TEXTCOLOR",   (0,1), (-1,-1), DARK),
            ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#E2E8F0")),
            ("ROWHEIGHT",   (0,0), (-1,-1), 18),
            ("LEFTPADDING", (0,0), (-1,-1), 8),
        ]))
        story.append(t)

        # Aspect analysis
        if aspect_df is not None and not aspect_df.empty:
            story.append(Paragraph("Aspect Analysis", h2_style))
            asp_data = [["Aspect", "Mentions", "% Positive", "Score", "Label"]]
            for _, row in aspect_df.iterrows():
                sentiment = row.get("sentiment_label","")
                color_map = {"Positive": "✅", "Negative": "❌", "Neutral": "➖"}
                asp_data.append([
                    str(row.get("aspect","")),
                    str(int(row.get("mention_count",0))),
                    f'{float(row.get("pct_positive",0)):.0f}%',
                    f'{float(row.get("avg_sentiment",0)):+.3f}',
                    f'{color_map.get(sentiment,"")} {sentiment}',
                ])
            at = Table(asp_data, colWidths=[4.5*cm,2.5*cm,2.5*cm,2.5*cm,4*cm])
            at.setStyle(TableStyle([
                ("BACKGROUND",  (0,0), (-1,0), INDIGO),
                ("TEXTCOLOR",   (0,0), (-1,0), colors.white),
                ("FONTNAME",    (0,0), (-1,0), "Helvetica-Bold"),
                ("FONTSIZE",    (0,0), (-1,-1), 9),
                ("TEXTCOLOR",   (0,1), (-1,-1), DARK),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT, colors.white]),
                ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#E2E8F0")),
                ("ROWHEIGHT",   (0,0), (-1,-1), 17),
                ("LEFTPADDING", (0,0), (-1,-1), 6),
            ]))
            story.append(at)

        # Keywords
        if kw_positive or kw_negative:
            story.append(Paragraph("Top Keywords", h2_style))
            kw_data = [["✅ Praise Keywords", "❌ Complaint Keywords"]]
            max_kw = max(len(kw_positive), len(kw_negative), 1)
            for i in range(min(max_kw, 8)):
                p = kw_positive[i][0] if i < len(kw_positive) else ""
                n = kw_negative[i][0] if i < len(kw_negative) else ""
                kw_data.append([p, n])
            kt = Table(kw_data, colWidths=[8*cm, 8*cm])
            kt.setStyle(TableStyle([
                ("BACKGROUND",  (0,0), (-1,0), colors.HexColor("#F0FDF4")),
                ("TEXTCOLOR",   (0,0), (0,0), GREEN),
                ("TEXTCOLOR",   (1,0), (1,0), RED),
                ("FONTNAME",    (0,0), (-1,0), "Helvetica-Bold"),
                ("FONTSIZE",    (0,0), (-1,-1), 9),
                ("TEXTCOLOR",   (0,1), (-1,-1), DARK),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT, colors.white]),
                ("GRID",        (0,0), (-1,-1), 0.5, colors.HexColor("#E2E8F0")),
                ("ROWHEIGHT",   (0,0), (-1,-1), 16),
                ("LEFTPADDING", (0,0), (-1,-1), 8),
            ]))
            story.append(kt)

        # Footer
        story.append(Spacer(1, 0.5*cm))
        story.append(HRFlowable(width="100%", thickness=0.5, color=GRAY))
        story.append(Paragraph(
            "Generated by Sentiment Lens · VADER Sentiment Analysis · "
            "Not for redistribution without permission",
            muted_style))

        doc.build(story)
        return buffer.getvalue()

    except ImportError:
        # reportlab not installed — return a plain text fallback
        txt = f"Sentiment Lens Report — {name}\n"
        txt += "=" * 40 + "\n"
        txt += f"Total Reviews: {stats.get('total',0)}\n"
        txt += f"Positive: {stats.get('pct_positive',0):.1f}%\n"
        txt += f"Negative: {stats.get('pct_negative',0):.1f}%\n"
        txt += f"Sentiment Score: {stats.get('avg_compound',0):+.3f}\n"
        txt += f"Trust Score: {trust:.1f}%\n"
        return txt.encode("utf-8")