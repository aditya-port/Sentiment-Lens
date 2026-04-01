"""
src/reports/monthly_report.py
------------------------------
Monthly place analysis report: current vs previous month comparison,
keyword signals, AI improvement suggestions, PDF generation, email delivery.
"""
from __future__ import annotations
import io, os, re
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd


def build_monthly_report(place_name: str, place_id: str, df_all: pd.DataFrame) -> dict:
    """Build monthly comparison dict from all reviews."""
    if df_all.empty:
        return {}
    df = df_all.copy()
    df["review_date"] = pd.to_datetime(df.get("review_date"), errors="coerce")
    df = df.dropna(subset=["review_date"])
    if df.empty:
        return {}

    now = datetime.utcnow()
    curr_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_end   = curr_start - timedelta(seconds=1)
    prev_start = prev_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    curr_df = df[df["review_date"] >= curr_start]
    prev_df = df[(df["review_date"] >= prev_start) & (df["review_date"] <= prev_end)]

    def _stats(d):
        if d.empty:
            return {"total":0,"pct_positive":0,"pct_negative":0,"avg_compound":0,"avg_rating":0,
                    "top_positive":[],"top_negative":[]}
        total = len(d)
        lbl   = d.get("sentiment_label", pd.Series(dtype=str))
        pos   = int((lbl == "Positive").sum()) if "sentiment_label" in d.columns else 0
        neg   = int((lbl == "Negative").sum()) if "sentiment_label" in d.columns else 0
        avg_s = float(pd.to_numeric(d.get("sentiment_score", pd.Series()), errors="coerce").mean() or 0)
        avg_r = float(pd.to_numeric(d.get("rating", pd.Series()), errors="coerce").mean() or 0)
        pos_kw, neg_kw = [], []
        try:
            from src.analysis.themes import get_sentiment_keywords
            kw = get_sentiment_keywords(d, top_n=5)
            pos_kw = [k for k, _ in kw.get("positive", [])[:5]]
            neg_kw = [k for k, _ in kw.get("negative", [])[:5]]
        except Exception:
            pass
        return {"total":total,"pct_positive":round(100*pos/max(total,1),1),
                "pct_negative":round(100*neg/max(total,1),1),
                "avg_compound":round(avg_s,4),"avg_rating":round(avg_r,2),
                "top_positive":pos_kw,"top_negative":neg_kw}

    curr_stats = _stats(curr_df)
    prev_stats = _stats(prev_df)

    def _delta(key):
        c = float(curr_stats.get(key, 0) or 0)
        p = float(prev_stats.get(key, 0) or 0)
        d = c - p
        return {"value":c,"delta":d,"direction":"up" if d>0 else "down" if d<0 else "flat"}

    suggestions = _generate_suggestions(place_name, curr_stats, prev_stats)

    return {
        "place_name": place_name, "place_id": place_id,
        "month":      now.strftime("%B %Y"),
        "generated":  now.strftime("%d %b %Y"),
        "curr": curr_stats, "prev": prev_stats,
        "metrics": {k: _delta(k) for k in
                    ["total","pct_positive","pct_negative","avg_compound","avg_rating"]},
        "suggestions": suggestions,
    }


def _generate_suggestions(place_name: str, curr: dict, prev: dict) -> list[str]:
    try:
        key = os.getenv("GROK_KEY", "")
        if not key:
            return _fallback_suggestions(curr, prev)
        import requests as _req
        prompt = (
            f"Business consultant for '{place_name}'. This month's review stats:\n"
            f"Reviews:{curr['total']}, Positive:{curr['pct_positive']}%, "
            f"Negative:{curr['pct_negative']}%, Sentiment:{curr['avg_compound']}, "
            f"Rating:{curr['avg_rating']}/5\n"
            f"Praise keywords: {', '.join(curr['top_positive']) or 'none'}\n"
            f"Complaint keywords: {', '.join(curr['top_negative']) or 'none'}\n"
            f"Last month: Positive {prev['pct_positive']}%, Negative {prev['pct_negative']}%\n\n"
            "Give exactly 4 numbered, specific, actionable improvement tips. Max 2 sentences each."
        )
        r = _req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model":"llama-3.3-70b-versatile","max_tokens":300,
                  "messages":[{"role":"user","content":prompt}]},
            timeout=20,
        )
        text = r.json()["choices"][0]["message"]["content"].strip()
        tips = []
        for line in text.split("\n"):
            line = line.strip()
            if line and (line[0].isdigit() or line.startswith("-")):
                clean = re.sub(r"^[\d\.\-\*]+\s*", "", line).strip()
                if len(clean) > 10:
                    tips.append(clean)
        return tips[:4] if tips else _fallback_suggestions(curr, prev)
    except Exception:
        return _fallback_suggestions(curr, prev)


def _fallback_suggestions(curr: dict, prev: dict) -> list[str]:
    tips = []
    neg_kw = curr.get("top_negative", [])
    if curr.get("pct_negative", 0) > 20 and neg_kw:
        tips.append(f"Address recurring complaints about: {', '.join(neg_kw[:3])}. Staff training or process changes can help.")
    elif curr.get("pct_negative", 0) > 20:
        tips.append("High negative rate detected. Identify the root cause from recent reviews and act quickly.")
    avg_r = float(curr.get("avg_rating", 0) or 0)
    if avg_r and avg_r < 4.0:
        tips.append("Average rating is below 4.0 ★. Publicly respond to negative reviews and offer resolutions to show you care.")
    delta_pos = float(curr.get("pct_positive",0)) - float(prev.get("pct_positive",0))
    if delta_pos < -5:
        tips.append("Positive reviews dropped vs last month. Re-engage satisfied customers and ask for their honest feedback.")
    pos_kw = curr.get("top_positive", [])
    if pos_kw:
        tips.append(f"Customers love: {', '.join(pos_kw[:3])}. Highlight these strengths in your marketing and social media.")
    if not tips:
        tips.append("Keep up the good work. Consistently responding to all reviews (positive and negative) builds long-term trust.")
    return tips[:4]


def generate_monthly_pdf(report: dict) -> bytes:
    """Generate PDF monthly report bytes."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles   import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units    import cm
        from reportlab.lib          import colors
        from reportlab.platypus     import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable)

        buf  = io.BytesIO()
        doc  = SimpleDocTemplate(buf, pagesize=A4,
                                 leftMargin=2*cm, rightMargin=2*cm,
                                 topMargin=2*cm, bottomMargin=2*cm)
        ss   = getSampleStyleSheet()
        IND  = colors.HexColor("#6366F1")
        GRN  = colors.HexColor("#10B981")
        RED  = colors.HexColor("#EF4444")
        GRAY = colors.HexColor("#6B7280")
        DARK = colors.HexColor("#0F172A")
        LGT  = colors.HexColor("#F8FAFF")

        h1  = ParagraphStyle("H1",  parent=ss["Title"],   fontSize=20, textColor=DARK, spaceAfter=2)
        h2  = ParagraphStyle("H2",  parent=ss["Heading2"],fontSize=13, textColor=IND,  spaceAfter=6, spaceBefore=14)
        bod = ParagraphStyle("Bod", parent=ss["Normal"],   fontSize=10, textColor=DARK, spaceAfter=4, leading=14)
        mut = ParagraphStyle("Mut", parent=ss["Normal"],   fontSize=9,  textColor=GRAY)
        tip = ParagraphStyle("Tip", parent=ss["Normal"],   fontSize=10, textColor=DARK, spaceAfter=6, leading=14, leftIndent=12)

        story = []
        story.append(Paragraph("Sentiment Lens", mut))
        story.append(Paragraph(f"Monthly Report — {report.get('month','')}", h1))
        story.append(Paragraph(f"{report.get('place_name','')}  ·  Generated {report.get('generated','')}", mut))
        story.append(HRFlowable(width="100%", thickness=1, color=IND, spaceAfter=12))

        story.append(Paragraph("This Month vs Last Month", h2))
        m    = report.get("metrics", {})
        curr = report.get("curr", {})
        prev = report.get("prev", {})

        def _arr(key):
            d = m.get(key, {})
            if d.get("direction") == "up":   return "▲"
            if d.get("direction") == "down": return "▼"
            return "—"

        tbl_data = [
            ["Metric",        "This Month",                              "Last Month",                              "Change"],
            ["Reviews",       str(int(curr.get("total",0))),             str(int(prev.get("total",0))),
             f'{_arr("total")} {abs(m.get("total",{}).get("delta",0)):.0f}'],
            ["% Positive",    f'{curr.get("pct_positive",0):.1f}%',     f'{prev.get("pct_positive",0):.1f}%',
             f'{_arr("pct_positive")} {abs(m.get("pct_positive",{}).get("delta",0)):.1f}%'],
            ["% Negative",    f'{curr.get("pct_negative",0):.1f}%',     f'{prev.get("pct_negative",0):.1f}%',
             f'{_arr("pct_negative")} {abs(m.get("pct_negative",{}).get("delta",0)):.1f}%'],
            ["Sentiment",     f'{curr.get("avg_compound",0):+.3f}',     f'{prev.get("avg_compound",0):+.3f}',
             f'{_arr("avg_compound")} {abs(m.get("avg_compound",{}).get("delta",0)):.3f}'],
            ["Avg Rating",    f'{curr.get("avg_rating",0):.2f} ★',      f'{prev.get("avg_rating",0):.2f} ★',
             f'{_arr("avg_rating")} {abs(m.get("avg_rating",{}).get("delta",0)):.2f}'],
        ]
        t = Table(tbl_data, colWidths=[5*cm,3.5*cm,3.5*cm,4*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),IND),("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),10),
            ("TEXTCOLOR",(0,1),(-1,-1),DARK),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[LGT,colors.white]),
            ("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#E2E8F0")),
            ("ROWHEIGHT",(0,0),(-1,-1),18),("LEFTPADDING",(0,0),(-1,-1),8),
        ]))
        story.append(t)

        pos_kw = curr.get("top_positive", [])
        neg_kw = curr.get("top_negative", [])
        if pos_kw or neg_kw:
            story.append(Paragraph("Keyword Signals This Month", h2))
            kw_rows = [["✅ What customers praised","❌ What customers complained about"]]
            for i in range(max(len(pos_kw),len(neg_kw),1)):
                kw_rows.append([pos_kw[i] if i<len(pos_kw) else "",
                                 neg_kw[i] if i<len(neg_kw) else ""])
            kt = Table(kw_rows, colWidths=[8*cm,8*cm])
            kt.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#F0FDF4")),
                ("TEXTCOLOR",(0,0),(0,0),GRN),("TEXTCOLOR",(1,0),(1,0),RED),
                ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),9),
                ("TEXTCOLOR",(0,1),(-1,-1),DARK),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[LGT,colors.white]),
                ("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#E2E8F0")),
                ("ROWHEIGHT",(0,0),(-1,-1),16),("LEFTPADDING",(0,0),(-1,-1),8),
            ]))
            story.append(kt)

        suggestions = report.get("suggestions", [])
        if suggestions:
            story.append(Paragraph("AI Improvement Recommendations", h2))
            for i, s in enumerate(suggestions, 1):
                story.append(Paragraph(f"{i}. {s}", tip))

        story.append(Spacer(1, 0.5*cm))
        story.append(HRFlowable(width="100%", thickness=0.5, color=GRAY))
        story.append(Paragraph(
            "Sentiment Lens · Monthly Report · AI by Groq · Sentiment by VADER", mut))
        doc.build(story)
        return buf.getvalue()

    except ImportError:
        r   = report
        txt = f"Sentiment Lens Monthly Report\n{r.get('place_name','')}\nMonth: {r.get('month','')}\n"
        txt += f"Reviews: {r.get('curr',{}).get('total',0)}\n"
        txt += f"Positive: {r.get('curr',{}).get('pct_positive',0):.1f}%\n"
        txt += f"Negative: {r.get('curr',{}).get('pct_negative',0):.1f}%\n\n"
        for s in r.get("suggestions", []):
            txt += f"• {s}\n"
        return txt.encode("utf-8")


def send_report_email(to_email: str, place_name: str, pdf_bytes: bytes, month: str) -> tuple[bool, str]:
    """Send monthly report email via Resend (preferred) or SMTP."""
    resend_key = os.getenv("RESEND_API_KEY", "")
    if resend_key:
        return _send_via_resend(to_email, place_name, pdf_bytes, month, resend_key)
    smtp_host = os.getenv("SMTP_HOST", "")
    if smtp_host:
        return _send_via_smtp(to_email, place_name, pdf_bytes, month)
    return False, "No email service configured. Add RESEND_API_KEY to .env (free at resend.com)."


def _send_via_resend(to, place, pdf, month, key):
    import requests as _req, base64
    try:
        r = _req.post("https://api.resend.com/emails",
            headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},
            json={
                "from":    os.getenv("REPORT_FROM_EMAIL","reports@resend.dev"),
                "to":      [to],
                "subject": f"📊 {place} — Monthly Sentiment Report ({month})",
                "html": (f"<h2>Monthly Sentiment Report</h2><p>Your report for <strong>{place}</strong>"
                         f" ({month}) is attached.</p><p>— Sentiment Lens</p>"),
                "attachments":[{"filename":f"{place.replace(' ','_')}_{month.replace(' ','_')}.pdf",
                                "content": base64.b64encode(pdf).decode()}],
            }, timeout=20)
        if r.status_code in (200,201):
            return True, f"✅ Report emailed to {to}"
        return False, f"Resend error: {r.json().get('message','unknown')}"
    except Exception as e:
        return False, f"Email failed: {e}"


def _send_via_smtp(to, place, pdf, month):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.base      import MIMEBase
    from email.mime.text      import MIMEText
    from email                import encoders
    try:
        host=os.getenv("SMTP_HOST",""); port=int(os.getenv("SMTP_PORT","587"))
        user=os.getenv("SMTP_USER",""); passwd=os.getenv("SMTP_PASS","")
        frm=os.getenv("SMTP_FROM",user)
        msg=MIMEMultipart(); msg["From"]=frm; msg["To"]=to
        msg["Subject"]=f"📊 {place} — Monthly Sentiment Report ({month})"
        msg.attach(MIMEText(f"Hi,\n\nYour monthly report for {place} ({month}) is attached.\n\n— Sentiment Lens","plain"))
        att=MIMEBase("application","pdf"); att.set_payload(pdf); encoders.encode_base64(att)
        att.add_header("Content-Disposition",f"attachment; filename={place.replace(' ','_')}_report.pdf")
        msg.attach(att)
        with smtplib.SMTP(host,port) as s:
            s.starttls(); s.login(user,passwd); s.sendmail(frm,to,msg.as_string())
        return True, f"✅ Report emailed to {to}"
    except Exception as e:
        return False, f"SMTP error: {e}"
