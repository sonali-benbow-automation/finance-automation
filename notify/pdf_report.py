from io import BytesIO
from decimal import Decimal

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

from notify.daily_summary import build_daily_summary_data


def to_decimal(x):
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def money_plain(x):
    v = to_decimal(x).quantize(Decimal("0.01"))
    return f"{v:,.2f}"


def signed_money_plain(x):
    v = to_decimal(x).quantize(Decimal("0.01"))
    if v > 0:
        return f"+{v:,.2f}"
    return f"{v:,.2f}"


def pct_plain(x):
    if x is None:
        return ""
    try:
        v = Decimal(str(x)) * Decimal("100")
        v = v.quantize(Decimal("0.01"))
        if v > 0:
            return f"+{v}%"
        return f"{v}%"
    except Exception:
        return ""


def truncate(s, n):
    s = "" if s is None else str(s)
    if len(s) <= n:
        return s
    if n <= 0:
        return ""
    if n == 1:
        return s[:1]
    return s[: n - 1] + "…"


def make_table(data, col_widths, numeric_cols=None):
    if numeric_cols is None:
        numeric_cols = set()
    t = Table(data, colWidths=col_widths, repeatRows=1, hAlign="LEFT")
    style_cmds = [
        ("FONTNAME", (0, 0), (-1, -1), "Courier"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.0),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E9ECEF")),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#333333")),
        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#333333")),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    for col in range(len(data[0])):
        style_cmds.append(("ALIGN", (col, 0), (col, 0), "CENTER"))
        if col in numeric_cols:
            style_cmds.append(("ALIGN", (col, 1), (col, -1), "RIGHT"))
        else:
            style_cmds.append(("ALIGN", (col, 1), (col, -1), "LEFT"))
    for r in range(1, len(data)):
        if r % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, r), (-1, r), colors.HexColor("#F7F7F7")))
    t.setStyle(TableStyle(style_cmds))
    return t


def make_balances_with_prev_table(rows):
    data = [["account", "type", "subtype", "current", "prev", "delta", "pct"]]
    for r in rows:
        row_type = (r.get("row_type") or "").lower()
        name = r.get("account_name")
        if row_type == "total":
            name = "net_worth"
        cur = r.get("current_signed") if "current_signed" in r else r.get("signed_current")
        prev = r.get("prior_signed") if "prior_signed" in r else r.get("signed_prior")
        delta = r.get("delta_signed") if "delta_signed" in r else r.get("signed_delta")
        pct = r.get("pct_change_abs") if "pct_change_abs" in r else r.get("signed_pct_change_abs")
        data.append(
            [
                truncate(name, 22),
                truncate(r.get("account_type"), 8),
                truncate(r.get("account_subtype"), 10),
                signed_money_plain(cur),
                signed_money_plain(prev),
                signed_money_plain(delta),
                pct_plain(pct),
            ]
        )
    t = make_table(
        data,
        col_widths=[
            2.05 * inch,
            0.90 * inch,
            1.05 * inch,
            0.95 * inch,
            0.95 * inch,
            0.95 * inch,
            0.70 * inch,
        ],
        numeric_cols={3, 4, 5, 6},
    )
    total_row_idx = None
    for i in range(1, len(rows) + 1):
        if (rows[i - 1].get("row_type") or "").lower() == "total":
            total_row_idx = i
            break
    if total_row_idx is not None:
        t.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, total_row_idx), (-1, total_row_idx), "Courier-Bold"),
                    ("BACKGROUND", (0, total_row_idx), (-1, total_row_idx), colors.HexColor("#E9ECEF")),
                ]
            )
        )
    return t


def pick_keys_for_totals(row):
    if not row:
        return None
    if "true_spend" in row or "true_spend_prev" in row:
        return {
            "spend": ("true_spend", "true_spend_prev", "true_spend_delta", "true_spend_pct_change_abs"),
            "income": ("true_income", "true_income_prev", "true_income_delta", "true_income_pct_change_abs"),
            "savings": ("savings", "savings_prev", "savings_delta", "savings_pct_change_abs"),
        }
    return None


def make_period_totals_table(label, row):
    keys = pick_keys_for_totals(row)
    if not keys:
        data = [["metric", "current", "prev", "delta", "pct"], ["(no data)", "", "", "", ""]]
        return make_table(
            data,
            col_widths=[1.65 * inch, 1.15 * inch, 1.15 * inch, 1.05 * inch, 0.70 * inch],
            numeric_cols={1, 2, 3, 4},
        )

    data = [["metric", "current", "prev", "delta", "pct"]]
    for metric_name in ["spend", "income", "savings"]:
        cur_k, prev_k, delta_k, pct_k = keys[metric_name]
        cur_v = row.get(cur_k)
        prev_v = row.get(prev_k)
        delta_v = row.get(delta_k)
        pct_v = row.get(pct_k)
        data.append(
            [
                f"{label}_{metric_name}",
                money_plain(cur_v),
                money_plain(prev_v),
                signed_money_plain(delta_v),
                pct_plain(pct_v),
            ]
        )

    return make_table(
        data,
        col_widths=[1.65 * inch, 1.15 * inch, 1.15 * inch, 1.05 * inch, 0.70 * inch],
        numeric_cols={1, 2, 3, 4},
    )


def make_source_coverage_table(rows):
    data = [["classification", "source", "tx_count", "abs_amount", "pct"]]
    for r in rows or []:
        data.append(
            [
                truncate(r.get("classification"), 14),
                truncate(r.get("classification_source"), 12),
                str(r.get("tx_count") or ""),
                money_plain(r.get("abs_amount_sum") or 0),
                pct_plain(r.get("pct_of_class_abs_amount")),
            ]
        )
    if len(data) == 1:
        data.append(["(none)", "", "", "", ""])
    return make_table(
        data,
        col_widths=[1.40 * inch, 1.25 * inch, 0.85 * inch, 1.10 * inch, 0.65 * inch],
        numeric_cols={2, 3, 4},
    )


def build_daily_summary_pdf(run_id):
    d = build_daily_summary_data(run_id)
    balances = d.get("balances") or []
    txs = d.get("transactions") or []
    today_with_prev = d.get("today_with_prev") or {}
    wtd_with_prev = d.get("wtd_with_prev") or {}
    mtd_with_prev = d.get("mtd_with_prev") or {}
    ytd_with_prev = d.get("ytd_with_prev") or {}
    source_breakdown = d.get("classification_source_breakdown") or []
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.45 * inch,
        rightMargin=0.45 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
    )
    styles = getSampleStyleSheet()
    mono = ParagraphStyle(
        "mono",
        parent=styles["Normal"],
        fontName="Courier",
        fontSize=9,
        leading=11,
    )
    mono_bold = ParagraphStyle(
        "mono_bold",
        parent=mono,
        fontName="Courier-Bold",
        fontSize=10,
        leading=12,
    )
    story = []
    story.append(Paragraph("daily_finance_summary", mono_bold))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"run_id: {d.get('run_id')}", mono))
    story.append(Paragraph(f"run_status: {d.get('run_status')}", mono))
    story.append(Paragraph(f"generated: {d.get('generated_label')}", mono))
    story.append(Spacer(1, 12))
    story.append(Paragraph("true_totals_with_prev", mono_bold))
    story.append(Spacer(1, 4))
    story.append(KeepTogether(make_period_totals_table("today", today_with_prev)))
    story.append(Spacer(1, 8))
    story.append(KeepTogether(make_period_totals_table("wtd", wtd_with_prev)))
    story.append(Spacer(1, 8))
    story.append(KeepTogether(make_period_totals_table("mtd", mtd_with_prev)))
    story.append(Spacer(1, 8))
    story.append(KeepTogether(make_period_totals_table("ytd", ytd_with_prev)))
    story.append(Spacer(1, 14))
    story.append(Paragraph("account_balances_with_prev", mono_bold))
    story.append(Spacer(1, 4))
    if not balances:
        story.append(Paragraph("No balances for this run.", mono))
    else:
        story.append(KeepTogether(make_balances_with_prev_table(balances)))
    story.append(Spacer(1, 14))
    story.append(Paragraph("classification_source_coverage", mono_bold))
    story.append(Spacer(1, 4))
    story.append(KeepTogether(make_source_coverage_table(source_breakdown)))
    story.append(Spacer(1, 14))
    story.append(Paragraph("transactions_delta", mono_bold))
    story.append(Spacer(1, 4))
    if not txs:
        story.append(Paragraph("No posted transactions for this run.", mono))
    else:
        tx_tbl = [["date", "item", "account", "name", "spent", "received", "net", "class", "axis", "src"]]
        for tx in txs:
            amt = to_decimal(tx.get("amount"))
            spent = amt if amt > 0 else Decimal("0")
            received = -amt if amt < 0 else Decimal("0")
            net = received - spent
            tx_tbl.append(
                [
                    truncate(tx.get("date"), 10),
                    truncate(tx.get("item_label"), 8),
                    truncate(tx.get("account_name"), 12),
                    truncate(tx.get("effective_merchant") or tx.get("merchant_name") or tx.get("name"), 20),
                    money_plain(spent) if spent else "",
                    money_plain(received) if received else "",
                    signed_money_plain(net),
                    truncate(tx.get("classification"), 8),
                    truncate(tx.get("behavior_axis"), 10),
                    truncate(tx.get("classification_source"), 10),
                ]
            )
        story.append(
            make_table(
                tx_tbl,
                col_widths=[
                    0.70 * inch,
                    0.70 * inch,
                    1.00 * inch,
                    1.80 * inch,
                    0.70 * inch,
                    0.70 * inch,
                    0.70 * inch,
                    0.65 * inch,
                    0.75 * inch,
                    0.65 * inch,
                ],
                numeric_cols={4, 5, 6},
            )
        )
    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes