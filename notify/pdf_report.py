from io import BytesIO
from decimal import Decimal

from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    BaseDocTemplate,
    PageTemplate,
    Frame,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
    KeepTogether,
    PageBreak,
    NextPageTemplate,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

from notify.daily_summary import build_daily_summary_data


def to_decimal_or_none(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def money_plain_or_blank(x):
    v = to_decimal_or_none(x)
    if v is None:
        return ""
    v = v.quantize(Decimal("0.01"))
    return f"{v:,.2f}"


def signed_money_plain_or_blank(x):
    v = to_decimal_or_none(x)
    if v is None:
        return ""
    v = v.quantize(Decimal("0.01"))
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


def make_table(data, col_widths, numeric_cols=None, repeat_header=True):
    if numeric_cols is None:
        numeric_cols = set()

    t = Table(
        data,
        colWidths=col_widths,
        repeatRows=1 if repeat_header else 0,
        hAlign="LEFT",
    )

    style_cmds = [
        ("FONTNAME", (0, 0), (-1, -1), "Courier"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.0),
        ("LEADING", (0, 0), (-1, -1), 10.0),
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


def fit_col_widths_for_page(
    rows,
    max_width_points,
    min_chars_per_col,
    max_chars_per_col,
    font_size=8.0,
):
    if not rows or not rows[0]:
        return []

    cols = len(rows[0])
    max_lens = [0] * cols
    for r in rows:
        for i in range(cols):
            v = "" if r[i] is None else str(r[i])
            max_lens[i] = max(max_lens[i], len(v))

    target_chars = []
    for i in range(cols):
        c = max_lens[i]
        if i < len(min_chars_per_col):
            c = max(c, min_chars_per_col[i])
        ccap = max_chars_per_col[i] if i < len(max_chars_per_col) else c
        c = min(c, ccap)
        target_chars.append(c)

    char_width = font_size * 0.62
    raw_widths = [c * char_width + 10 for c in target_chars]
    total = sum(raw_widths) if raw_widths else 1.0

    if total <= max_width_points:
        return raw_widths

    scale = max_width_points / total
    return [w * scale for w in raw_widths]


def make_balances_with_prev_table(rows, available_width_points):
    data = [["account", "type", "subtype", "current", "prev", "delta", "pct"]]
    for r in rows:
        row_type = (r.get("row_type") or "").lower()
        name = r.get("account_name")
        if row_type == "total":
            name = "net_worth"

        data.append(
            [
                truncate(name, 32),
                truncate(r.get("account_type"), 10),
                truncate(r.get("account_subtype"), 14),
                signed_money_plain_or_blank(r.get("current_signed")),
                signed_money_plain_or_blank(r.get("prior_signed")),
                signed_money_plain_or_blank(r.get("delta_signed")),
                pct_plain(r.get("pct_change_abs")),
            ]
        )

    col_widths = fit_col_widths_for_page(
        data,
        max_width_points=available_width_points,
        min_chars_per_col=[10, 4, 6, 8, 8, 8, 4],
        max_chars_per_col=[32, 10, 14, 12, 12, 12, 8],
        font_size=8.0,
    )

    t = make_table(
        data,
        col_widths=col_widths,
        numeric_cols={3, 4, 5, 6},
        repeat_header=True,
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
            "necessity": ("necessity_spend", "necessity_spend_prev", "necessity_spend_delta", "necessity_spend_pct_change_abs"),
            "discretionary": ("discretionary_spend", "discretionary_spend_prev", "discretionary_spend_delta", "discretionary_spend_pct_change_abs"),
            "income": ("true_income", "true_income_prev", "true_income_delta", "true_income_pct_change_abs"),
            "reimbursements": ("reimbursements", "reimbursements_prev", "reimbursements_delta", "reimbursements_pct_change_abs"),
            "savings": ("savings", "savings_prev", "savings_delta", "savings_pct_change_abs"),
            "savings_rate": ("savings_rate", "savings_rate_prev", "savings_rate_delta", "savings_rate_pct_change_abs"),
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
    order = ["spend", "necessity", "discretionary", "income", "reimbursements", "savings", "savings_rate"]

    for metric_name in order:
        cur_k, prev_k, delta_k, pct_k = keys[metric_name]
        cur_v = row.get(cur_k)
        prev_v = row.get(prev_k)
        delta_v = row.get(delta_k)
        pct_v = row.get(pct_k)

        is_rate = metric_name == "savings_rate"
        data.append(
            [
                f"{label}_{metric_name}",
                pct_plain(cur_v) if is_rate else money_plain_or_blank(cur_v),
                pct_plain(prev_v) if is_rate else money_plain_or_blank(prev_v),
                pct_plain(delta_v) if is_rate else signed_money_plain_or_blank(delta_v),
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
                money_plain_or_blank(r.get("abs_amount_sum")),
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


def make_transactions_appendix_table(rows, available_width_points):
    data = [["date", "account", "effective", "net", "class", "src", "cat", "rule"]]

    for tx in rows or []:
        amt = to_decimal_or_none(tx.get("amount"))
        net = None
        if amt is not None:
            net = -amt

        rule_id = tx.get("matched_rule_id")
        rule_txt = "" if rule_id is None else str(rule_id)

        data.append(
            [
                truncate(tx.get("date"), 10),
                truncate(tx.get("account_name"), 18),
                truncate(tx.get("effective_merchant"), 36),
                signed_money_plain_or_blank(net),
                truncate(tx.get("classification"), 10),
                truncate(tx.get("classification_source"), 10),
                truncate(tx.get("category"), 14),
                truncate(rule_txt, 8),
            ]
        )

    col_widths = fit_col_widths_for_page(
        data,
        max_width_points=available_width_points,
        min_chars_per_col=[10, 10, 18, 8, 6, 6, 6, 4],
        max_chars_per_col=[10, 18, 36, 12, 10, 10, 14, 8],
        font_size=8.0,
    )

    return make_table(
        data,
        col_widths=col_widths,
        numeric_cols={3},
        repeat_header=True,
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

    left_margin = 0.45 * inch
    right_margin = 0.45 * inch
    top_margin = 0.45 * inch
    bottom_margin = 0.45 * inch

    portrait_w, portrait_h = letter
    landscape_w, landscape_h = landscape(letter)

    portrait_frame = Frame(
        left_margin,
        bottom_margin,
        portrait_w - left_margin - right_margin,
        portrait_h - top_margin - bottom_margin,
        id="portrait",
    )
    landscape_frame = Frame(
        left_margin,
        bottom_margin,
        landscape_w - left_margin - right_margin,
        landscape_h - top_margin - bottom_margin,
        id="landscape",
    )

    doc = BaseDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=left_margin,
        rightMargin=right_margin,
        topMargin=top_margin,
        bottomMargin=bottom_margin,
    )

    portrait_template = PageTemplate(id="portrait", frames=[portrait_frame], pagesize=letter)
    landscape_template = PageTemplate(id="landscape", frames=[landscape_frame], pagesize=landscape(letter))
    doc.addPageTemplates([portrait_template, landscape_template])

    styles = getSampleStyleSheet()
    mono = ParagraphStyle("mono", parent=styles["Normal"], fontName="Courier", fontSize=9, leading=11)
    mono_bold = ParagraphStyle("mono_bold", parent=mono, fontName="Courier-Bold", fontSize=10, leading=12)

    available_portrait_width = portrait_w - left_margin - right_margin
    available_landscape_width = landscape_w - left_margin - right_margin

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

    balances_block = []
    balances_block.append(Paragraph("account_balances_with_prev", mono_bold))
    balances_block.append(Spacer(1, 4))
    if not balances:
        balances_block.append(Paragraph("No balances for this run.", mono))
    else:
        balances_block.append(make_balances_with_prev_table(balances, available_portrait_width))
    story.append(KeepTogether(balances_block))
    story.append(Spacer(1, 14))

    story.append(Paragraph("classification_source_coverage", mono_bold))
    story.append(Spacer(1, 4))
    story.append(KeepTogether(make_source_coverage_table(source_breakdown)))
    story.append(Spacer(1, 10))

    story.append(NextPageTemplate("landscape"))
    story.append(PageBreak())

    story.append(Paragraph("transactions_appendix", mono_bold))
    story.append(Spacer(1, 4))
    if not txs:
        story.append(Paragraph("No posted transactions for this run.", mono))
    else:
        story.append(make_transactions_appendix_table(txs, available_landscape_width))

    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes