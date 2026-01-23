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


def net_plain(x):
    v = to_decimal(x).quantize(Decimal("0.01"))
    if v > 0:
        return f"+{v:,.2f}"
    return f"{v:,.2f}"


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


def make_balances_table(rows):
    data = [["account", "type", "subtype", "balance"]]
    for r in rows:
        row_type = r.get("row_type")
        name = r.get("account_name")
        if row_type == "total":
            name = "net_worth"
        data.append(
            [
                truncate(name, 28),
                truncate(r.get("account_type"), 10),
                truncate(r.get("account_subtype"), 12),
                net_plain(r.get("signed_current")),
            ]
        )
    t = make_table(
        data,
        col_widths=[2.65 * inch, 1.05 * inch, 1.25 * inch, 1.20 * inch],
        numeric_cols={3},
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


def build_daily_summary_pdf(run_id):
    d = build_daily_summary_data(run_id)
    today_spent = to_decimal(d.get("today_spent"))
    today_received = to_decimal(d.get("today_received"))
    today_net = today_received - today_spent
    wtd_spent = to_decimal(d.get("wtd_spent"))
    wtd_received = to_decimal(d.get("wtd_received"))
    wtd_net = wtd_received - wtd_spent
    mtd_spent = to_decimal(d.get("mtd_spent"))
    mtd_received = to_decimal(d.get("mtd_received"))
    mtd_net = mtd_received - mtd_spent
    ytd_spent = to_decimal(d.get("ytd_spent"))
    ytd_received = to_decimal(d.get("ytd_received"))
    ytd_net = ytd_received - ytd_spent
    balances = d.get("balances") or []
    txs = d.get("transactions") or []
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
    story.append(Paragraph("today_delta", mono_bold))
    story.append(Spacer(1, 4))
    today_tbl = [
        ["spent", "received", "net"],
        [money_plain(today_spent), money_plain(today_received), net_plain(today_net)],
    ]
    story.append(
        KeepTogether(
            make_table(
                today_tbl,
                col_widths=[1.45 * inch, 1.45 * inch, 1.45 * inch],
                numeric_cols={0, 1, 2},
            )
        )
    )
    story.append(Spacer(1, 14))
    story.append(Paragraph("rollups", mono_bold))
    story.append(Spacer(1, 4))
    roll_tbl = [
        ["period", "spent", "received", "net"],
        ["week_to_date", money_plain(wtd_spent), money_plain(wtd_received), net_plain(wtd_net)],
        ["month_to_date", money_plain(mtd_spent), money_plain(mtd_received), net_plain(mtd_net)],
        ["year_to_date", money_plain(ytd_spent), money_plain(ytd_received), net_plain(ytd_net)],
    ]
    story.append(
        KeepTogether(
            make_table(
                roll_tbl,
                col_widths=[1.85 * inch, 1.25 * inch, 1.25 * inch, 1.25 * inch],
                numeric_cols={1, 2, 3},
            )
        )
    )
    story.append(Spacer(1, 14))
    story.append(Paragraph("account_balances", mono_bold))
    story.append(Spacer(1, 4))
    if not balances:
        story.append(Paragraph("No balances for this run.", mono))
    else:
        story.append(KeepTogether(make_balances_table(balances)))
    story.append(Spacer(1, 14))
    story.append(Paragraph("transactions_delta", mono_bold))
    story.append(Spacer(1, 4))
    if not txs:
        story.append(Paragraph("No posted transactions for this run.", mono))
    else:
        tx_tbl = [["date", "item", "account", "name", "spent", "received", "net"]]
        for tx in txs:
            amt = to_decimal(tx.get("amount"))
            spent = amt if amt > 0 else Decimal("0")
            received = -amt if amt < 0 else Decimal("0")
            net = received - spent
            tx_tbl.append(
                [
                    truncate(tx.get("date"), 10),
                    truncate(tx.get("item_label"), 10),
                    truncate(tx.get("account_name"), 14),
                    truncate(tx.get("merchant_name") or tx.get("name"), 24),
                    money_plain(spent) if spent else "",
                    money_plain(received) if received else "",
                    net_plain(net),
                ]
            )
        story.append(
            make_table(
                tx_tbl,
                col_widths=[
                    0.75 * inch,
                    0.95 * inch,
                    1.20 * inch,
                    2.15 * inch,
                    0.75 * inch,
                    0.75 * inch,
                    0.75 * inch,
                ],
                numeric_cols={4, 5, 6},
            )
        )
    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes