from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
import html

from config import TIMEZONE
from db.db import db_conn
from notify.queries import (
    NET_WORTH_LATEST,
    TODAY_TOTALS,
    WTD_TOTALS,
    MTD_TOTALS,
    YTD_TOTALS,
    POSTED_TRANSACTIONS_LATEST_RUN,
)

TZ = ZoneInfo(TIMEZONE or "America/New_York")


def to_decimal(value):
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def format_money(value):
    q = value.quantize(Decimal("0.01"))
    return f"${q:,.2f}"


def fetch_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        if row is None:
            return {}
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))


def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, r)) for r in rows]


def format_tx_amount(amount):
    amt = to_decimal(amount)
    if amt < 0:
        return f"+{format_money(-amt)}"
    return format_money(amt)


def format_tx_name(tx):
    name = (tx.get("merchant_name") or tx.get("name") or "").strip()
    return name or "(unknown)"


def build_daily_summary_data(include_transactions=True):
    now_local = datetime.now(TZ)
    date_label = now_local.strftime("%Y-%m-%d")
    with db_conn() as conn:
        today = fetch_one(conn, TODAY_TOTALS)
        wtd = fetch_one(conn, WTD_TOTALS)
        mtd = fetch_one(conn, MTD_TOTALS)
        ytd = fetch_one(conn, YTD_TOTALS)
        net = fetch_one(conn, NET_WORTH_LATEST)
        txs = fetch_all(conn, POSTED_TRANSACTIONS_LATEST_RUN) if include_transactions else []
    data = {
        "date_label": date_label,
        "totals": {
            "today_spent": to_decimal(today.get("today_spent")),
            "today_received": to_decimal(today.get("today_received")),
            "wtd_spent": to_decimal(wtd.get("wtd_spent")),
            "wtd_received": to_decimal(wtd.get("wtd_received")),
            "mtd_spent": to_decimal(mtd.get("mtd_spent")),
            "mtd_received": to_decimal(mtd.get("mtd_received")),
            "ytd_spent": to_decimal(ytd.get("ytd_spent")),
            "ytd_received": to_decimal(ytd.get("ytd_received")),
        },
        "net_worth": to_decimal(net.get("net_worth")),
        "transactions": txs,
    }
    return data


def build_daily_summary_html(include_transactions=True):
    data = build_daily_summary_data(include_transactions=include_transactions)
    t = data["totals"]
    def esc(x):
        return html.escape("" if x is None else str(x))
    def money(x):
        return esc(format_money(x))
    def net_cell(x):
        color = "#1a7f37" if x >= 0 else "#b00020"
        sign = "" if x < 0 else "+"
        return f'<span style="color:{color};">{sign}{money(abs(x))}</span>'
    date_label = esc(data["date_label"])
    generated = esc(datetime.now(TZ).strftime("%Y-%m-%d %H:%M %Z"))
    net_worth = esc(format_money(data["net_worth"]))
    txs = data["transactions"] or []
    base = "font-family: Arial, Helvetica, sans-serif; font-size:12px; color:#111;"
    title = "font-size:16px; font-weight:800;"
    section = "font-size:14px; font-weight:800;"
    table = "border-collapse:collapse; width:auto; font-size:12px; font-family: Arial, Helvetica, sans-serif;"
    th = "border:1px solid #333; background:#e9ecef; padding:3px 6px; font-weight:700; font-size:12px; white-space:nowrap;"
    td = "border:1px solid #333; padding:3px 6px; font-size:12px; white-space:nowrap;"
    td_r = "border:1px solid #333; padding:3px 6px; text-align:right; font-variant-numeric:tabular-nums; font-size:12px; white-space:nowrap;"
    zebra = "background:#f7f7f7;"
    footer = "border:1px solid #333; background:#f0f0f0; padding:6px 8px; font-weight:900;"
    totals_rows = [
        ("Today", t["today_spent"], t["today_received"]),
        ("Week-to-date", t["wtd_spent"], t["wtd_received"]),
        ("Month-to-date", t["mtd_spent"], t["mtd_received"]),
        ("Year-to-date", t["ytd_spent"], t["ytd_received"]),
    ]
    totals_html = []
    for i, (label, spent, received) in enumerate(totals_rows):
        net = received - spent
        totals_html.append(
            f"""
            <tr style="{zebra if i % 2 else ''}">
              <td style="{td} font-weight:800;">{label}</td>
              <td style="{td_r}">{money(spent)}</td>
              <td style="{td_r}">{money(received)}</td>
              <td style="{td_r}">{net_cell(net)}</td>
            </tr>
            """
        )
    tx_html = ""
    if include_transactions:
        if not txs:
            tx_html = f"""
            <div style="margin-top:14px;">
              <div style="{section}">Transactions</div>
              <div>No posted transactions</div>
            </div>
            """
        else:
            spent_sum = Decimal("0")
            received_sum = Decimal("0")
            rows = []
            for i, tx in enumerate(txs):
                amt = to_decimal(tx["amount"])
                spent = amt if amt > 0 else Decimal("0")
                received = -amt if amt < 0 else Decimal("0")
                spent_sum += spent
                received_sum += received
                net = received - spent
                rows.append(
                    f"""
                    <tr style="{zebra if i % 2 else ''}">
                      <td style="{td}">{esc(tx['date'])}</td>
                      <td style="{td}">{esc(tx['item_label'])}</td>
                      <td style="{td}">{esc(tx['account_name'])}</td>
                      <td style="{td}">{esc(format_tx_name(tx))}</td>
                      <td style="{td_r}">{money(spent) if spent else ""}</td>
                      <td style="{td_r}">{money(received) if received else ""}</td>
                      <td style="{td_r}">{net_cell(net)}</td>
                    </tr>
                    """
                )
            tx_html = f"""
            <div style="margin-top:16px;">
              <div style="{section}">Transactions</div>
              <table style="{table}">
                <thead>
                  <tr>
                    <th style="{th}">Date</th>
                    <th style="{th}">Item</th>
                    <th style="{th}">Account</th>
                    <th style="{th}">Name</th>
                    <th style="{th} text-align:right;">Spent</th>
                    <th style="{th} text-align:right;">Received</th>
                    <th style="{th} text-align:right;">Net</th>
                  </tr>
                </thead>
                <tbody>
                  {''.join(rows)}
                </tbody>
                <tfoot>
                  <tr>
                    <td style="{footer}" colspan="4">TOTAL</td>
                    <td style="{footer} text-align:right;">{money(spent_sum)}</td>
                    <td style="{footer} text-align:right;">{money(received_sum)}</td>
                    <td style="{footer} text-align:right;">{net_cell(received_sum - spent_sum)}</td>
                  </tr>
                </tfoot>
              </table>
            </div>
            """
    return f"""
    <html>
      <body style="{base}; margin:0; padding:18px;">
        <div style="max-width:1000px; margin:0 auto;">
          <div style="{title}">Daily Finance Summary</div>
          <div>Date: {date_label}</div>
          <div>Generated: {generated}</div>
          <div style="{section}; margin-top:12px;">Totals</div>
          <table style="{table}">
            <thead>
              <tr>
                <th style="{th}">Period</th>
                <th style="{th} text-align:right;">Spent</th>
                <th style="{th} text-align:right;">Received</th>
                <th style="{th} text-align:right;">Net</th>
              </tr>
            </thead>
            <tbody>
              {''.join(totals_html)}
            </tbody>
          </table>
          <div style="margin-top:10px;">
            <strong>Net Worth:</strong> {net_worth}
          </div>
          {tx_html}
        </div>
      </body>
    </html>
    """