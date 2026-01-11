from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
import html

from config import TIMEZONE
from db.db import db_conn
from notify.queries import (
    RUN_META,
    NET_WORTH_FOR_RUN,
    TODAY_TOTALS_FOR_RUN,
    WTD_TOTALS,
    MTD_TOTALS,
    YTD_TOTALS,
    POSTED_TRANSACTIONS_FOR_RUN,
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


def format_tx_name(tx):
    name = (tx.get("merchant_name") or tx.get("name") or "").strip()
    return name or "(unknown)"


def build_daily_summary_data(run_id, include_transactions=True):
    now_local = datetime.now(TZ)
    generated_label = now_local.strftime("%Y-%m-%d %H:%M %Z")
    with db_conn() as conn:
        meta = fetch_one(conn, RUN_META, (run_id,))
        today = fetch_one(conn, TODAY_TOTALS_FOR_RUN, (run_id,))
        wtd = fetch_one(conn, WTD_TOTALS)
        mtd = fetch_one(conn, MTD_TOTALS)
        ytd = fetch_one(conn, YTD_TOTALS)
        net = fetch_one(conn, NET_WORTH_FOR_RUN, (run_id,))
        txs = fetch_all(conn, POSTED_TRANSACTIONS_FOR_RUN, (run_id,)) if include_transactions else []
    return {
        "run_id": run_id,
        "generated_label": generated_label,
        "run_status": meta.get("status"),
        "today_spent": to_decimal(today.get("today_spent")),
        "today_received": to_decimal(today.get("today_received")),
        "wtd_spent": to_decimal(wtd.get("wtd_spent")),
        "wtd_received": to_decimal(wtd.get("wtd_received")),
        "mtd_spent": to_decimal(mtd.get("mtd_spent")),
        "mtd_received": to_decimal(mtd.get("mtd_received")),
        "ytd_spent": to_decimal(ytd.get("ytd_spent")),
        "ytd_received": to_decimal(ytd.get("ytd_received")),
        "net_worth": to_decimal(net.get("net_worth")),
        "transactions": txs,
    }


def build_daily_summary_html(run_id, include_transactions=True):
    d = build_daily_summary_data(run_id, include_transactions)

    def esc(x):
        return html.escape("" if x is None else str(x))

    def money(x):
        return esc(format_money(x))

    def net_cell(x):
        color = "#1a7f37" if x >= 0 else "#b00020"
        sign = "" if x < 0 else "+"
        return f'<span style="color:{color};">{sign}{money(abs(x))}</span>'

    base = "font-family: Arial, Helvetica, sans-serif; font-size:12px; color:#111;"
    title = "font-size:16px; font-weight:800;"
    section = "font-size:12px; font-weight:800; text-align:left;"
    table = "border-collapse:collapse; width:auto; font-size:12px; table-layout:auto;"
    th = "border:1px solid #333; background:#e9ecef; padding:1px 3px; font-weight:700; text-align:center; white-space:nowrap;"
    td = "border:1px solid #333; padding:1px 3px; white-space:nowrap;"
    td_r = "border:1px solid #333; padding:1px 3px; text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap;"
    zebra = "background:#f7f7f7;"

    delta_net = d["today_received"] - d["today_spent"]

    rollup_rows = [
        ("WEEK-TO-DATE", d["wtd_spent"], d["wtd_received"]),
        ("MONTH-TO-DATE", d["mtd_spent"], d["mtd_received"]),
        ("YEAR-TO-DATE", d["ytd_spent"], d["ytd_received"]),
    ]

    rollup_html = []
    for i, (label, spent, received) in enumerate(rollup_rows):
        rollup_html.append(f"""
        <tr style="{zebra if i % 2 else ''}">
          <td style="{td} font-weight:800;">{label}</td>
          <td style="{td_r}">{money(spent)}</td>
          <td style="{td_r}">{money(received)}</td>
          <td style="{td_r}">{net_cell(received - spent)}</td>
        </tr>
        """)

    tx_rows = []
    for i, tx in enumerate(d["transactions"] or []):
        amt = to_decimal(tx["amount"])
        spent = amt if amt > 0 else Decimal("0")
        received = -amt if amt < 0 else Decimal("0")
        tx_rows.append(f"""
        <tr style="{zebra if i % 2 else ''}">
          <td style="{td}">{esc(tx.get('date'))}</td>
          <td style="{td}">{esc(tx.get('item_label'))}</td>
          <td style="{td}">{esc(tx.get('account_name'))}</td>
          <td style="{td}">{esc(format_tx_name(tx))}</td>
          <td style="{td_r}">{money(spent) if spent else ""}</td>
          <td style="{td_r}">{money(received) if received else ""}</td>
          <td style="{td_r}">{net_cell(received - spent)}</td>
        </tr>
        """)

    return f"""
    <html>
    <body style="{base}; margin:0; padding:8px;">
      <div style="max-width:1000px; margin:0 auto;">
        <div style="{title}">DAILY FINANCE SUMMARY</div>
        <div><strong>RUN ID:</strong> {esc(d["run_id"])}</div>
        <div><strong>RUN STATUS:</strong> {esc(d["run_status"])}</div>
        <div><strong>GENERATED:</strong> {esc(d["generated_label"])}</div>

        <div style="{section}; margin-top:12px;">TODAY (DELTA FOR THIS RUN)</div>
        <table style="{table}">
          <thead>
            <tr>
              <th style="{th}">SPENT</th>
              <th style="{th}">RECEIVED</th>
              <th style="{th}">NET</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td style="{td_r}">{money(d["today_spent"])}</td>
              <td style="{td_r}">{money(d["today_received"])}</td>
              <td style="{td_r}">{net_cell(delta_net)}</td>
            </tr>
          </tbody>
        </table>

        <div style="{section}; margin-top:12px;">ROLLUPS (AS OF GENERATED TIME)</div>
        <table style="{table}">
          <thead>
            <tr>
              <th style="{th}">PERIOD</th>
              <th style="{th}">SPENT</th>
              <th style="{th}">RECEIVED</th>
              <th style="{th}">NET</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rollup_html)}
          </tbody>
        </table>

        <div style="margin-top:12px;"><strong>NET WORTH:</strong> {money(d["net_worth"])}</div>

        <div style="{section}; margin-top:12px;">TRANSACTIONS (DELTA FOR THIS RUN)</div>
        <table style="{table}">
          <thead>
            <tr>
              <th style="{th}">DATE</th>
              <th style="{th}">ITEM</th>
              <th style="{th}">ACCOUNT</th>
              <th style="{th}">NAME</th>
              <th style="{th}">SPENT</th>
              <th style="{th}">RECEIVED</th>
              <th style="{th}">NET</th>
            </tr>
          </thead>
          <tbody>
            {''.join(tx_rows)}
          </tbody>
        </table>
      </div>
    </body>
    </html>
    """