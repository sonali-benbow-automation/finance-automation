from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from config import TIMEZONE
from db.db import db_conn
from notify.queries import (
    NET_WORTH_LATEST,
    TODAY_TOTALS,
    WTD_TOTALS,
    MTD_TOTALS,
    POSTED_TRANSACTIONS_LATEST_RUN,
)

TZ = ZoneInfo(TIMEZONE or "America/New_York")


class Totals:
    def __init__(self, spent, received):
        self.spent = spent
        self.received = received


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


def format_tx_line(tx):
    name = (tx.get("merchant_name") or tx.get("name") or "").strip()
    amount = to_decimal(tx.get("amount"))
    if amount < 0:
        amount_str = f"+{format_money(-amount)}"
    else:
        amount_str = format_money(amount)
    return f"• {name} — {amount_str}"


def build_daily_summary_text(max_tx_lines=10, include_transactions=False):
    now_local = datetime.now(TZ)
    date_label = now_local.strftime("%Y-%m-%d")
    with db_conn() as conn:
        today = fetch_one(conn, TODAY_TOTALS)
        wtd = fetch_one(conn, WTD_TOTALS)
        mtd = fetch_one(conn, MTD_TOTALS)
        net = fetch_one(conn, NET_WORTH_LATEST)
        transactions = []
        if include_transactions:
            transactions = fetch_all(conn, POSTED_TRANSACTIONS_LATEST_RUN)
    today_totals = Totals(
        to_decimal(today.get("today_spent")),
        to_decimal(today.get("today_received")),
    )
    wtd_totals = Totals(
        to_decimal(wtd.get("wtd_spent")),
        to_decimal(wtd.get("wtd_received")),
    )
    mtd_totals = Totals(
        to_decimal(mtd.get("mtd_spent")),
        to_decimal(mtd.get("mtd_received")),
    )
    net_worth = to_decimal(net.get("net_worth"))
    lines = []
    lines.append(f"DAILY FINANCE SUMMARY: {date_label}")
    lines.append(
        f"DAILY Spent: {format_money(today_totals.spent)} and "
        f"Received: {format_money(today_totals.received)} | "
        f"WEEKLY Spent: {format_money(wtd_totals.spent)} and "
        f"Received: {format_money(wtd_totals.received)} | "
        f"MONTHLY Spent: {format_money(mtd_totals.spent)} and "
        f"Received: {format_money(mtd_totals.received)}"
    )
    lines.append(f"NET WORTH LATEST: {format_money(net_worth)}")
    if include_transactions and transactions:
        lines.append("")
        lines.append("RECENT POSTED TRANSACTIONS:")
        for tx in transactions[:max_tx_lines]:
            lines.append(format_tx_line(tx))
    return "\n".join(lines)


if __name__ == "__main__":
    print(build_daily_summary_text(include_transactions=True))