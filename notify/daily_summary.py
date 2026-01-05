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


def _to_decimal(x):
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _fmt_money(x):
    q = x.quantize(Decimal("0.01"))
    return f"${q:,.2f}"


def _fetch_one(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            return {}
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))


def _fetch_all(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, r)) for r in rows]


def _format_tx_line(tx):
    name = (tx.get("merchant_name") or tx.get("name") or "").strip()
    amt = _to_decimal(tx.get("amount"))
    if amt < 0:
        amt_str = f"+{_fmt_money(-amt)}"
    else:
        amt_str = _fmt_money(amt)
    return f"• {name} — {amt_str}"


def build_daily_summary_text(max_tx_lines=10):
    now_local = datetime.now(TZ)
    date_label = now_local.strftime("%Y-%m-%d")
    with db_conn() as conn:
        today = _fetch_one(conn, TODAY_TOTALS)
        wtd = _fetch_one(conn, WTD_TOTALS)
        mtd = _fetch_one(conn, MTD_TOTALS)
        net = _fetch_one(conn, NET_WORTH_LATEST)
        txs = _fetch_all(conn, POSTED_TRANSACTIONS_LATEST_RUN)
    today_totals = Totals(
        _to_decimal(today.get("today_spent")),
        _to_decimal(today.get("today_received")),
    )
    wtd_totals = Totals(
        _to_decimal(wtd.get("wtd_spent")),
        _to_decimal(wtd.get("wtd_received")),
    )
    mtd_totals = Totals(
        _to_decimal(mtd.get("mtd_spent")),
        _to_decimal(mtd.get("mtd_received")),
    )

    net_worth = _to_decimal(net.get("net_worth"))

    lines = []
    lines.append(f"DAILY FINANCE SUMMARY: {date_label}")
    lines.append(
        f"DAILY Spent: {_fmt_money(today_totals.spent)} and "
        f"Received: {_fmt_money(today_totals.received)} | "
        f"WEEKLY Spent: {_fmt_money(wtd_totals.spent)} and "
        f"Received: {_fmt_money(wtd_totals.received)} | "
        f"MONTHLY Spent: {_fmt_money(mtd_totals.spent)} and "
        f"Received: {_fmt_money(mtd_totals.received)}"
    )
    lines.append(f"NET WORTH LATEST: {_fmt_money(net_worth)}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(build_daily_summary_text())