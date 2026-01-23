from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

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
    BALANCES_FOR_RUN,
)

TZ = ZoneInfo(TIMEZONE or "America/New_York")


def to_decimal(value):
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


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
        balances = fetch_all(conn, BALANCES_FOR_RUN, (run_id,))
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
        "balances": balances,
        "transactions": txs,
    }