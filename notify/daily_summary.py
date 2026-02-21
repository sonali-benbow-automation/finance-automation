from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from config import TIMEZONE
from db.db import db_conn
from notify.queries import (
    RUN_META,
    BALANCES_WITH_PREV_FOR_RUN,
    TODAY_TOTALS_FOR_RUN,
    TODAY_TOTALS_WITH_PREV_FOR_RUN,
    WTD_TOTALS_WITH_PREV,
    MTD_TOTALS_WITH_PREV,
    YTD_TOTALS_WITH_PREV,
    POSTED_TRANSACTIONS_FOR_RUN,
    CLASSIFICATION_SOURCE_BREAKDOWN_FOR_RUN,
)

TZ = ZoneInfo(TIMEZONE or "America/New_York")


def to_decimal_or_none(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def fetch_one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        if row is None:
            return {}
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]


def normalize_pct_or_none(v):
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def normalize_totals_row(row):
    if not row:
        return {}

    out = dict(row)

    money_fields = [
        "true_spend",
        "true_spend_prev",
        "true_spend_delta",
        "necessity_spend",
        "necessity_spend_prev",
        "necessity_spend_delta",
        "discretionary_spend",
        "discretionary_spend_prev",
        "discretionary_spend_delta",
        "true_income",
        "true_income_prev",
        "true_income_delta",
        "reimbursements",
        "reimbursements_prev",
        "reimbursements_delta",
        "savings",
        "savings_prev",
        "savings_delta",
        "transfers_out",
        "transfers_in",
        "invest_out",
        "invest_in",
        "fees_out",
        "ignored_abs",
    ]

    pct_fields = [
        "true_spend_pct_change_abs",
        "necessity_spend_pct_change_abs",
        "discretionary_spend_pct_change_abs",
        "true_income_pct_change_abs",
        "reimbursements_pct_change_abs",
        "savings_pct_change_abs",
        "savings_rate",
        "savings_rate_prev",
        "savings_rate_delta",
        "savings_rate_pct_change_abs",
    ]

    for k in money_fields:
        if k in out:
            out[k] = to_decimal_or_none(out.get(k))

    for k in pct_fields:
        if k in out:
            out[k] = normalize_pct_or_none(out.get(k))

    return out


def build_daily_summary_data(run_id, include_transactions=True):
    now_local = datetime.now(TZ)
    generated_label = now_local.strftime("%Y-%m-%d %H:%M %Z")

    with db_conn() as conn:
        meta = fetch_one(conn, RUN_META, (run_id,))
        balances = fetch_all(conn, BALANCES_WITH_PREV_FOR_RUN, (run_id,))
        today = fetch_one(conn, TODAY_TOTALS_FOR_RUN, (run_id,))
        today_with_prev = fetch_one(conn, TODAY_TOTALS_WITH_PREV_FOR_RUN, (run_id,))
        wtd_with_prev = fetch_one(conn, WTD_TOTALS_WITH_PREV)
        mtd_with_prev = fetch_one(conn, MTD_TOTALS_WITH_PREV)
        ytd_with_prev = fetch_one(conn, YTD_TOTALS_WITH_PREV)
        source_breakdown = fetch_all(conn, CLASSIFICATION_SOURCE_BREAKDOWN_FOR_RUN, (run_id,))
        txs = fetch_all(conn, POSTED_TRANSACTIONS_FOR_RUN, (run_id,)) if include_transactions else []

    return {
        "run_id": run_id,
        "generated_label": generated_label,
        "run_status": meta.get("status"),
        "balances": balances,
        "today": normalize_totals_row(today),
        "today_with_prev": normalize_totals_row(today_with_prev),
        "wtd_with_prev": normalize_totals_row(wtd_with_prev),
        "mtd_with_prev": normalize_totals_row(mtd_with_prev),
        "ytd_with_prev": normalize_totals_row(ytd_with_prev),
        "classification_source_breakdown": source_breakdown,
        "transactions": txs,
    }