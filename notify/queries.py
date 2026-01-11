from config import TABLES, TIMEZONE

RUNS_TABLE = TABLES["runs"]
PLAID_ITEMS_TABLE = TABLES["plaid_items"]
ACCOUNTS_TABLE = TABLES["accounts"]
BALANCE_SNAPSHOTS_TABLE = TABLES["balance_snapshots"]
TRANSACTIONS_TABLE = TABLES["transactions"]

SQL_TZ = TIMEZONE or "America/New_York"


RUN_META = f"""
select
  id as run_id,
  run_type,
  env,
  started_at,
  finished_at,
  status,
  error
from {RUNS_TABLE}
where id = %s;
"""


NET_WORTH_FOR_RUN = f"""
select
  coalesce(sum(
    case
      when a.type in ('credit', 'loan') then -abs(coalesce(bs.current, 0))
      else coalesce(bs.current, 0)
    end
  ), 0) as net_worth
from {BALANCE_SNAPSHOTS_TABLE} bs
join {ACCOUNTS_TABLE} a
  on a.id = bs.account_pk
where bs.run_id = %s
  and a.include_in_app = true
  and a.active = true;
"""


TODAY_TOTALS_FOR_RUN = f"""
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as today_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as today_received
from {TRANSACTIONS_TABLE} t
join {ACCOUNTS_TABLE} a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.last_seen_run_id = %s;
"""


POSTED_TRANSACTIONS_FOR_RUN = f"""
select
  t.date,
  t.name,
  t.merchant_name,
  t.amount,
  a.account_id,
  a.name as account_name,
  pi.label as item_label,
  t.sync_status
from {TRANSACTIONS_TABLE} t
join {ACCOUNTS_TABLE} a
  on a.id = t.account_pk
join {PLAID_ITEMS_TABLE} pi
  on pi.id = a.plaid_item_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.last_seen_run_id = %s
order by t.date desc, t.amount desc;
"""


WTD_TOTALS = f"""
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as wtd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as wtd_received
from {TRANSACTIONS_TABLE} t
join {ACCOUNTS_TABLE} a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('week', (now() at time zone '{SQL_TZ}'))::date
  and t.date <= (now() at time zone '{SQL_TZ}')::date;
"""


MTD_TOTALS = f"""
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as mtd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as mtd_received
from {TRANSACTIONS_TABLE} t
join {ACCOUNTS_TABLE} a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('month', (now() at time zone '{SQL_TZ}'))::date
  and t.date <= (now() at time zone '{SQL_TZ}')::date;
"""


YTD_TOTALS = f"""
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as ytd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as ytd_received
from {TRANSACTIONS_TABLE} t
join {ACCOUNTS_TABLE} a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('year', (now() at time zone '{SQL_TZ}'))::date
  and t.date <= (now() at time zone '{SQL_TZ}')::date;
"""