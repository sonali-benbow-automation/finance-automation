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


BALANCES_WITH_PREV_FOR_RUN = f"""
with current_run as (
  select id, env
  from runs
  where id = %s
),
prior_run as (
  select r.id as prior_run_id
  from runs r
  join current_run cr on cr.env = r.env
  where r.run_type = 'daily_sync'
    and r.status = 'success'
    and r.id < cr.id
  order by r.id desc
  limit 1
),
current_per_account as (
  select
    bs.account_pk,
    coalesce(nullif(a.official_name, ''), nullif(a.name, ''), a.account_id) as account_name,
    a.type as account_type,
    a.subtype as account_subtype,
    case
      when a.type in ('credit', 'loan') then -abs(coalesce(bs.current, 0))
      else coalesce(bs.current, 0)
    end as current_signed
  from balance_snapshots bs
  join accounts a
    on a.id = bs.account_pk
  join current_run cr
    on true
  where bs.run_id = cr.id
    and a.include_in_app = true
    and a.active = true
),
prior_per_account as (
  select
    bs.account_pk,
    case
      when a.type in ('credit', 'loan') then -abs(coalesce(bs.current, 0))
      else coalesce(bs.current, 0)
    end as prior_signed
  from balance_snapshots bs
  join accounts a
    on a.id = bs.account_pk
  join prior_run pr
    on pr.prior_run_id = bs.run_id
  where a.include_in_app = true
    and a.active = true
),
joined as (
  select
    c.account_name,
    c.account_type,
    c.account_subtype,
    c.current_signed,
    p.prior_signed,
    (c.current_signed - coalesce(p.prior_signed, 0)) as delta_signed,
    case
      when p.prior_signed is null then null
      when abs(p.prior_signed) = 0 then null
      else (c.current_signed - p.prior_signed) / abs(p.prior_signed)
    end as pct_change_abs
  from current_per_account c
  left join prior_per_account p
    on p.account_pk = c.account_pk
),
unioned as (
  select
    'account' as row_type,
    account_name,
    account_type,
    account_subtype,
    current_signed,
    prior_signed,
    delta_signed,
    pct_change_abs,
    0 as sort_key
  from joined

  union all

  select
    'total' as row_type,
    'net_worth' as account_name,
    null as account_type,
    null as account_subtype,
    coalesce(sum(current_signed), 0) as current_signed,
    coalesce(sum(prior_signed), 0) as prior_signed,
    coalesce(sum(current_signed), 0) - coalesce(sum(prior_signed), 0) as delta_signed,
    case
      when coalesce(sum(prior_signed), 0) = 0 then null
      else (coalesce(sum(current_signed), 0) - coalesce(sum(prior_signed), 0)) / abs(coalesce(sum(prior_signed), 0))
    end as pct_change_abs,
    1 as sort_key
  from joined
)
select
  row_type,
  account_name,
  account_type,
  account_subtype,
  current_signed,
  prior_signed,
  delta_signed,
  pct_change_abs
from unioned
order by
  sort_key,
  account_type nulls last,
  account_subtype nulls last,
  account_name;
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