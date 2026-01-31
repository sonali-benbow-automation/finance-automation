from config import TABLES, TIMEZONE

RUNS_TABLE = TABLES["runs"]
PLAID_ITEMS_TABLE = TABLES["plaid_items"]
ACCOUNTS_TABLE = TABLES["accounts"]
BALANCE_SNAPSHOTS_TABLE = TABLES["balance_snapshots"]
TRANSACTIONS_TABLE = TABLES["transactions"]
MERCHANT_RULES_TABLE = TABLES["merchant_rules"]

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
with classified_transactions as (
  with tx_base as (
    select
      t.id as tx_pk,
      t.transaction_id,
      t.account_pk,
      t.amount,
      t.date,
      t.name,
      t.merchant_name,
      t.pending,
      t.removed,
      t.first_seen_run_id,
      t.last_seen_run_id,
      coalesce(nullif(t.merchant_name, ''), nullif(t.name, '')) as effective_merchant
    from {TRANSACTIONS_TABLE} t
  ),
  matching_rules as (
    select
      b.tx_pk,
      r.id as rule_id,
      r.classification,
      r.behavior_axis,
      r.category,
      r.priority
    from tx_base b
    join {ACCOUNTS_TABLE} a
      on a.id = b.account_pk
    join {PLAID_ITEMS_TABLE} pi
      on pi.id = a.plaid_item_pk
    join {MERCHANT_RULES_TABLE} r
      on r.env = pi.env
     and r.active = true
     and b.effective_merchant is not null
     and (
       (r.match_type = 'ilike'
        and b.effective_merchant ilike ('%' || r.pattern || '%'))
       or
       (r.match_type = 'contains'
        and position(r.pattern in b.effective_merchant) > 0)
       or
       (r.match_type = 'regex'
        and b.effective_merchant ~* r.pattern)
     )
  ),
  ranked_rules as (
    select
      m.*,
      row_number() over (
        partition by m.tx_pk
        order by m.priority asc, m.rule_id asc
      ) as rn
    from matching_rules m
  ),
  best_rule as (
    select
      tx_pk,
      rule_id as matched_rule_id,
      classification,
      behavior_axis,
      category
    from ranked_rules
    where rn = 1
  )
  select
    b.tx_pk,
    b.transaction_id,
    b.account_pk,
    b.amount,
    b.date,
    b.name,
    b.merchant_name,
    b.effective_merchant,
    b.pending,
    b.removed,
    b.first_seen_run_id,
    b.last_seen_run_id,
    br.matched_rule_id,
    br.classification,
    br.behavior_axis,
    br.category
  from tx_base b
  left join best_rule br
    on br.tx_pk = b.tx_pk
)
select
  ct.date,
  ct.name,
  ct.merchant_name,
  ct.effective_merchant,
  ct.amount,
  a.account_id,
  a.name as account_name,
  pi.label as item_label,
  ct.classification,
  ct.behavior_axis,
  ct.category,
  ct.matched_rule_id
from classified_transactions ct
join {ACCOUNTS_TABLE} a
  on a.id = ct.account_pk
join {PLAID_ITEMS_TABLE} pi
  on pi.id = a.plaid_item_pk
where a.include_in_app = true
  and a.active = true
  and ct.removed = false
  and coalesce(ct.pending, false) = false
  and ct.last_seen_run_id = %s
order by ct.date desc, ct.amount desc;
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