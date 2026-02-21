from config import TABLES, TIMEZONE

RUNS_TABLE = TABLES["runs"]
PLAID_ITEMS_TABLE = TABLES["plaid_items"]
ACCOUNTS_TABLE = TABLES["accounts"]
BALANCE_SNAPSHOTS_TABLE = TABLES["balance_snapshots"]
TRANSACTIONS_TABLE = TABLES["transactions"]
MERCHANT_RULES_TABLE = TABLES["merchant_rules"]
MANUAL_BALANCES_TABLE = TABLES["manual_balances"]
MANUAL_BALANCE_HISTORY_TABLE = TABLES["manual_balance_history"]

SQL_TZ = TIMEZONE or "America/New_York"


UPDATE_MANUAL_BALANCES_TABLE = f"""
insert into
manual_balances (env, key, label, signed_balance, note, updated_at)
values (
  'production',
  'discover_savings',
  'Discover Savings (manual)',
  12345.67,
  'manual balance override',
  now()
)
on conflict (env, key) do update set
  signed_balance = excluded.signed_balance,
  label = excluded.label,
  note = excluded.note,
  updated_at = now();
"""


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
  select id, env, started_at
  from {RUNS_TABLE}
  where id = %s
),
prior_run as (
  select r.id as prior_run_id,
         r.started_at as prior_started_at
  from {RUNS_TABLE} r
  join current_run cr on cr.env = r.env
  where r.run_type = 'daily_sync'
    and r.status = 'success'
    and r.id < cr.id
  order by r.id desc
  limit 1
),

plaid_current_per_account as (
  select
    bs.account_pk,
    coalesce(nullif(a.official_name, ''), nullif(a.name, ''), a.account_id) as account_name,
    a.type as account_type,
    a.subtype as account_subtype,
    case
      when a.type in ('credit', 'loan') then -abs(coalesce(bs.current, 0))
      else coalesce(bs.current, 0)
    end as current_signed
  from {BALANCE_SNAPSHOTS_TABLE} bs
  join {ACCOUNTS_TABLE} a on a.id = bs.account_pk
  join current_run cr on true
  where bs.run_id = cr.id
    and a.include_in_app = true
    and a.active = true
),
plaid_prior_per_account as (
  select
    bs.account_pk,
    case
      when a.type in ('credit', 'loan') then -abs(coalesce(bs.current, 0))
      else coalesce(bs.current, 0)
    end as prior_signed
  from {BALANCE_SNAPSHOTS_TABLE} bs
  join {ACCOUNTS_TABLE} a on a.id = bs.account_pk
  join prior_run pr on pr.prior_run_id = bs.run_id
  where a.include_in_app = true
    and a.active = true
),
plaid_joined as (
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
  from plaid_current_per_account c
  left join plaid_prior_per_account p on p.account_pk = c.account_pk
),

manual_current as (
  select
    mb.env,
    mb.key,
    mb.label as account_name,
    'manual' as account_type,
    mb.key as account_subtype,
    mb.signed_balance as current_signed
  from {MANUAL_BALANCES_TABLE} mb
  join current_run cr on cr.env = mb.env
),
manual_prior as (
  select
    mb.env,
    mb.key,
    h.signed_balance as prior_signed
  from manual_current mb
  left join prior_run pr on true
  left join lateral (
    select h.signed_balance
    from {MANUAL_BALANCE_HISTORY_TABLE} h
    where h.env = mb.env
      and h.key = mb.key
      and pr.prior_started_at is not null
      and h.snapshot_at <= pr.prior_started_at
    order by h.snapshot_at desc
    limit 1
  ) h on true
),
manual_joined as (
  select
    mc.account_name,
    mc.account_type,
    mc.account_subtype,
    mc.current_signed,
    mp.prior_signed,
    (mc.current_signed - coalesce(mp.prior_signed, 0)) as delta_signed,
    case
      when mp.prior_signed is null then null
      when abs(mp.prior_signed) = 0 then null
      else (mc.current_signed - mp.prior_signed) / abs(mp.prior_signed)
    end as pct_change_abs
  from manual_current mc
  left join manual_prior mp
    on mp.env = mc.env and mp.key = mc.key
),

joined as (
  select * from plaid_joined
  union all
  select * from manual_joined
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
    case
      when count(prior_signed) = 0 then null
      else coalesce(sum(prior_signed), 0)
    end as prior_signed,
    case
      when count(prior_signed) = 0 then null
      else coalesce(sum(current_signed), 0) - coalesce(sum(prior_signed), 0)
    end as delta_signed,
    case
      when count(prior_signed) = 0 then null
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


CLASSIFIED_TX_FOR_ENV_CTE = f"""
tx_base as (
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
    t.personal_finance_category,
    coalesce(nullif(t.merchant_name, ''), nullif(t.name, '')) as effective_merchant
  from {TRANSACTIONS_TABLE} t
),
env_scoped as (
  select
    b.*,
    a.account_id,
    a.name as account_name,
    a.type as account_type,
    a.subtype as account_subtype,
    pi.env as item_env,
    pi.label as item_label
  from tx_base b
  join {ACCOUNTS_TABLE} a
    on a.id = b.account_pk
  join {PLAID_ITEMS_TABLE} pi
    on pi.id = a.plaid_item_pk
  where a.include_in_app = true
    and a.active = true
    and b.removed = false
    and coalesce(b.pending, false) = false
),
matching_rules as (
  select
    e.tx_pk,
    r.id as rule_id,
    r.classification as rule_classification,
    r.behavior_axis as rule_behavior_axis,
    r.category as rule_category,
    r.priority
  from env_scoped e
  join {MERCHANT_RULES_TABLE} r
    on r.env = e.item_env
   and r.active = true
   and e.effective_merchant is not null
   and (
     (r.match_type = 'ilike' and e.effective_merchant ilike ('%%' || r.pattern || '%%'))
     or
     (r.match_type = 'contains' and position(r.pattern in e.effective_merchant) > 0)
     or
     (r.match_type = 'regex' and e.effective_merchant ~* r.pattern)
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
    rule_classification,
    rule_behavior_axis,
    rule_category
  from ranked_rules
  where rn = 1
),
pfc as (
  select
    e.tx_pk,
    lower(coalesce(e.personal_finance_category->>'primary', '')) as pfc_primary
  from env_scoped e
),
pfc_mapped as (
  select
    p.tx_pk,
    case
      when p.pfc_primary in ('income', 'payroll', 'bonus', 'benefits') then 'income'
      when p.pfc_primary in ('transfer_in', 'transfer_out') then 'transfer'
      when p.pfc_primary in ('investment', 'investment_income', 'securities_trades') then 'invest'
      when p.pfc_primary in ('bank_fees', 'overdraft', 'interest_charged', 'late_fee') then 'fee'
      when p.pfc_primary = '' then 'unknown'
      else 'expense'
    end as mapped_classification,
    case
      when p.pfc_primary in (
        'groceries',
        'transportation',
        'gas',
        'public_transit',
        'healthcare',
        'medical',
        'pharmacy',
        'insurance',
        'utilities',
        'rent',
        'mortgage',
        'phone',
        'internet',
        'childcare',
        'education',
        'taxes'
      ) then 'necessity'
      when p.pfc_primary = '' then null
      else 'discretionary'
    end as mapped_behavior_axis,
    nullif(p.pfc_primary, '') as mapped_category
  from pfc p
),
classified_tx as (
  select
    e.tx_pk,
    e.transaction_id,
    e.account_pk,
    e.account_id,
    e.account_name,
    e.account_type,
    e.account_subtype,
    e.item_env as env,
    e.item_label,
    e.amount,
    e.date,
    e.name,
    e.merchant_name,
    e.effective_merchant,
    e.first_seen_run_id,
    e.last_seen_run_id,
    br.matched_rule_id,
    coalesce(br.rule_classification, pm.mapped_classification, 'unknown') as classification,
    case
      when coalesce(br.rule_classification, pm.mapped_classification, 'unknown') = 'expense'
        then coalesce(br.rule_behavior_axis, pm.mapped_behavior_axis, 'discretionary')
      else null
    end as behavior_axis,
    coalesce(br.rule_category, pm.mapped_category) as category,
    case
      when br.matched_rule_id is not null then 'rule'
      when pm.mapped_classification is not null then 'pfc_fallback'
      else 'unknown'
    end as classification_source
  from env_scoped e
  left join best_rule br
    on br.tx_pk = e.tx_pk
  left join pfc_mapped pm
    on pm.tx_pk = e.tx_pk
)
"""


TOTALS_AGG_SELECT = """
  coalesce(sum(x.amount) filter (where x.classification = 'expense' and x.amount > 0), 0) as true_spend,
  coalesce(sum(x.amount) filter (where x.classification = 'expense' and x.amount > 0 and x.behavior_axis = 'necessity'), 0) as necessity_spend,
  coalesce(sum(x.amount) filter (where x.classification = 'expense' and x.amount > 0 and x.behavior_axis = 'discretionary'), 0) as discretionary_spend,

  coalesce(sum(-x.amount) filter (where x.classification = 'income' and x.amount < 0), 0) as true_income,
  coalesce(sum(-x.amount) filter (where x.classification = 'cash_in_non_income' and x.amount < 0), 0) as reimbursements,

  (coalesce(sum(-x.amount) filter (where x.classification = 'income' and x.amount < 0), 0)
   - coalesce(sum(x.amount) filter (where x.classification = 'expense' and x.amount > 0), 0)) as savings,

  case
    when coalesce(sum(-x.amount) filter (where x.classification = 'income' and x.amount < 0), 0) = 0 then null
    else
      (coalesce(sum(-x.amount) filter (where x.classification = 'income' and x.amount < 0), 0)
       - coalesce(sum(x.amount) filter (where x.classification = 'expense' and x.amount > 0), 0))
      /
      coalesce(sum(-x.amount) filter (where x.classification = 'income' and x.amount < 0), 0)
  end as savings_rate,

  coalesce(sum(x.amount) filter (where x.classification = 'transfer' and x.amount > 0), 0) as transfers_out,
  coalesce(sum(-x.amount) filter (where x.classification = 'transfer' and x.amount < 0), 0) as transfers_in,

  coalesce(sum(x.amount) filter (where x.classification = 'invest' and x.amount > 0), 0) as invest_out,
  coalesce(sum(-x.amount) filter (where x.classification = 'invest' and x.amount < 0), 0) as invest_in,

  coalesce(sum(x.amount) filter (where x.classification = 'fee' and x.amount > 0), 0) as fees_out,
  coalesce(sum(abs(x.amount)) filter (where x.classification = 'ignore'), 0) as ignored_abs
"""


TOTALS_DELTA_SELECT = """
  cur.true_spend,
  prev.true_spend as true_spend_prev,
  (cur.true_spend - prev.true_spend) as true_spend_delta,
  case
    when prev.true_spend is null then null
    when prev.true_spend = 0 then null
    else (cur.true_spend - prev.true_spend) / abs(prev.true_spend)
  end as true_spend_pct_change_abs,

  cur.necessity_spend,
  prev.necessity_spend as necessity_spend_prev,
  (cur.necessity_spend - prev.necessity_spend) as necessity_spend_delta,
  case
    when prev.necessity_spend is null then null
    when prev.necessity_spend = 0 then null
    else (cur.necessity_spend - prev.necessity_spend) / abs(prev.necessity_spend)
  end as necessity_spend_pct_change_abs,

  cur.discretionary_spend,
  prev.discretionary_spend as discretionary_spend_prev,
  (cur.discretionary_spend - prev.discretionary_spend) as discretionary_spend_delta,
  case
    when prev.discretionary_spend is null then null
    when prev.discretionary_spend = 0 then null
    else (cur.discretionary_spend - prev.discretionary_spend) / abs(prev.discretionary_spend)
  end as discretionary_spend_pct_change_abs,

  cur.true_income,
  prev.true_income as true_income_prev,
  (cur.true_income - prev.true_income) as true_income_delta,
  case
    when prev.true_income is null then null
    when prev.true_income = 0 then null
    else (cur.true_income - prev.true_income) / abs(prev.true_income)
  end as true_income_pct_change_abs,

  cur.reimbursements,
  prev.reimbursements as reimbursements_prev,
  (cur.reimbursements - prev.reimbursements) as reimbursements_delta,
  case
    when prev.reimbursements is null then null
    when prev.reimbursements = 0 then null
    else (cur.reimbursements - prev.reimbursements) / abs(prev.reimbursements)
  end as reimbursements_pct_change_abs,

  cur.savings,
  prev.savings as savings_prev,
  (cur.savings - prev.savings) as savings_delta,
  case
    when prev.savings is null then null
    when prev.savings = 0 then null
    else (cur.savings - prev.savings) / abs(prev.savings)
  end as savings_pct_change_abs,

  cur.savings_rate,
  prev.savings_rate as savings_rate_prev,
  (cur.savings_rate - prev.savings_rate) as savings_rate_delta,
  case
    when prev.savings_rate is null then null
    when abs(prev.savings_rate) = 0 then null
    else (cur.savings_rate - prev.savings_rate) / abs(prev.savings_rate)
  end as savings_rate_pct_change_abs
"""


TODAY_TOTALS_FOR_RUN = f"""
with
{CLASSIFIED_TX_FOR_ENV_CTE}
select
  {TOTALS_AGG_SELECT}
from classified_tx x
where x.last_seen_run_id = %s;
"""


TODAY_TOTALS_WITH_PREV_FOR_RUN = f"""
with current_run as (
  select id, env
  from {RUNS_TABLE}
  where id = %s
),
prior_run as (
  select r.id as prior_run_id
  from {RUNS_TABLE} r
  join current_run cr on cr.env = r.env
  where r.run_type = 'daily_sync'
    and r.status = 'success'
    and r.id < cr.id
  order by r.id desc
  limit 1
),
{CLASSIFIED_TX_FOR_ENV_CTE},
scoped as (
  select 'current' as scope, x.*
  from classified_tx x
  join current_run cr on true
  where x.last_seen_run_id = cr.id

  union all

  select 'prior' as scope, x.*
  from classified_tx x
  join prior_run pr on true
  where x.last_seen_run_id = pr.prior_run_id
),
agg as (
  select
    scope,
    {TOTALS_AGG_SELECT}
  from scoped x
  group by scope
),
cur as (
  select *
  from agg
  where scope = 'current'

  union all

  select
    'current' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'current')
),
prev as (
  select *
  from agg
  where scope = 'prior'

  union all

  select
    'prior' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'prior')
)
select
  {TOTALS_DELTA_SELECT}
from cur
cross join prev;
"""


WTD_TOTALS_WITH_PREV = f"""
with bounds as (
  select
    date_trunc('week', (now() at time zone '{SQL_TZ}'))::date as cur_start,
    (now() at time zone '{SQL_TZ}')::date as cur_end
),
prev_bounds as (
  select
    (b.cur_start - interval '7 days')::date as prev_start,
    (b.cur_end - interval '7 days')::date as prev_end
  from bounds b
),
{CLASSIFIED_TX_FOR_ENV_CTE},
scoped as (
  select 'current' as scope, x.*
  from classified_tx x
  join bounds b on true
  where x.date >= b.cur_start
    and x.date <= b.cur_end

  union all

  select 'prior' as scope, x.*
  from classified_tx x
  join prev_bounds p on true
  where x.date >= p.prev_start
    and x.date <= p.prev_end
),
agg as (
  select
    scope,
    {TOTALS_AGG_SELECT}
  from scoped x
  group by scope
),
cur as (
  select *
  from agg
  where scope = 'current'

  union all

  select
    'current' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'current')
),
prev as (
  select *
  from agg
  where scope = 'prior'

  union all

  select
    'prior' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'prior')
)
select
  {TOTALS_DELTA_SELECT}
from cur
cross join prev;
"""


MTD_TOTALS_WITH_PREV = f"""
with bounds as (
  select
    date_trunc('month', (now() at time zone '{SQL_TZ}'))::date as cur_start,
    (now() at time zone '{SQL_TZ}')::date as cur_end
),
prev_bounds as (
  select
    (b.cur_start - interval '1 month')::date as prev_start,
    least(
      (b.cur_start - interval '1 month')::date + (b.cur_end - b.cur_start),
      (b.cur_start - interval '1 day')::date
    ) as prev_end
  from bounds b
),
{CLASSIFIED_TX_FOR_ENV_CTE},
scoped as (
  select 'current' as scope, x.*
  from classified_tx x
  join bounds b on true
  where x.date >= b.cur_start
    and x.date <= b.cur_end

  union all

  select 'prior' as scope, x.*
  from classified_tx x
  join prev_bounds p on true
  where x.date >= p.prev_start
    and x.date <= p.prev_end
),
agg as (
  select
    scope,
    {TOTALS_AGG_SELECT}
  from scoped x
  group by scope
),
cur as (
  select *
  from agg
  where scope = 'current'

  union all

  select
    'current' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'current')
),
prev as (
  select *
  from agg
  where scope = 'prior'

  union all

  select
    'prior' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'prior')
)
select
  {TOTALS_DELTA_SELECT}
from cur
cross join prev;
"""


YTD_TOTALS_WITH_PREV = f"""
with bounds as (
  select
    date_trunc('year', (now() at time zone '{SQL_TZ}'))::date as cur_start,
    (now() at time zone '{SQL_TZ}')::date as cur_end
),
prev_bounds as (
  select
    (b.cur_start - interval '1 year')::date as prev_start,
    least(
      (b.cur_start - interval '1 year')::date + (b.cur_end - b.cur_start),
      (b.cur_start - interval '1 day')::date
    ) as prev_end
  from bounds b
),
{CLASSIFIED_TX_FOR_ENV_CTE},
scoped as (
  select 'current' as scope, x.*
  from classified_tx x
  join bounds b on true
  where x.date >= b.cur_start
    and x.date <= b.cur_end

  union all

  select 'prior' as scope, x.*
  from classified_tx x
  join prev_bounds p on true
  where x.date >= p.prev_start
    and x.date <= p.prev_end
),
agg as (
  select
    scope,
    {TOTALS_AGG_SELECT}
  from scoped x
  group by scope
),
cur as (
  select *
  from agg
  where scope = 'current'

  union all

  select
    'current' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'current')
),
prev as (
  select *
  from agg
  where scope = 'prior'

  union all

  select
    'prior' as scope,
    null::numeric as true_spend,
    null::numeric as necessity_spend,
    null::numeric as discretionary_spend,
    null::numeric as true_income,
    null::numeric as reimbursements,
    null::numeric as savings,
    null::numeric as savings_rate,
    null::numeric as transfers_out,
    null::numeric as transfers_in,
    null::numeric as invest_out,
    null::numeric as invest_in,
    null::numeric as fees_out,
    null::numeric as ignored_abs
  where not exists (select 1 from agg where scope = 'prior')
)
select
  {TOTALS_DELTA_SELECT}
from cur
cross join prev;
"""


POSTED_TRANSACTIONS_FOR_RUN = f"""
with
{CLASSIFIED_TX_FOR_ENV_CTE}
select
  x.date,
  x.name,
  x.merchant_name,
  x.effective_merchant,
  x.amount,
  x.account_id,
  x.account_name,
  x.item_label,
  x.classification,
  x.behavior_axis,
  x.category,
  x.matched_rule_id,
  x.classification_source
from classified_tx x
where x.last_seen_run_id = %s
order by x.date desc, x.amount desc;
"""


CLASSIFICATION_SOURCE_BREAKDOWN_FOR_RUN = f"""
with
{CLASSIFIED_TX_FOR_ENV_CTE},
base as (
  select
    x.classification,
    x.classification_source,
    count(*) as tx_count,
    coalesce(sum(abs(x.amount)), 0) as abs_amount_sum
  from classified_tx x
  where x.last_seen_run_id = %s
  group by x.classification, x.classification_source
),
with_pct as (
  select
    b.*,
    case
      when sum(b.abs_amount_sum) over (partition by b.classification) = 0 then null
      else b.abs_amount_sum / sum(b.abs_amount_sum) over (partition by b.classification)
    end as pct_of_class_abs_amount
  from base b
)
select
  classification,
  classification_source,
  tx_count,
  abs_amount_sum,
  pct_of_class_abs_amount
from with_pct
order by classification, classification_source;
"""