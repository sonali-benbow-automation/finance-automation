NET_WORTH_LATEST = """
with latest_balances_run as (
  select max(id) as run_id
  from runs
  where run_type = 'balances'
    and status = 'success'
)
select
  coalesce(sum(
    case
      when a.type in ('credit', 'loan') then -abs(coalesce(bs.current, 0))
      else coalesce(bs.current, 0)
    end
  ), 0) as net_worth
from balance_snapshots bs
join accounts a
  on a.label = bs.label
 and a.account_id = bs.account_id
join latest_balances_run r
  on bs.run_id = r.run_id
where a.include_in_app = true
  and a.active = true;
"""

POSTED_TRANSACTIONS_LATEST_RUN = """
with latest_run as (
  select max(id) as run_id
  from runs
  where status = 'success'
)
select
  t.date,
  t.name,
  t.merchant_name,
  t.amount,
  t.label,
  t.account_id,
  t.sync_status
from transactions t
join accounts a
  on a.label = t.label
 and a.account_id = t.account_id
join latest_run r
  on t.last_seen_run_id = r.run_id
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
order by t.date desc, t.amount desc;
"""

TODAY_TOTALS = """
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as today_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as today_received
from transactions t
join accounts a
  on a.label = t.label
 and a.account_id = t.account_id
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date = (now() at time zone 'America/New_York')::date;
"""

WTD_TOTALS = """
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as wtd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as wtd_received
from transactions t
join accounts a
  on a.label = t.label
 and a.account_id = t.account_id
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('week', (now() at time zone 'America/New_York'))::date
  and t.date <= (now() at time zone 'America/New_York')::date;
"""

MTD_TOTALS = """
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as mtd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as mtd_received
from transactions t
join accounts a
  on a.label = t.label
 and a.account_id = t.account_id
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('month', (now() at time zone 'America/New_York'))::date
  and t.date <= (now() at time zone 'America/New_York')::date;
"""