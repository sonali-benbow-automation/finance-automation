from config import TIMEZONE

SQL_TIMEZONE = TIMEZONE or "America/New_York"

NET_WORTH_FOR_RUN = """
select
  coalesce(sum(
    case
      when a.type in ('credit', 'loan') then -abs(coalesce(bs.current, 0))
      else coalesce(bs.current, 0)
    end
  ), 0) as net_worth
from balance_snapshots bs
join accounts a
  on a.id = bs.account_pk
where bs.run_id = %s
  and a.include_in_app = true
  and a.active = true;
"""

POSTED_TRANSACTIONS_FOR_RUN = """
select
  t.date,
  t.name,
  t.merchant_name,
  t.amount,
  a.account_id,
  a.name as account_name,
  pi.label as item_label,
  t.sync_status
from transactions t
join accounts a
  on a.id = t.account_pk
join plaid_items pi
  on pi.id = a.plaid_item_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.last_seen_run_id = %s
order by t.date desc, t.amount desc;
"""

TODAY_TOTALS_FOR_RUN = """
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as today_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as today_received
from transactions t
join accounts a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.last_seen_run_id = %s;
"""

WTD_TOTALS = f"""
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as wtd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as wtd_received
from transactions t
join accounts a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('week', (now() at time zone '{SQL_TIMEZONE}'))::date
  and t.date <= (now() at time zone '{SQL_TIMEZONE}')::date;
"""

MTD_TOTALS = f"""
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as mtd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as mtd_received
from transactions t
join accounts a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('month', (now() at time zone '{SQL_TIMEZONE}'))::date
  and t.date <= (now() at time zone '{SQL_TIMEZONE}')::date;
"""

YTD_TOTALS = f"""
select
  coalesce(sum(case when t.amount > 0 then t.amount else 0 end), 0) as ytd_spent,
  coalesce(sum(case when t.amount < 0 then -t.amount else 0 end), 0) as ytd_received
from transactions t
join accounts a
  on a.id = t.account_pk
where a.include_in_app = true
  and a.active = true
  and t.removed = false
  and coalesce(t.pending, false) = false
  and t.date >= date_trunc('year', (now() at time zone '{SQL_TIMEZONE}'))::date
  and t.date <= (now() at time zone '{SQL_TIMEZONE}')::date;
"""