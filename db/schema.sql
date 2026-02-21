create extension if not exists pgcrypto;

create table if not exists ${PLAID_ITEMS_TABLE} (
  id bigserial primary key,
  label text not null,
  env text not null default 'sandbox',
  institution_name text not null,
  institution_id text not null,
  item_id text not null unique,
  access_token_enc bytea not null,
  access_token_kid text not null default 'v1',
  transactions_enabled boolean not null default false,
  balances_enabled boolean not null default true,
  active boolean not null default true,
  archived_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint plaid_items_unique_env_label unique (env, label)
);

create table if not exists ${RUNS_TABLE} (
  id bigserial primary key,
  run_type text not null,
  env text not null default 'sandbox',
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running',
  error text
);

create table if not exists ${ACCOUNTS_TABLE} (
  id bigserial primary key,
  plaid_item_pk bigint not null,
  account_id text not null,
  name text,
  official_name text,
  type text,
  subtype text,
  mask text,
  iso_currency_code text,
  include_in_app boolean not null default true,
  active boolean not null default true,
  raw jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint accounts_item_fk
    foreign key (plaid_item_pk) references ${PLAID_ITEMS_TABLE}(id) on delete cascade,
  constraint accounts_unique_item_account
    unique (plaid_item_pk, account_id)
);

create table if not exists ${CURSORS_TABLE} (
  id bigserial primary key,
  plaid_item_pk bigint not null unique,
  transactions_cursor text,
  updated_at timestamptz not null default now(),
  constraint cursors_item_fk
    foreign key (plaid_item_pk) references ${PLAID_ITEMS_TABLE}(id) on delete cascade
);

create table if not exists ${PLAID_BALANCES_RAW_TABLE} (
  id bigserial primary key,
  run_id bigint not null,
  plaid_item_pk bigint not null,
  label text,
  env text not null default 'sandbox',
  fetched_at timestamptz not null default now(),
  payload jsonb not null,
  constraint plaid_balances_raw_run_fk
    foreign key (run_id) references ${RUNS_TABLE}(id) on delete cascade,
  constraint plaid_balances_raw_item_fk
    foreign key (plaid_item_pk) references ${PLAID_ITEMS_TABLE}(id) on delete cascade
);

create table if not exists ${PLAID_TRANSACTIONS_RAW_TABLE} (
  id bigserial primary key,
  run_id bigint not null,
  plaid_item_pk bigint not null,
  label text,
  env text not null default 'sandbox',
  fetched_at timestamptz not null default now(),
  page_index integer not null,
  payload jsonb not null,
  constraint plaid_transactions_raw_run_fk
    foreign key (run_id) references ${RUNS_TABLE}(id) on delete cascade,
  constraint plaid_transactions_raw_item_fk
    foreign key (plaid_item_pk) references ${PLAID_ITEMS_TABLE}(id) on delete cascade
);

create table if not exists ${NOTIFICATIONS_TABLE} (
  id bigserial primary key,
  run_id bigint,
  channel text not null,
  status text not null,
  message text,
  error text,
  created_at timestamptz not null default now(),
  constraint notifications_run_fk
    foreign key (run_id) references ${RUNS_TABLE}(id) on delete set null,
  constraint notifications_unique_run_channel
    unique (run_id, channel)
);

create table if not exists ${HOSTED_LINK_SESSIONS_TABLE} (
  id bigserial primary key,
  label text not null,
  env text not null default 'sandbox',
  link_token text not null unique,
  hosted_link_url text not null,
  webhook_url text not null,
  status text not null default 'created'
    check (status in ('created','success','failed')),
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists ${PLAID_WEBHOOK_EVENTS_TABLE} (
  id bigserial primary key,
  webhook_type text,
  webhook_code text,
  link_session_id text,
  link_token text,
  status text,
  environment text,
  raw jsonb not null,
  received_at timestamptz not null default now()
);

create index if not exists idx_plaid_webhook_events_link_token
  on ${PLAID_WEBHOOK_EVENTS_TABLE} (link_token);

create index if not exists idx_plaid_webhook_events_received_at
  on ${PLAID_WEBHOOK_EVENTS_TABLE} (received_at);

create table if not exists ${MERCHANT_RULES_TABLE} (
  id bigserial primary key,
  env text not null default 'sandbox',
  match_type text not null default 'ilike'
    check (match_type in ('ilike','regex','contains')),
  pattern text not null,
  classification text not null
    check (classification in (
      'expense',
      'income',
      'cash_in_non_income',
      'transfer',
      'invest',
      'fee',
      'ignore'
    )),
  behavior_axis text
    check (behavior_axis in ('necessity','discretionary')),
  category text,
  priority integer not null default 100,
  active boolean not null default true,
  note text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists ${MANUAL_BALANCES_TABLE} (
  id bigserial primary key,
  env text not null default 'sandbox',
  key text not null,
  label text not null,
  signed_balance numeric not null,
  note text,
  updated_at timestamptz not null default now(),
  constraint manual_balances_unique_env_key unique (env, key)
);

create table if not exists ${MANUAL_BALANCE_HISTORY_TABLE} (
  id bigserial primary key,
  env text not null,
  key text not null,
  signed_balance numeric not null,
  snapshot_at timestamptz not null default now(),
  note text
);

create or replace function log_manual_balance_change()
returns trigger as $$$$
begin
  insert into ${MANUAL_BALANCE_HISTORY_TABLE} (env, key, signed_balance, snapshot_at, note)
  values (new.env, new.key, new.signed_balance, now(), new.note);
  return new;
end;
$$$$ language plpgsql;

drop trigger if exists trg_log_manual_balance_change on ${MANUAL_BALANCES_TABLE};

create trigger trg_log_manual_balance_change
after insert or update of signed_balance, note
on ${MANUAL_BALANCES_TABLE}
for each row
execute function log_manual_balance_change();

alter table ${PLAID_ITEMS_TABLE} enable row level security;
alter table ${ACCOUNTS_TABLE} enable row level security;
alter table ${CURSORS_TABLE} enable row level security;
alter table ${RUNS_TABLE} enable row level security;
alter table ${NOTIFICATIONS_TABLE} enable row level security;
alter table ${HOSTED_LINK_SESSIONS_TABLE} enable row level security;
alter table ${PLAID_WEBHOOK_EVENTS_TABLE} enable row level security;
alter table ${MERCHANT_RULES_TABLE} enable row level security;
alter table ${MANUAL_BALANCES_TABLE} enable row level security;
alter table ${MANUAL_BALANCE_HISTORY_TABLE} enable row level security;

revoke all on all tables in schema public from anon, authenticated;
revoke all on all sequences in schema public from anon, authenticated;

grant select, insert, update, delete on all tables in schema public to service_role;
grant usage, select on all sequences in schema public to service_role;

drop policy if exists service_role_all on ${PLAID_ITEMS_TABLE};
create policy service_role_all on ${PLAID_ITEMS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${ACCOUNTS_TABLE};
create policy service_role_all on ${ACCOUNTS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${CURSORS_TABLE};
create policy service_role_all on ${CURSORS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${RUNS_TABLE};
create policy service_role_all on ${RUNS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${NOTIFICATIONS_TABLE};
create policy service_role_all on ${NOTIFICATIONS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${HOSTED_LINK_SESSIONS_TABLE};
create policy service_role_all on ${HOSTED_LINK_SESSIONS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${PLAID_WEBHOOK_EVENTS_TABLE};
create policy service_role_all on ${PLAID_WEBHOOK_EVENTS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${MERCHANT_RULES_TABLE};
create policy service_role_all on ${MERCHANT_RULES_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${MANUAL_BALANCES_TABLE};
create policy service_role_all on ${MANUAL_BALANCES_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${MANUAL_BALANCE_HISTORY_TABLE};
create policy service_role_all on ${MANUAL_BALANCE_HISTORY_TABLE}
for all to service_role using (true) with check (true);