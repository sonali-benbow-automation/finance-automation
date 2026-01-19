create extension if not exists pgcrypto;

create table if not exists ${PLAID_ITEMS_TABLE} (
  id bigserial primary key,
  label text not null unique,
  env text not null default 'sandbox',
  institution_name text not null,
  institution_id text not null,
  item_id text not null unique,
  access_token text not null,
  transactions_enabled boolean not null default false,
  balances_enabled boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
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

create table if not exists ${BALANCE_SNAPSHOTS_TABLE} (
  id bigserial primary key,
  run_id bigint not null,
  account_pk bigint not null,
  current numeric,
  available numeric,
  credit_limit numeric,
  iso_currency_code text,
  snapshot_at timestamptz not null default now(),
  raw jsonb,
  constraint balance_snapshots_run_fk
    foreign key (run_id) references ${RUNS_TABLE}(id) on delete cascade,
  constraint balance_snapshots_account_fk
    foreign key (account_pk) references ${ACCOUNTS_TABLE}(id) on delete cascade,
  constraint balance_snapshots_unique_per_run_account
    unique (run_id, account_pk)
);

create table if not exists ${TRANSACTIONS_TABLE} (
  id bigserial primary key,
  account_pk bigint not null,
  transaction_id text not null unique,
  amount numeric not null,
  iso_currency_code text,
  date date,
  pending boolean not null default false,
  pending_transaction_id text,
  name text,
  merchant_name text,
  category_id text,
  category text,
  personal_finance_category jsonb,
  payment_channel text,
  transaction_type text,
  authorized_date date,
  datetime timestamptz,
  authorized_datetime timestamptz,
  sync_status text not null,
  constraint transactions_sync_status_check
    check (sync_status in ('added','modified','removed')),
  removed boolean not null default false,
  removed_at timestamptz,
  first_seen_run_id bigint,
  last_seen_run_id bigint,
  raw jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint transactions_account_fk
    foreign key (account_pk) references ${ACCOUNTS_TABLE}(id) on delete cascade,
  constraint transactions_first_seen_run_fk
    foreign key (first_seen_run_id) references ${RUNS_TABLE}(id),
  constraint transactions_last_seen_run_fk
    foreign key (last_seen_run_id) references ${RUNS_TABLE}(id)
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



alter table ${PLAID_ITEMS_TABLE} enable row level security;
alter table ${ACCOUNTS_TABLE} enable row level security;
alter table ${TRANSACTIONS_TABLE} enable row level security;
alter table ${BALANCE_SNAPSHOTS_TABLE} enable row level security;
alter table ${CURSORS_TABLE} enable row level security;
alter table ${RUNS_TABLE} enable row level security;
alter table ${NOTIFICATIONS_TABLE} enable row level security;
alter table ${HOSTED_LINK_SESSIONS_TABLE} enable row level security;
alter table ${PLAID_WEBHOOK_EVENTS_TABLE} enable row level security;


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

drop policy if exists service_role_all on ${TRANSACTIONS_TABLE};
create policy service_role_all on ${TRANSACTIONS_TABLE}
for all to service_role using (true) with check (true);

drop policy if exists service_role_all on ${BALANCE_SNAPSHOTS_TABLE};
create policy service_role_all on ${BALANCE_SNAPSHOTS_TABLE}
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