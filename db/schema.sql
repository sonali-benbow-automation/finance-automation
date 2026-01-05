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
    foreign key (run_id) references ${RUNS_TABLE}(id) on delete set null
);