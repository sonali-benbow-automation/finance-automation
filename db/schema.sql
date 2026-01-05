create table if not exists ${PLAID_ITEMS_TABLE} (
  id bigserial primary key,
  label text not null unique,
  institution_name text not null,
  institution_id text not null,
  item_id text not null unique,
  access_token text not null,
  transactions_enabled boolean not null default false,
  balances_enabled boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists ${RUNS_TABLE} (
  id bigserial primary key,
  run_type text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running',
  error text
);

create table if not exists ${BALANCE_SNAPSHOTS_TABLE} (
  id bigserial primary key,
  run_id bigint not null,
  label text not null,
  account_id text not null,
  account_name text,
  account_type text,
  account_subtype text,
  mask text,
  current numeric,
  available numeric,
  credit_limit numeric,
  iso_currency_code text,
  snapshot_at timestamptz not null default now(),
  constraint balance_snapshots_unique_per_run unique (run_id, account_id),
  constraint balance_snapshots_run_fk foreign key (run_id) references ${RUNS_TABLE}(id)
);

create table if not exists ${ACCOUNTS_TABLE} (
  id bigserial primary key,
  label text not null,
  account_id text not null,
  name text,
  official_name text,
  type text,
  subtype text,
  mask text,
  iso_currency_code text,
  include_in_app boolean not null default true,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint accounts_unique_label_account unique (label, account_id),
  constraint accounts_label_fk foreign key (label) references ${PLAID_ITEMS_TABLE}(label)
);

create table if not exists ${CURSORS_TABLE} (
  id bigserial primary key,
  label text not null unique,
  transactions_cursor text,
  updated_at timestamptz not null default now(),
  constraint cursors_label_fk foreign key (label) references ${PLAID_ITEMS_TABLE}(label)
);

create table if not exists ${TRANSACTIONS_TABLE} (
  id bigserial primary key,
  label text not null,
  transaction_id text not null unique,
  account_id text not null,
  name text,
  merchant_name text,
  amount numeric,
  iso_currency_code text,
  date date,
  pending boolean,
  pending_transaction_id text,
  sync_status text not null,
  constraint transactions_sync_status_check
    check (sync_status in ('added', 'modified', 'removed')),
  removed boolean not null default false,
  removed_at timestamptz,
  first_seen_run_id bigint,
  last_seen_run_id bigint,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint transactions_label_fk
    foreign key (label) references ${PLAID_ITEMS_TABLE}(label),
  constraint transactions_first_seen_run_fk
    foreign key (first_seen_run_id) references ${RUNS_TABLE}(id),
  constraint transactions_last_seen_run_fk
    foreign key (last_seen_run_id) references ${RUNS_TABLE}(id)
);