# Finance Automation

## Project goal

Build an automated personal finance pipeline using Plaid, Supabase PostgreSQL, local Apache Spark, and daily financial reporting.

## Architecture

- `plaid_src/` contains the Plaid client, sandbox setup, and Flask Hosted Link API. The API logs webhook events and exchanges public tokens. `db/repos/items.py` stores access tokens encrypted with PostgreSQL `pgcrypto`.
- `ingest/ingest_plaid.py` fetches data from Plaid, updates the account lookup table, and stores complete balance responses and transaction pages as JSONB. Raw writes and cursor updates commit together. `run_ingest()` returns the ingestion run ID.
- Spark reads the raw responses for that run and matches accounts using `(plaid_item_pk, account_id)` to get `account_pk`. The two Spark jobs pass the prepared rows to `db/repos/transactions.py` and `db/repos/balances.py`. PostgreSQL stores the processed rows for reporting.
- `jobs/daily_sync.py` calls ingestion, transaction processing, balance processing, and the email digest in that order with the same run ID. `notify/queries.py` applies merchant rules at report time and falls back to Plaid's `personal_finance_category`. The report then renders a PDF and sends it over SMTP. `jobs/retry_notifications.py` retries failed email notifications.
- `config.py` loads environment settings. `db/init_db.py` substitutes configured names into `db/schema.sql`, which defines 14 tables and expects Supabase roles.

## November 24 handoff

- Removed `TRANSACTIONS_START_DATE` from ingestion, config, and the local ignored `.env`. The old filter discarded older additions and modifications on an initial sync. Ingestion now saves all pages Plaid sends. Earlier discarded entries do not reappear automatically. Keep pagination, cursor updates, and account upserts in ingestion because Spark depends on them.
- `jobs.process_transactions.apply_plaid_transaction_changes(run_id)` filters to one run, combines `added`, `modified`, and `removed` into one Spark table, and keeps `sync_status` on every row. It sorts by `plaid_item_pk` and `page_index` before writing. The Spark job no longer joins against saved transactions. PostgreSQL checks for an existing row when it applies each change, so a modification can find a transaction inserted on an earlier page in the same batch.
- The transaction writer uses positional `%s` parameters and one loop, with separate functions for insert, update, and removal. Additions skip duplicate transaction IDs. Modifications refresh the extracted fields while preserving `first_seen_run_id` and any existing removal state. Removals mark the row and retain its history and first removal timestamp. Updates and removals set `updated_at` explicitly. PostgreSQL supplies the initial `created_at` and `updated_at` on insert. The job prints the unmatched count after commit. It does not persist those issues yet.
- `jobs.process_balances.apply_plaid_balance_snapshots(run_id)` expands the raw `accounts` list, preserves decimal precision, and uses the raw `fetched_at` as `snapshot_at`. The balance writer saves one snapshot per `(run_id, account_pk)`, skips duplicates, and prints the skipped count after commit. An unmatched account can fail the batch because `account_pk` is required.
- The local schema and configuration include both `transactions` and `balance_snapshots`. The live Supabase columns, defaults, constraints, and row-level security for both tables were checked with read-only metadata. The balance definition matches the existing live table, so no Supabase migration is needed for this work. The current daily job and reporting imports work locally with the restored configuration.
- Verified local Spark extraction and account matching on synthetic data, balance timestamp preservation, positional SQL values with mock cursors, and the daily call order with mocks. A transaction insert was tested with rollback. A separate live transaction batch inserted 29 test rows for run 8, and those exact rows were deleted and verified gone. Temporary test code and the hard-coded run ID were removed. No complete live daily run, live balance write, or email delivery was verified in this session.
- The active local `.venv/bin/python` is Python 3.12.11 with PySpark 4.3.0. `requirements.txt` now pins `pyspark==4.3.0`. If PyCharm cannot import PySpark, make sure its interpreter points to the project `.venv/bin/python`. If a synthetic Spark test picks up a different worker Python, set `PYSPARK_PYTHON` to that same executable.

## Working conventions

- Work incrementally. Explain one concrete change at a time in plain language and agree on it before editing. Use meaningful names. When the user is learning, explain what the code does and why it belongs in the pipeline.
- Keep `.env`, credentials, tokens, and financial payloads out of logs and shared output. Use synthetic fixtures for local verification. Get approval before installing dependencies, running ingestion, applying schema changes, or contacting live services.
- Check the working tree before editing and preserve unrelated changes.
- Use configured table names and preserve sandbox and production isolation. Keep raw data and existing reporting history available when making schema or processing changes.
- Preserve the transaction between raw writes and cursor advancement. Define replay order and stable keys before adding writes so retries do not duplicate transactions or balance snapshots.
- Keep the Discover-to-Capital One migration separate until Spark processing and reporting are working. Before switching balance sources, verify which Plaid and manual balances represent the same account so it is counted once.

## Next step

1. Start with `balance_snapshots.raw`. In `jobs/process_balances.py`, read the `accounts` array as JSON strings and expand it to one string per account. Keep the original nested `balances` object as `raw`, then extract the typed columns separately. Add `raw` to the balance writer's insert as JSONB. Verify with synthetic data containing an extra field outside the typed schema. The column already exists and requires no schema change.
2. Apply the same pattern to `transactions.raw`. Save the complete original Plaid transaction for additions and modifications. Preserve the saved raw object on a removal. The `ARRAY<STRING>` extraction approach was verified in local Spark for both payload shapes. Both writers currently leave `raw` null on insert.
3. Verify the PDF and reporting totals with fixture data, including environment isolation and reruns, before trying the full daily flow against live services.

## Deferred follow-ups

- Persist unmatched modifications and removals in a `transaction_processing_issues` table. Keep the daily job processing other changes. Record the transaction ID, run ID, source page, and change type. Track open and resolved issues and deduplicate retries. For missing modifications, review earlier raw pages for the original addition before recovering a row. An unmatched removal has no saved row to mark.
- Add logging across the pipeline and schedule the daily job after the main flow is complete. There is no scheduler wired to `DAILY_DIGEST_HOUR`.
- Define behavior for repeated changes to the same transaction on the same page and for replay of older runs. The current Spark sort only orders pages within a Plaid item. Also review missing `next_cursor` handling and how to reset the cursor when a Plaid item is replaced in place.
- Scope week-, month-, and year-to-date reports to the selected environment when sandbox and production share a database. Keep merchant-rule classifications in the reporting layer. Plaid's legacy `category` and `category_id` can be null.
