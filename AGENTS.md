# Finance Automation

## Project Vision and Target Architecture

Finance Automation is a personal financial data engineering and analytics platform. It brings together automated ingestion, distributed processing, persistent storage, reporting, and interactive analytics. Keep it affordable and maintainable for personal use.

### Core design principles

Separate ingestion, processing, storage, reporting, and presentation. Preserve raw source data for replay, auditing, and troubleshooting. Support incremental processing and idempotent execution. Give each business rule one owner. Favor modular, extensible, testable code, comprehensive operational visibility, and simple infrastructure. Prefer open-source and free-tier options where practical.

### Ingestion and Bronze

Python retrieves data from Plaid, handles API errors, retries, pagination, and incremental synchronization, and records ingestion status. Persist the original API response wherever practical, with ingestion timestamps, source and endpoint metadata, and execution identifiers. Bronze is the durable source for historical reprocessing and tracing curated records back to source. Financial transformations and reporting calculations belong downstream.

### Processing and Silver

PySpark reads raw payloads and metadata using explicit JSON schemas, normalizes nested structures, converts types, validates data, and deduplicates records. It owns merchant mapping and categorization rules for curated data, applies incremental changes, and writes structured records to PostgreSQL. Spark should run locally first and remain portable to Databricks or another Spark environment. Databricks is optional.

Silver stores structured, validated datasets with appropriate keys, relationships, constraints, and history. Candidate datasets include transactions, institutions, accounts, balances, balance snapshots, merchants, transaction categories, investments, and liabilities. Reporting should reuse these datasets directly.

### Reporting and Gold

Centralize financial calculations in shared reporting datasets or services using views, materialized views, tables, or query services as appropriate. Examples include net worth history, daily and monthly spending, income and expenses, cash flow, savings metrics, account balance history, merchant and category spending, recurring expenses, and historical comparisons. The dashboard, Excel exports, and email should use the same calculations and filter selections.

### Orchestration

The pipeline initializes execution, ingests and persists raw payloads, runs Spark and persists curated data, performs data quality checks, refreshes reporting datasets, and records the final status. Eventually support scheduled and manual runs, task dependencies, retries, failure handling, timeouts, execution history, concurrency protection, and selective reprocessing of failed stages. Consider Render Cron Jobs, Prefect, or another lightweight option when the requirements are known.

### Logging, auditing, and data quality

- Use structured application logs with timestamps, levels, components, pipeline and job run IDs, stages, event descriptions, timings, and errors. Protect credentials, tokens, connection strings, and sensitive raw financial data.
- Use a unique pipeline run ID to associate ingestion, processing, reporting, and audit results. Track job run and parent IDs, job names, triggers, statuses, start and end times, durations, retry attempts, and errors. Potential statuses are pending, running, succeeded, failed, skipped, and cancelled.
- Capture ingestion metrics for attempted, successful, and failed requests, payloads received and persisted, source records received, accounts processed, and duration.
- Capture Spark metrics for raw payloads read, source records parsed, input and output rows, actual inserts, updates and deletes, duplicates, rejects, skips, validation failures, processing duration, and write duration. Distinguish raw payload, parsed record, transformed row, and database write counts. Derive write counts from actual write results. Collect metrics during existing operations and reuse computations where practical.
- Check required fields, schema compatibility, duplicate IDs, referential integrity, financial values, missing account relationships, unexpected count changes, freshness, and reconciliation discrepancies. Persist meaningful results for historical analysis.
- Potential audit datasets include pipeline runs, job runs, ingestion and processing metrics, data quality results, pipeline events, and report history. Design the schema around existing structures and implementation needs.

### Web application and reporting

The primary interface is a responsive desktop and mobile web dashboard. Use React, TypeScript, and Vite, with Material UI or a similar component library and Recharts or a similar chart library. Prefer reusable components. Use a Python REST API, preferably Flask, with the existing database. A native mobile app is optional beyond the current scope.

Use a clean, dark fintech design with consistent typography and spacing, interactive charts, reusable metric cards, searchable and filterable tables, clear financial indicators, and accessible navigation. Potential sections include overview, net worth, transactions, spending, cash flow, accounts, merchant analytics, reports, pipeline operations, and settings.

Prioritize on-demand reporting over automated daily emails. Support custom date ranges, account/category/merchant filters, historical comparisons, interactive previews, professionally formatted Excel workbooks with summaries and detail, on-demand email, and optional additional export formats.

### Pipeline operations dashboard

Provide a mobile-accessible operations view backed by persisted execution records and events. Show current run and job, stage statuses, start and elapsed times, ingestion and Spark counts, actual database writes, quality results, history, errors, recent events, last successful sync, and freshness. Support authorized manual runs and failed stage reprocessing. Use polling or another suitable approach for updates. Show progress percentages only when measurable.

### Security

Require dashboard authentication and authorize access to financial data, operations endpoints, report generation, and pipeline execution. Use secure sessions or tokens, HTTPS for remote access, parameterized queries, least-privilege database access, and backups. Store database and Plaid credentials in environment variables or appropriate secret management. Keep credentials, access tokens, and privileged database keys out of source control and the frontend. Keep raw ingestion payloads off public-facing endpoints. Prefer an established identity provider or database-supported authentication over custom password management.

### Deployment and infrastructure

Support affordable remote access and separate local and deployed configuration. Render is a candidate for frontend, API, scheduling, and logging. Local hardware is also suitable, especially for Spark. Keep the design portable. Evaluate hosting and scheduling costs, Spark memory, database persistence, remote access, security, reliability, backups, and recovery.

### Testing and implementation guidance

Cover ingestion, parsing, Spark transformations, merchant rules, incremental processing, persistence, financial calculations, quality checks, reporting, API authentication and authorization, and exports with synthetic or sanitized data. Keep actual personal financial information out of fixtures and source control.

This section describes the intended end state. Inspect the repository before proposing structural changes. Respect existing conventions, preserve current behavior unless requested, and evolve the system through focused changes with relevant tests. Identify prerequisites and trade-offs, maintain layer boundaries, and avoid unnecessary dependencies or unrelated future features. Report only measured metrics, actual execution statuses, and verified financial results.

## Current architecture

- `plaid_src/` contains the Plaid client, sandbox setup, and Flask Hosted Link API. The API logs webhook events and exchanges public tokens. `db/repos/items.py` stores access tokens encrypted with PostgreSQL `pgcrypto`.
- `ingest/ingest_plaid.py` fetches data from Plaid and stores complete balance responses and transaction pages as JSONB. Raw writes and cursor updates commit together. `run_ingest()` returns the ingestion run ID.
- The Spark account job reads the saved Plaid account responses for that run and upserts the account lookup. The transaction and balance jobs match accounts on `(plaid_item_pk, account_id)` to get `account_pk`. All three jobs pass prepared rows to their PostgreSQL repositories. PostgreSQL stores the processed rows for reporting.
- `jobs/daily_sync.py` calls ingestion, account processing, transaction processing, balance processing, and the email digest in that order with the same run ID. `notify/queries.py` applies merchant rules at report time and falls back to Plaid's `personal_finance_category`. The report then renders a PDF and sends it over SMTP. `jobs/retry_notifications.py` retries failed email notifications.
- `config.py` loads environment settings. `db/init_db.py` substitutes configured names into `db/schema.sql`, which defines 14 tables and expects Supabase roles.

## November 24 handoff

- Removed `TRANSACTIONS_START_DATE` from ingestion, config, and the local ignored `.env`. The old filter discarded older additions and modifications on an initial sync. Ingestion now saves all pages Plaid sends. Earlier discarded entries do not reappear automatically. Keep pagination and cursor updates in ingestion. Account upserts now run immediately after ingestion, before transaction and balance processing.
- `jobs.process_transactions.apply_plaid_transaction_changes(run_id)` filters to one run, combines `added`, `modified`, and `removed` into one Spark table, and keeps `sync_status` on every row. It sorts by `plaid_item_pk` and `page_index` before writing. The Spark job no longer joins against saved transactions. PostgreSQL checks for an existing row when it applies each change, so a modification can find a transaction inserted on an earlier page in the same batch.
- The transaction writer uses positional `%s` parameters and one loop, with separate functions for insert, update, and removal. Additions skip duplicate transaction IDs. Modifications refresh the extracted fields while preserving `first_seen_run_id` and any existing removal state. Removals mark the row and retain its history and first removal timestamp. Updates and removals set `updated_at` explicitly. PostgreSQL supplies the initial `created_at` and `updated_at` on insert. The job logs the unmatched modification and removal count after commit. It does not persist those issues yet.
- `jobs.process_balances.apply_plaid_balance_snapshots(run_id)` expands the raw `accounts` list, preserves decimal precision, and uses the raw `fetched_at` as `snapshot_at`. The balance writer saves one snapshot per `(run_id, account_pk)`, skips duplicates, and logs the skipped count after commit. An unmatched account can fail the batch because `account_pk` is required.
- The local schema and configuration include both `transactions` and `balance_snapshots`. The live Supabase columns, defaults, constraints, and row-level security for both tables were checked with read-only metadata. The balance definition matches the existing live table, so no Supabase migration is needed for this work. The current daily job and reporting imports work locally with the restored configuration.
- Verified local Spark extraction and account matching on synthetic data, balance timestamp preservation, positional SQL values with mock cursors, and the daily call order with mocks. A transaction insert was tested with rollback. A separate live transaction batch inserted 29 test rows for run 8, and those exact rows were deleted and verified gone. Temporary test code and the hard-coded run ID were removed. No complete live daily run, live balance write, or email delivery was verified in this session.
- The active local `.venv/bin/python` is Python 3.12.11 with PySpark 4.3.0. `requirements.txt` now pins `pyspark==4.3.0`. If PyCharm cannot import PySpark, make sure its interpreter points to the project `.venv/bin/python`. If a synthetic Spark test picks up a different worker Python, set `PYSPARK_PYTHON` to that same executable.

## Account processing, cleanup, and logging handoff

- Ingestion saves complete raw Plaid responses and keeps cursor updates in the same transaction as the raw writes. `jobs.process_accounts.apply_plaid_accounts(run_id)` reads the selected run's account responses and uses the existing upsert through `write_account_rows()`. The daily job runs accounts before transactions and balance snapshots. The account writer skips rows without an account ID and currently writes SQL `NULL` to `accounts.raw`, including on updates. The original response remains in Bronze.
- `jobs/spark.py` shares session setup, SSL-required JDBC reads, and the account lookup join. Database cleanup moved the account write loop into its repository, standardized JSONB parameter adaptation, consolidated duplicate Plaid item and token helpers, and removed unused helpers.
- `app_logging.py` configures Python console logging for job entry points and the Plaid API. Ingestion, Spark jobs, PDF/email, retries, sandbox setup, and schema initialization log relevant events and stage outcomes. Timed stage logs include the run ID when available and log exception types without raw exception text. CLI failures still exit with a nonzero status.
- Current Spark stage counts are limited to `duplicates_skipped` for balance snapshots and `unmatched_changes` for transaction modifications/removals. The account stage logs status and duration and has no row counts yet. Spark input totals, successful write totals, and persistent stage metrics remain pending.
- Compilation and mocked checks covered the daily call order, writer parameters, raw JSON serialization, account row handling, log fields, skipped email, and safe failure output. Live Spark and PostgreSQL execution of this refactor and logging work remain unverified.

## Working conventions

- Work incrementally. Explain one concrete change at a time in plain language and agree on it before editing. Use meaningful names. When the user is learning, explain what the code does and why it belongs in the pipeline.
- Keep `.env`, credentials, tokens, and financial payloads out of logs and shared output. Use synthetic fixtures for local verification. Get approval before installing dependencies, running ingestion, applying schema changes, or contacting live services.
- Check the working tree before editing and preserve unrelated changes.
- Use configured table names and preserve sandbox and production isolation. Keep raw data and existing reporting history available when making schema or processing changes.
- Preserve the transaction between raw writes and cursor advancement. Define replay order and stable keys before adding writes so retries do not duplicate transactions or balance snapshots.
- Keep the Discover-to-Capital One migration separate until Spark processing and reporting are working. Before switching balance sources, verify which Plaid and manual balances represent the same account so it is counted once.

## Next step

**Next change.** Add account metrics first. Count rows received by `write_account_rows()`, accounts successfully upserted, and rows skipped for missing account IDs during the existing iteration. Return the counts and add them to the account stage completion log after commit. Verify with synthetic rows and a failed-write case. The upsert returns an ID for both inserts and updates, so report a combined upsert count.

Then add balance and transaction metrics from the existing writer loops and PostgreSQL write results. Track balance inserts and duplicates. Track transaction additions inserted, duplicates skipped, modifications applied, removals marked, and unmatched modifications/removals. Distinguish writer input from raw payload and parsed record counts. Reuse existing operations when measuring counts. Add persistent stage metrics and status after the console totals are verified.

**Existing follow-ups.**

The Spark account job intentionally writes SQL `NULL` to `accounts.raw` for now, including when it updates an existing account. Defer copying the original account object until the follow-up for curated `raw` fields. The complete source response remains in Bronze.

1. Start with `balance_snapshots.raw`. In `jobs/process_balances.py`, read the `accounts` array as JSON strings and expand it to one string per account. Keep the original nested `balances` object as `raw`, then extract the typed columns separately. Add `raw` to the balance writer's insert as JSONB. Verify with synthetic data containing an extra field outside the typed schema. The column already exists and requires no schema change.
2. Apply the same pattern to `transactions.raw`. Save the complete original Plaid transaction for additions and modifications. Preserve the saved raw object on a removal. The `ARRAY<STRING>` extraction approach was verified in local Spark for both payload shapes. Both writers currently leave `raw` null on insert.
3. Verify the PDF and reporting totals with fixture data, including environment isolation and reruns, before trying the full daily flow against live services.

## Deferred follow-ups

- Persist unmatched modifications and removals in a `transaction_processing_issues` table. Keep the daily job processing other changes. Record the transaction ID, run ID, source page, and change type. Track open and resolved issues and deduplicate retries. For missing modifications, review earlier raw pages for the original addition before recovering a row. An unmatched removal has no saved row to mark.
- Persist stage status and metrics, then schedule the pipeline after the main flow is complete. Keep automated daily email optional as dashboard and on-demand reporting take priority. There is no scheduler wired to `DAILY_DIGEST_HOUR`.
- Define behavior for repeated changes to the same transaction on the same page and for replay of older runs. The current Spark sort only orders pages within a Plaid item. Also review missing `next_cursor` handling and how to reset the cursor when a Plaid item is replaced in place.
- Scope week-, month-, and year-to-date reports to the selected environment when sandbox and production share a database. Merchant-rule classifications currently live in reporting. Move them when the curated categorization design is ready, then have reporting consume the curated result. Plaid's legacy `category` and `category_id` can be null.
