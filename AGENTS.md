# Finance Automation

## Project goal

Build an automated personal finance pipeline using Plaid, Supabase PostgreSQL, local Apache Spark, and daily financial reporting.

## Architecture

- `config.py` loads environment settings and defines the Plaid environment, database table names, transaction start date, timezone, and notification settings.
- `plaid_src/` contains the Plaid client, sandbox setup, and Flask Hosted Link API. The API logs webhook events and exchanges public tokens. `db/repos/items.py` stores access tokens encrypted with PostgreSQL `pgcrypto`.
- `db/init_db.py` substitutes configured table names into `db/schema.sql`. The schema defines items, accounts, cursors, run records, raw Plaid payloads, notification records, Hosted Link sessions, webhook events, merchant rules, manual balances, and manual balance history. Its grants and policies expect Supabase roles.
- `ingest/ingest_plaid.py` selects active items for the configured environment, updates account metadata, and stores balance responses and transaction sync pages as JSONB. Raw writes and cursor updates commit together. Run records track ingestion success or failure.
- `jobs/test_spark.py` is a local Spark JDBC probe for `plaid_transactions_raw`. It displays rows, schema, and count. PySpark is absent from `requirements.txt`.
- `notify/queries.py` expects normalized balance snapshots and transactions. `notify/daily_summary.py` assembles the report data, `notify/pdf_report.py` renders it, and `notify/send_email.py` sends it over SMTP and records delivery status.
- `jobs/daily_sync.py` is intended to run ingestion and send the digest. `jobs/retry_notifications.py` retries failed email notifications. The repository has no scheduler wired to `DAILY_DIGEST_HOUR`.

## Verified development status

This snapshot was reviewed on `main` at `7eb0e92` (`Change to local Spark`, February 22, 2026).

- Commit `84d6389` replaced normalized ingestion writes with raw JSONB storage. Use its parent as the reference for the previous reporting schema and writers.
- The Spark transformation and write path is missing. No job converts raw balance responses or transaction changes into reporting rows.
- Both job entry points import `notify/queries.py` through the email module. The import fails on the missing `TABLES["balance_snapshots"]` key. `TABLES["transactions"]` is also missing, and the current schema creates neither table.
- The reporting SQL contains merchant rules, Plaid category fallback, transfer and reimbursement classifications, balance comparisons, and period totals. Week-, month-, and year-to-date queries need explicit environment scoping when sandbox and production share a database.
- Transaction sync advances through `next_cursor` and applies `TRANSACTIONS_START_DATE` to added and modified transactions only when no cursor is saved. The current code lacks a check for a missing `next_cursor`. Replacing a Plaid item in place can also carry forward the cursor attached to its database row.
- The Discover sandbox plan and manual Discover Savings entry are present. The Discover-to-Capital One migration and its balance deduplication rules remain deferred.
- These findings come from source and Git history. The deployed schema, stored data, and live integrations have not been verified.

## Working conventions

- Work incrementally. Explain each proposed implementation step and agree on it before editing.
- Keep `.env`, credentials, tokens, and financial payloads out of logs and shared output. Use synthetic fixtures for local verification. Get approval before installing dependencies, running ingestion, applying schema changes, or contacting live services.
- Check the working tree before editing and preserve unrelated changes.
- Use configured table names and preserve sandbox and production isolation. Keep raw data and existing reporting history available when making schema or processing changes.
- Preserve the transaction between raw writes and cursor advancement. Define replay order and stable keys before adding writes so retries do not duplicate transactions or balance snapshots.
- Keep the Discover-to-Capital One migration separate until Spark processing and reporting are working. Before switching balance sources, verify which Plaid and manual balances represent the same account so it is counted once.

## Next steps

1. Compare the last normalized schema and writers with the current report queries. Agree on transaction and balance keys, page ordering, how additions, modifications, and removals affect state, the start-date rule, and how to preserve history that raw replay cannot reconstruct.
2. Build a local Spark transformation for one synthetic run. Verify account mapping, balance snapshots, transaction changes, and identical output on replay.
3. Restore the reporting schema and config through a data-preserving migration. Write the processed rows and verify that the existing SQL and PDF work with fixture data, including environment isolation.
4. Wire the daily job to ingest, process, build the PDF, and send it in that order. Verify the retry path against the processed data before enabling live delivery.
