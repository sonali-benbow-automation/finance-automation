# Finance Automation

## Project goal

Build an automated personal finance pipeline using Plaid, Supabase PostgreSQL, local Apache Spark, and daily financial reporting.

## Architecture

- `config.py` loads environment settings and defines the Plaid environment, database table names, transaction start date, timezone, and notification settings.
- `plaid_src/` contains the Plaid client, sandbox setup, and Flask Hosted Link API. The API logs webhook events and exchanges public tokens. `db/repos/items.py` stores access tokens encrypted with PostgreSQL `pgcrypto`.
- `db/init_db.py` substitutes configured table names into `db/schema.sql`. The schema defines 13 tables, including `transactions`, both raw Plaid tables, and the manual balance tables. The security statements expect Supabase roles.
- `ingest/ingest_plaid.py` selects active items for the configured environment, updates account metadata, and stores balance responses and transaction sync pages as JSONB. Raw writes and cursor updates commit together. Run records track ingestion success or failure.
- `jobs/process_transactions.py` replaced the original `jobs/test_spark.py` JDBC probe. It reads configured raw transactions, accounts, and saved transactions over JDBC with SSL required. It parses and expands Plaid's lists, formats transaction fields, and matches accounts using `(plaid_item_pk, account_id)` to get `account_pk`.
- `notify/queries.py` expects normalized balance snapshots and transactions. `notify/daily_summary.py` assembles the report data, `notify/pdf_report.py` renders it, and `notify/send_email.py` sends it over SMTP and records delivery status.
- `jobs/daily_sync.py` is intended to run ingestion and send the digest. `jobs/retry_notifications.py` retries failed email notifications. The repository has no scheduler wired to `DAILY_DIGEST_HOUR`.

## Verified development status

The Spark and schema work was committed as `0530a7c` and pushed to GitHub `main` in merge commit `bf87857` on November 22, 2026.

- Commit `84d6389` replaced normalized ingestion writes with raw JSONB storage. Use its parent as the reference for the previous reporting schema and writers.
- The local Spark JDBC read succeeded in PyCharm. The verified setup was Python 3.12 and PySpark 4.3.0 in the project `.venv`. PySpark is still absent from `requirements.txt`. When running synthetic tests, point `PYSPARK_PYTHON` at the same interpreter as the driver if Spark picks up a different system Python.
- The shared Spark preparation code handles `added` and `modified` lists. Added rows set both seen-run IDs from the incoming run. Modified rows get `last_seen_run_id` from the incoming run and use an inner join on `transaction_id` to preserve `first_seen_run_id` from the saved row. A modified row with no saved match is currently omitted. Synthetic Spark checks passed for both paths, including the account join and run IDs.
- The removal function extracts `transaction_id` and `run_id` for a later database update. The script previews additions with `.show()` and calls the modified and removed functions without an action on their results. It filters raw responses to the explicit run ID before preparing changes, does not retain page order in the extracted rows, and performs no database writes. Balance processing is still missing.
- `TRANSACTIONS_TABLE=transactions` was restored in the local ignored `.env`, with the matching setting in `config.py` and substitution in `db/init_db.py`. The live `transactions` columns and constraints were verified through PostgreSQL metadata. Live row-level security and the `service_role_all` policy were confirmed. The schema was rerun successfully in Supabase, and the saved SQL uses unqualified transaction table references. A metadata audit covered the columns, constraints, and indexes of all 13 configured tables. Supabase's existing service-role grants should be checked independently when provisioning a database. The initializer uses `CREATE TABLE IF NOT EXISTS`, so existing column changes require explicit migrations.
- Both report job entry points still import `notify/queries.py` through the email module and fail on the missing `TABLES["balance_snapshots"]` key. `transactions` is configured again. The balance snapshots schema and the reporting pipeline still need to be restored.
- The reporting SQL contains merchant rules, Plaid category fallback, transfer and reimbursement classifications, balance comparisons, and period totals. Week-, month-, and year-to-date queries need explicit environment scoping when sandbox and production share a database.
- Transaction sync advances through `next_cursor` and applies `TRANSACTIONS_START_DATE` to added and modified transactions only when no cursor is saved. The current code lacks a check for a missing `next_cursor`. Replacing a Plaid item in place can also carry forward the cursor attached to its database row.
- The Discover sandbox plan and manual Discover Savings entry are present. The Discover-to-Capital One migration and its balance deduplication rules remain deferred.

## November 23 handoff

- Call `jobs.process_transactions.main(run_id)` from Python with an explicit ingestion run ID. The command-line parser was removed at the user's request. All three preparation helpers receive the same filtered raw responses and use the `run_id` column already on those rows.
- The mocked two-run wiring check passed. The assistant's local Spark check could not open a loopback port. The user confirmed they completed the run-filter check. The daily job still needs to pass the ID returned by `run_ingest()` to Spark.
- The local `.venv/bin/python` was verified as Python 3.11.14 with PySpark 4.1.1. On this Mac, PyCharm had registered that executable under the label `Python 3.9 (finance-automation)`. Select the executable by path if PyCharm reports no interpreter. Machine-specific PyCharm settings were left local.

## Working conventions

- Work incrementally. Explain one concrete change at a time in plain language and agree on it before editing. Use meaningful names. When the user is learning, explain what the code does and why it belongs in the pipeline.
- Keep `.env`, credentials, tokens, and financial payloads out of logs and shared output. Use synthetic fixtures for local verification. Get approval before installing dependencies, running ingestion, applying schema changes, or contacting live services.
- Check the working tree before editing and preserve unrelated changes.
- Use configured table names and preserve sandbox and production isolation. Keep raw data and existing reporting history available when making schema or processing changes.
- Preserve the transaction between raw writes and cursor advancement. Define replay order and stable keys before adding writes so retries do not duplicate transactions or balance snapshots.
- Keep the Discover-to-Capital One migration separate until Spark processing and reporting are working. Before switching balance sources, verify which Plaid and manual balances represent the same account so it is counted once.

## Next steps

1. **Next change**. Carry the existing `page_index` column through both selections in `extract_transaction_details()` and through the removal output. This keeps the source page number on each addition, modification, and removal. Keep this step read-only.
2. Before writing, define replay order for each Plaid item and agree on how to handle repeated changes, missing account matches, and a modification whose transaction is added in the same run. Use the writer from `84d6389^` as a reference for a replay-safe PostgreSQL write. Preserve the saved `first_seen_run_id`, update `last_seen_run_id` for changes, and mark removals without deleting transaction history.
3. Restore the balance snapshots schema and config through a data-preserving migration and build the balance transformation. Verify the SQL and PDF with fixture data, including environment isolation and replay.
4. Wire the daily job to ingest, process the returned run, build the PDF, and send it in that order. Verify the retry path before enabling live delivery.
