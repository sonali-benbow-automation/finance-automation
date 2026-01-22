import os
import json
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

TABLES = {
    "plaid_items": os.getenv("PLAID_ITEMS_TABLE", "plaid_items"),
    "accounts": os.getenv("ACCOUNTS_TABLE", "accounts"),
    "balance_snapshots": os.getenv("BALANCE_SNAPSHOTS_TABLE", "balance_snapshots"),
    "transactions": os.getenv("TRANSACTIONS_TABLE", "transactions"),
    "cursors": os.getenv("CURSORS_TABLE", "cursors"),
    "runs": os.getenv("RUNS_TABLE", "runs"),
    "notifications": os.getenv("NOTIFICATIONS_TABLE", "notifications"),
    "hosted_link_sessions": os.getenv("HOSTED_LINK_SESSIONS_TABLE", "hosted_link_sessions"),
    "plaid_webhook_events": os.getenv("PLAID_WEBHOOK_EVENTS_TABLE", "plaid_webhook_events"),
}

def table(name):
    return TABLES[name]

DEFAULT_SANDBOX_PLANS = [
    {
        "label": "Chase",
        "query": "Chase",
        "preferred_name": None,
        "transactions_enabled": True,
        "balances_enabled": True,
        "initial_products": ["transactions"],
    },
    {
        "label": "Discover",
        "query": "Discover",
        "preferred_name": None,
        "transactions_enabled": True,
        "balances_enabled": True,
        "initial_products": ["transactions"],
    },
    {
        "label": "Vanguard",
        "query": "Vanguard",
        "preferred_name": None,
        "transactions_enabled": False,
        "balances_enabled": True,
        "initial_products": ["transactions"],
    },
    {
        "label": "Empower Retirement",
        "query": "Empower",
        "preferred_name": "Empower Retirement",
        "transactions_enabled": False,
        "balances_enabled": True,
        "initial_products": ["transactions"],
    },
]

def get_sandbox_plans():
    raw = os.getenv("SANDBOX_PLANS_JSON")
    if not raw:
        return DEFAULT_SANDBOX_PLANS
    return json.loads(raw)


PLAID_ENV = os.getenv("PLAID_ENV", "sandbox")
PLAID_CLIENT_ID = os.getenv("PLAID_CLIENT_ID")
PLAID_SECRET = os.getenv("PLAID_SANDBOX_SECRET" if PLAID_ENV == "sandbox" else "PLAID_PRODUCTION_SECRET")
if not PLAID_CLIENT_ID or not PLAID_SECRET:
    raise RuntimeError("PLAID_CLIENT_ID and PLAID_SECRET are required")

PLAID_TOKEN_KEY = os.getenv("PLAID_TOKEN_KEY")
if not PLAID_TOKEN_KEY:
    raise RuntimeError("PLAID_TOKEN_KEY is required")

PLAID_HOSTS = {
    "sandbox": "https://sandbox.plaid.com",
    "production": "https://production.plaid.com",
}
PLAID_HOST = PLAID_HOSTS[PLAID_ENV]

PLAID_REDIRECT_URI = os.getenv("PLAID_REDIRECT_URI")


INGEST_TRANSACTIONS_DEFAULT = True
INGEST_BALANCES_DEFAULT = True


NOTIFICATIONS_ENABLED = os.getenv("NOTIFICATIONS_ENABLED", "true").lower() == "true"
DAILY_DIGEST_HOUR = int(os.getenv("DAILY_DIGEST_HOUR", "9"))
TIMEZONE = os.getenv("TIMEZONE", "America/New_York")
TRANSACTIONS_START_DATE = os.getenv("TRANSACTIONS_START_DATE")