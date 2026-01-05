from plaid.configuration import Configuration
from plaid.api_client import ApiClient
from plaid.api.plaid_api import PlaidApi
from config import PLAID_CLIENT_ID, PLAID_SECRET, PLAID_HOST

def get_plaid_client():
    config = Configuration(
        host=PLAID_HOST,
        api_key={
            "clientId": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET,
        },
    )
    return PlaidApi(ApiClient(config))