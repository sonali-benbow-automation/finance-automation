from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.country_code import CountryCode

from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest

from plaid_src.client import get_plaid_client
from db.repos.items import upsert_item


def create_link_token(
    user_id,
    client_name="Finance",
    products=None,
    country_codes=None,
    language="en",
    redirect_uri=None,
    webhook=None,
    hosted_link=False,
):
    client = get_plaid_client()
    products = products or ["transactions"]
    country_codes = country_codes or ["US"]
    kwargs = dict(
        user=LinkTokenCreateRequestUser(client_user_id=str(user_id)),
        client_name=client_name,
        products=[Products(p) for p in products],
        country_codes=[CountryCode(c) for c in country_codes],
        language=language,
    )
    if redirect_uri:
        kwargs["redirect_uri"] = str(redirect_uri)
    if webhook:
        kwargs["webhook"] = str(webhook)
    if hosted_link:
        kwargs["hosted_link"] = {}
    req = LinkTokenCreateRequest(**kwargs)
    resp = client.link_token_create(req)
    out = {"link_token": resp["link_token"]}
    if resp.get("hosted_link_url"):
        out["hosted_link_url"] = resp["hosted_link_url"]
    return out


def exchange_public_token_and_store_item(
    conn,
    public_token,
    env,
    label=None,
    transactions_enabled=True,
    balances_enabled=True,
):
    client = get_plaid_client()
    exch_req = ItemPublicTokenExchangeRequest(public_token=public_token)
    exch_resp = client.item_public_token_exchange(exch_req)
    access_token = exch_resp["access_token"]
    item_id = exch_resp["item_id"]
    item_resp = client.item_get(ItemGetRequest(access_token=access_token))
    item = item_resp.get("item") or {}
    institution_id = item.get("institution_id")
    if not institution_id:
        raise RuntimeError("Plaid item/get did not return institution_id")
    inst_resp = client.institutions_get_by_id(
        InstitutionsGetByIdRequest(
            institution_id=institution_id,
            country_codes=[CountryCode("US")],
        )
    )
    inst = inst_resp.get("institution") or {}
    institution_name = inst.get("name")
    if not institution_name:
        raise RuntimeError("Plaid institutions/get_by_id did not return institution name")
    resolved_label = label if (isinstance(label, str) and label.strip()) else institution_name.strip()
    plaid_item_pk = upsert_item(
        conn,
        label=resolved_label,
        institution_name=institution_name,
        institution_id=institution_id,
        item_id=item_id,
        access_token_plaintext=access_token,
        transactions_enabled=transactions_enabled,
        balances_enabled=balances_enabled,
        env_override=env,
    )
    return plaid_item_pk, item_id, resolved_label