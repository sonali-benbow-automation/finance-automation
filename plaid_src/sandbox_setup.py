from plaid.model.institutions_search_request import InstitutionsSearchRequest
from plaid.model.country_code import CountryCode
from plaid.model.products import Products
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest

from plaid_src.client import get_plaid_client
from db.db import db_conn
from db.repos.items import item_exists, upsert_item
from config import get_sandbox_plans


def find_institution_id(client, query, preferred_name=None):
    req = InstitutionsSearchRequest(
        query=query,
        products=[Products("transactions")],
        country_codes=[CountryCode("US")],
    )
    resp = client.institutions_search(req)
    institutions = resp.get("institutions", [])
    if not institutions:
        raise RuntimeError(f"No institutions found for query: {query}")
    if preferred_name:
        for inst in institutions:
            if (inst.get("name") or "").strip().lower() == preferred_name.strip().lower():
                return inst["institution_id"], inst["name"]
    for inst in institutions:
        if (inst.get("name") or "").strip().lower() == query.strip().lower():
            return inst["institution_id"], inst["name"]
    inst = institutions[0]
    return inst["institution_id"], inst["name"]


def create_item(client, institution_id, initial_products):
    cleaned = []
    for p in initial_products:
        if p == "balances":
            cleaned.append("transactions")
        else:
            cleaned.append(p)
    products = [Products(p) for p in cleaned]
    pub_req = SandboxPublicTokenCreateRequest(
        institution_id=institution_id,
        initial_products=products,
    )
    pub_resp = client.sandbox_public_token_create(pub_req)
    exch_req = ItemPublicTokenExchangeRequest(public_token=pub_resp["public_token"])
    exch_resp = client.item_public_token_exchange(exch_req)
    return exch_resp["access_token"], exch_resp["item_id"]


def main():
    client = get_plaid_client()
    plans = get_sandbox_plans()
    created = 0
    skipped = 0
    with db_conn() as conn:
        for plan in plans:
            label = plan["label"]
            if item_exists(conn, label):
                skipped += 1
                continue
            institution_id, matched_name = find_institution_id(
                client,
                plan["query"],
                plan.get("preferred_name"),
            )
            access_token, item_id = create_item(
                client,
                institution_id,
                plan.get("initial_products", ["transactions"]),
            )

            upsert_item(
                conn,
                label=label,
                institution_name=matched_name,
                institution_id=institution_id,
                item_id=item_id,
                access_token_plaintext=access_token,
                transactions_enabled=plan.get("transactions_enabled", False),
                balances_enabled=plan.get("balances_enabled", True),
            )
            created += 1

    print(f"Sandbox setup complete. created={created} skipped={skipped}")


if __name__ == "__main__":
    main()