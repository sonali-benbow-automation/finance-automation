import os
from flask import Flask, request, jsonify

from db.db import db_conn
from plaid_src.auth import require_admin
from plaid_src.client import get_plaid_client
from plaid_src.link import create_link_token, exchange_public_token_and_store_item

from db.repos.hosted_link_sessions import (
    create_session,
    get_by_link_token,
    mark_success,
    mark_failed,
)
from db.repos.webhook_events import insert_event

from plaid.model.link_token_get_request import LinkTokenGetRequest


def normalize_status(v):
    if v is None:
        return ""
    return str(v).strip().upper()


def extract_public_token_from_link_token_get(resp_dict):
    # Hosted Link docs: public token should be available via /link/token/get for completed sessions.
    # Response shapes can vary; try a few explicit locations.
    tokens = resp_dict.get("public_tokens")
    if isinstance(tokens, list) and tokens:
        return tokens[0]

    token = resp_dict.get("public_token")
    if isinstance(token, str) and token:
        return token

    link_session = resp_dict.get("link_session") or {}

    tokens2 = link_session.get("public_tokens")
    if isinstance(tokens2, list) and tokens2:
        return tokens2[0]

    token2 = link_session.get("public_token")
    if isinstance(token2, str) and token2:
        return token2

    # Multi-Item Link can include results; keep this for completeness
    results = resp_dict.get("results") or {}
    item_add_results = results.get("item_add_results")
    if isinstance(item_add_results, list) and item_add_results:
        first = item_add_results[0] or {}
        t = first.get("public_token")
        if isinstance(t, str) and t:
            return t

    return None


def create_app():
    app = Flask(__name__)

    @app.get("/health")
    def health():
        return jsonify({"ok": True})

    @app.post("/api/plaid/link_token")
    def plaid_link_token():
        if not require_admin():
            return jsonify({"error": "unauthorized"}), 401

        data = request.get_json(force=True) or {}
        user_id = data.get("user_id", "me")
        label = data.get("label", "Primary")

        resp = create_link_token(
            user_id=f"{user_id}:{label}",
            client_name="Finance",
            products=data.get("products") or ["transactions"],
            country_codes=data.get("country_codes") or ["US"],
            language=data.get("language") or "en",
            redirect_uri=data.get("redirect_uri") or None,
            webhook=data.get("webhook") or None,
            hosted_link=False,
        )
        return jsonify({"link_token": resp["link_token"]})

    @app.post("/api/plaid/hosted_link/create")
    def plaid_hosted_link_create():
        if not require_admin():
            return jsonify({"error": "unauthorized"}), 401

        data = request.get_json(force=True) or {}
        label = data.get("label", "Primary")
        user_id = data.get("user_id", "me")
        webhook_url = data.get("webhook_url")

        if not webhook_url:
            return jsonify({"error": "missing webhook_url"}), 400

        resp = create_link_token(
            user_id=f"{user_id}:{label}",
            client_name="Finance",
            products=data.get("products") or ["transactions"],
            country_codes=data.get("country_codes") or ["US"],
            language=data.get("language") or "en",
            redirect_uri=None,
            webhook=webhook_url,
            hosted_link=True,
        )

        link_token = resp["link_token"]
        hosted_link_url = resp.get("hosted_link_url")
        if not hosted_link_url:
            return jsonify({"error": "plaid did not return hosted_link_url"}), 500

        with db_conn() as conn:
            create_session(
                conn=conn,
                label=label,
                link_token=link_token,
                hosted_link_url=hosted_link_url,
                webhook_url=webhook_url,
            )

        return jsonify({"link_token": link_token, "hosted_link_url": hosted_link_url})

    @app.post("/api/plaid/hosted_link/finalize")
    def plaid_hosted_link_finalize():
        if not require_admin():
            return jsonify({"error": "unauthorized"}), 401

        data = request.get_json(force=True) or {}
        link_token = data.get("link_token")
        if not link_token:
            return jsonify({"error": "missing link_token"}), 400

        client = get_plaid_client()

        # Correct Plaid SDK usage: LinkTokenGetRequest + .to_dict()
        req = LinkTokenGetRequest(link_token=link_token)
        resp_obj = client.link_token_get(req)
        resp = resp_obj.to_dict() if hasattr(resp_obj, "to_dict") else dict(resp_obj)

        link_session = resp.get("link_session") or {}
        status_norm = normalize_status(link_session.get("status") or resp.get("status"))

        public_token = extract_public_token_from_link_token_get(resp)

        # If not completed yet, caller can retry finalize
        if status_norm != "SUCCESS":
            return jsonify(
                {
                    "ok": True,
                    "status": status_norm,
                    "public_token_present": bool(public_token),
                }
            )

        # Completed but token not present yet (retry)
        if not public_token:
            return jsonify(
                {
                    "ok": True,
                    "status": status_norm,
                    "public_token_present": False,
                    "stored": False,
                    "message": "SUCCESS but no public_token returned by link/token/get yet; retry finalize",
                }
            )

        with db_conn() as conn:
            sess = get_by_link_token(conn, link_token)
            label = (sess or {}).get("label") or "Primary"

            try:
                exchange_public_token_and_store_item(
                    conn=conn,
                    public_token=public_token,
                    label=label,
                    institution_name="unknown",
                    institution_id="unknown",
                    transactions_enabled=True,
                    balances_enabled=True,
                )
                mark_success(conn, link_token)
            except Exception as e:
                mark_failed(conn, link_token, error=str(e))
                raise

        return jsonify({"ok": True, "status": status_norm, "stored": True})

    @app.post("/api/plaid/webhook")
    def plaid_webhook():
        payload = request.get_json(force=True) or {}

        # Always persist raw webhook for audit
        with db_conn() as conn:
            insert_event(conn, payload)

        webhook_type = payload.get("webhook_type")
        webhook_code = payload.get("webhook_code")

        # Your observed payload is LINK/EVENTS (telemetry). Log only; do not fail sessions.
        if webhook_type == "LINK" and webhook_code == "EVENTS":
            return jsonify({"ok": True, "logged": True, "handled": False})

        # If Plaid ever sends SESSION_FINISHED here, you can still finalize via the endpoint.
        return jsonify({"ok": True, "logged": True, "handled": False})

    @app.post("/api/plaid/exchange")
    def plaid_exchange():
        if not require_admin():
            return jsonify({"error": "unauthorized"}), 401

        data = request.get_json(force=True) or {}
        public_token = data.get("public_token")
        label = data.get("label")
        institution_id = data.get("institution_id")
        institution_name = data.get("institution_name")

        if not public_token:
            return jsonify({"error": "missing public_token"}), 400
        if not label:
            return jsonify({"error": "missing label"}), 400
        if not institution_id or not institution_name:
            return jsonify({"error": "missing institution_id or institution_name"}), 400

        transactions_enabled = bool(data.get("transactions_enabled", True))
        balances_enabled = bool(data.get("balances_enabled", True))

        with db_conn() as conn:
            plaid_item_pk, item_id = exchange_public_token_and_store_item(
                conn=conn,
                public_token=public_token,
                label=label,
                institution_name=institution_name,
                institution_id=institution_id,
                transactions_enabled=transactions_enabled,
                balances_enabled=balances_enabled,
            )

        return jsonify({"plaid_item_pk": plaid_item_pk, "item_id": item_id})

    return app


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    port = int(os.getenv("PORT", "5055"))
    create_app().run(host="0.0.0.0", port=port, debug=debug)