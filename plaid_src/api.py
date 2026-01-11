import os
from flask import Flask, request, jsonify

from db.db import db_conn
from plaid_src.link import create_link_token, exchange_public_token_and_store_item


def create_app():
    app = Flask(__name__)
    @app.post("/api/plaid/link_token")
    def plaid_link_token():
        data = request.get_json(force=True) or {}
        user_id = data.get("user_id", "me")
        label = data.get("label", "Primary")
        link_token = create_link_token(
            user_id=f"{user_id}:{label}",
            client_name="Finance",
            products=data.get("products") or ["transactions"],
            country_codes=data.get("country_codes") or ["US"],
            language=data.get("language") or "en",
            redirect_uri=data.get("redirect_uri"),
        )
        return jsonify({"link_token": link_token})
    @app.post("/api/plaid/exchange")
    def plaid_exchange():
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
    port = int(os.getenv("PORT", "5000"))
    create_app().run(host="0.0.0.0", port=port, debug=debug)