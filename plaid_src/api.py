import os
from flask import Flask, request, jsonify, Response

from db.db import db_conn
from plaid_src.auth import require_admin
from plaid_src.link import create_link_token, exchange_public_token_and_store_item

from db.repos.hosted_link_sessions import (
    create_session,
    get_by_link_token,
    mark_success,
    mark_failed,
)
from db.repos.webhook_events import insert_event
from config import PLAID_REDIRECT_URI


def create_app():
    app = Flask(__name__)

    @app.get("/health")
    def health():
        return jsonify({"ok": True})
    @app.get("/plaid/redirect")
    def plaid_redirect():
        return Response("ok", mimetype="text/plain")
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
            redirect_uri=PLAID_REDIRECT_URI,
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

    @app.post("/api/plaid/hosted_link/status")
    def plaid_hosted_link_status():
        if not require_admin():
            return jsonify({"error": "unauthorized"}), 401
        data = request.get_json(force=True) or {}
        link_token = data.get("link_token")
        if not link_token:
            return jsonify({"error": "missing link_token"}), 400
        with db_conn() as conn:
            sess = get_by_link_token(conn, link_token)
        if not sess:
            return jsonify({"error": "not_found"}), 404
        return jsonify(
            {
                "ok": True,
                "link_token": sess.get("link_token"),
                "label": sess.get("label"),
                "env": sess.get("env"),
                "status": sess.get("status"),
                "error": sess.get("error"),
                "created_at": str(sess.get("created_at")) if sess.get("created_at") else None,
                "updated_at": str(sess.get("updated_at")) if sess.get("updated_at") else None,
            }
        )

    @app.post("/api/plaid/webhook")
    def plaid_webhook():
        payload = request.get_json(force=True) or {}
        with db_conn() as conn:
            insert_event(conn, payload)
        webhook_type = payload.get("webhook_type")
        webhook_code = payload.get("webhook_code")
        if webhook_type == "LINK" and webhook_code == "EVENTS":
            return jsonify({"ok": True, "logged": True, "handled": False})
        if webhook_type == "LINK" and webhook_code == "SESSION_FINISHED":
            status = (payload.get("status") or "").strip().lower()
            link_token = payload.get("link_token")
            link_session_id = payload.get("link_session_id")
            public_tokens = payload.get("public_tokens") or []
            public_token = public_tokens[0] if isinstance(public_tokens, list) and public_tokens else None
            if not link_token:
                return jsonify(
                    {
                        "ok": True,
                        "logged": True,
                        "handled": False,
                        "error": "missing link_token",
                        "link_session_id": link_session_id,
                    }
                )
            with db_conn() as conn:
                sess = get_by_link_token(conn, link_token)
                if not sess:
                    return jsonify(
                        {
                            "ok": True,
                            "logged": True,
                            "handled": False,
                            "error": "unknown link_token",
                            "link_token": link_token,
                            "link_session_id": link_session_id,
                        }
                    )
                if sess["status"] == "success":
                    return jsonify({"ok": True, "logged": True, "handled": True, "stored": True})
                if status != "success":
                    mark_failed(conn, link_token, error=f"SESSION_FINISHED status={status}")
                    return jsonify({"ok": True, "logged": True, "handled": True, "stored": False})
                if not public_token:
                    mark_failed(conn, link_token, error="SESSION_FINISHED missing public_token")
                    return jsonify({"ok": True, "logged": True, "handled": True, "stored": False})
                plaid_item_pk, item_id, item_label = exchange_public_token_and_store_item(
                    conn=conn,
                    public_token=public_token,
                    label=None,
                    transactions_enabled=True,
                    balances_enabled=True,
                )
                mark_success(conn, link_token)
            return jsonify(
                {
                    "ok": True,
                    "logged": True,
                    "handled": True,
                    "stored": True,
                    "plaid_item_pk": plaid_item_pk,
                    "item_id": item_id,
                    "label": item_label,
                }
            )
        return jsonify({"ok": True, "logged": True, "handled": False})
    return app


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    port = int(os.getenv("PORT", "5055"))
    create_app().run(host="0.0.0.0", port=port, debug=debug)