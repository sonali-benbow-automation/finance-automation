import os
from flask import request

def require_admin():
    expected = os.getenv("ADMIN_API_TOKEN")
    if not expected:
        raise RuntimeError("ADMIN_API_TOKEN is required")
    got = request.headers.get("X-Admin-Token")
    if not got or got != expected:
        return False
    return True