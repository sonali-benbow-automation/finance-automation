import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from config import TIMEZONE, NOTIFICATIONS_ENABLED
from db.db import db_conn
from db.repos.notifications import upsert_notification
from notify.daily_summary import build_daily_summary_html

load_dotenv()

TZ = ZoneInfo(TIMEZONE or "America/New_York")


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def send_daily_digest_email(run_id, subject=None, include_transactions=True):
    channel = "email"
    if not NOTIFICATIONS_ENABLED:
        with db_conn() as conn:
            upsert_notification(
                conn=conn,
                run_id=run_id,
                channel=channel,
                status="skipped",
                message="NOTIFICATIONS_ENABLED is false",
                error=None,
            )
        return {"skipped": True}
    smtp_user = require_env("SMTP_EMAIL")
    smtp_password = require_env("SMTP_PASS")
    smtp_host = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("EMAIL_SMTP_PORT", "587"))
    from_addr = os.getenv("EMAIL_FROM", smtp_user)
    to_addr = require_env("EMAIL_TO")
    if subject is None:
        date_label = datetime.now(TZ).strftime("%Y-%m-%d")
        subject = f"Daily Finance Summary: {date_label}"
    html_body = build_daily_summary_html(include_transactions=include_transactions)
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"Finance Digest <{from_addr}>"
    msg["To"] = to_addr
    msg.set_content(html_body, subtype="html")
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        with db_conn() as conn:
            upsert_notification(
                conn=conn,
                run_id=run_id,
                channel=channel,
                status="success",
                message=f"to={to_addr} subject={subject}",
                error=None,
            )
        return {"subject": subject, "to": to_addr}
    except Exception as e:
        err = str(e)
        with db_conn() as conn:
            upsert_notification(
                conn=conn,
                run_id=run_id,
                channel=channel,
                status="failed",
                message=f"to={to_addr if 'to_addr' in locals() else ''} subject={subject if subject else ''}",
                error=err,
            )
        raise RuntimeError(f"Email send failed: {err}") from e


def main():
    raise RuntimeError("Use daily_sync.py as the entry point for scheduled runs.")


if __name__ == "__main__":
    main()