import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from config import TIMEZONE, NOTIFICATIONS_ENABLED
from notify.daily_summary import build_daily_summary_html

load_dotenv()

TZ = ZoneInfo(TIMEZONE or "America/New_York")


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def send_daily_digest_email(subject=None, include_transactions=True):
    if not NOTIFICATIONS_ENABLED:
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
        return {"subject": subject, "to": to_addr}
    except Exception as e:
        raise RuntimeError(f"Email send failed: {e}") from e


def main():
    result = send_daily_digest_email()
    if result.get("skipped"):
        print("Notifications are disabled. Email not sent.")
        return
    print("Sent email")
    print(f"To: {result['to']}")
    print(f"Subject: {result['subject']}")


if __name__ == "__main__":
    main()