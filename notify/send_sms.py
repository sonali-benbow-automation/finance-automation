from config import NOTIFICATIONS_ENABLED, MY_NUMBER
from notify.daily_summary import build_daily_summary_text
from notify.twilio_client import send_sms

def send_summary_text():
    if not NOTIFICATIONS_ENABLED:
        return None
    body = build_daily_summary_text(include_transactions=True)
    sid = send_sms(to_number=MY_NUMBER, body=body)
    return sid

def main():
    sid = send_summary_text()

if __name__ == "__main__":
    main()