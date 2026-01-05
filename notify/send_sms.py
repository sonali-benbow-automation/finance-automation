import os
from dotenv import load_dotenv
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from notify.daily_summary import build_daily_summary_text

load_dotenv()

def _require(name):
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

def send_summary_text():
    account_sid = _require("TWILIO_ACCOUNT_SID")
    auth_token = _require("TWILIO_AUTH_TOKEN")
    from_number = _require("TWILIO_PHONE_NUMBER")
    to_number = _require("MY_NUMBER")
    client = Client(account_sid, auth_token)
    body = build_daily_summary_text()

    try:
        msg = client.messages.create(to=to_number, from_=from_number, body=body)
        return msg.body, msg.sid
    except TwilioRestException as e:
        raise RuntimeError(f"Twilio error: {e}") from e

def main():
    body, sid = send_summary_text()
    print("Sent SMS")
    print(f"SID: {sid}")

if __name__ == "__main__":
    main()