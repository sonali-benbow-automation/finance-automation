from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from config import (
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_PHONE_NUMBER,
)

def send_sms(to_number, body):
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    try:
        msg = client.messages.create(
            to=to_number,
            from_=TWILIO_PHONE_NUMBER,
            body=body,
        )
        return msg.sid
    except TwilioRestException as e:
        raise RuntimeError(f"Twilio error: {e}") from e