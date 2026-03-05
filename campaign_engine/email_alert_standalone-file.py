import smtplib
from email.message import EmailMessage
from datetime import datetime
import logging
from config.campaign_config import APP_PASSWORD

logger = logging.getLogger(__name__)


def send_email(subject, body, recipients,
               sender, app_password):

    try:
        msg = EmailMessage()

        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)

        html_body = f"""
        <html>
            <body>
                {body}
                <br><br>
                Sent at: {datetime.now():%Y-%m-%d %H:%M:%S}
            </body>
        </html>
        """

        msg.set_content("HTML email required")
        msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, app_password)
            server.send_message(msg)

        logger.info("Email sent successfully")

    except Exception as e:
        logger.error(f"Email failed: {e}")


send_email(
    subject="Campaign Completed",
    body="<b>File processed successfully</b>",
    recipients=["balkrishna.11.shah@gmail.com"],
    sender="balkrishna.datacurate@gmail.com",
    app_password=APP_PASSWORD
)
