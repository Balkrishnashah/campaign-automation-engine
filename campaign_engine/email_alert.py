import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email_templates import TEMPLATES
from datetime import datetime


import logging

logger = logging.getLogger(__name__)

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "balkrishna.datacurate@gmail.com"
APP_PASSWORD = "xxxxxxxxxxxx"

CC_LIST = [
    # "balkrishna.datacurate@gmail.com"
    # ""
    # ""
    ]

def send_email(subject, html_body, to_email):
    
    msg = MIMEMultipart()
    
    msg["From"] = SENDER_EMAIL
    msg["To"] = to_email
    msg["Cc"] = ", ".join(CC_LIST)
    msg["Subject"] = subject
    
    msg.attach(MIMEText(html_body, "html"))
    
    recipients = [to_email] +CC_LIST
    try: 
        logger.info("Connecting to SMTP server...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, APP_PASSWORD)
            server.send_message(msg, to_addrs=recipients)
            
        logger.info(f"Email sent successfully to {recipients}\n ")

    except Exception as e:
        logger.error(f"Email sending failed for {to_email}: {e}\n", exc_info=True)
        raise
    
def send_alert(alert_type, analyst_email, **kwargs):
    logger.info("Prepping for Email Alert: \n ")
    template = TEMPLATES.get(alert_type)
    if not template:
        raise ValueError("Unknown alert type")
    
    subject = template["subject"]
    
    body = template["body"].format(
        analyst = analyst_email,
        time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        **kwargs
    )
    send_email(subject, body, analyst_email)
    