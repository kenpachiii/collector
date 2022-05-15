import logging
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

def exception(server: smtplib.SMTP, recipients: list, msg: str):
    msg = MIMEText(msg)
    msg['From'] = 'botti.notification@gmail.com'
    msg['To'] = ', '.join(recipients)

    server.sendmail('botti.notification@gmail.com', recipients, msg.as_string())
    
def send_sms(msg: str) -> None:
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:

            server.ehlo()
            server.starttls()
            server.login('botti.notification@gmail.com', 'yygakfowwmpogiuy')
            
            recipients = ['9286323030@vtext.com']

            msg = MIMEText(msg)
            msg['From'] = 'botti.notification@gmail.com'
            msg['To'] = ', '.join(recipients)

            server.sendmail('botti.notification@gmail.com', recipients, msg.as_string())

            server.close()

    except Exception as e:
        logger.error('failed sending sms {}'.format(str(e)))
        pass

    