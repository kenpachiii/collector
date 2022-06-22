import logging
import smtplib
import time
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

queue = []

def exception(server: smtplib.SMTP, recipients: list, msg: str):
    msg = MIMEText(msg)
    msg['From'] = 'botti.notification@gmail.com'
    msg['To'] = ', '.join(recipients)

    server.sendmail('botti.notification@gmail.com', recipients, msg.as_string())

def delay_message(msg: str) -> bool:

    delay = False

    for i in range(0, len(queue)):

        (sms, timestamp) = queue[i]
        if msg == sms and int(time.time()) - timestamp < 3600:
            delay = True

    return delay

def update_queue(msg: str) -> None:
    
    for i in range(0, len(queue)):

        (sms, timestamp) = queue[i]
        if msg == sms:
            queue.pop(i)
            return

    queue.append((msg, int(time.time())))

def send_sms(msg: str) -> None:
    try:

        if not delay_message(msg):

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

                update_queue(msg)

    except Exception as e:
        logger.error('failed sending sms {}'.format(str(e)))
        pass

    