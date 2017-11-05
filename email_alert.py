import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(sender_adr, recipient_adr, subject, body):
    print("Sending Email")
    msg = MIMEMultipart()
    msg["From"] = sender_adr
    msg["To"] = recipient_adr
    msg["Subject"] = subject
    msg.attach(MIMEText(body))

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(sender_adr, "twitternumbers")
    server.sendmail(sender_adr, recipient_adr, msg.as_string())
    server.quit()
    print("Email Sent")


send_email("twitternumbersalerts@gmail.com",
           "twitternumbersalerts@gmail.com",
           "Alert!",
           "Another Alert message")
