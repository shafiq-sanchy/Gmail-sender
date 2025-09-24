# tasks.py

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_task(sender_email, password, recipient_email, subject, message):
    """
    Function to send an email. This will be executed by the RQ worker in the background.
    """
    try:
        # Create the email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))

        # Connect to the Gmail SMTP server and send the email
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        
        print(f"Successfully sent email to {recipient_email}")
        return f"Email sent to {recipient_email}"
    except Exception as e:
        print(f"Error sending email to {recipient_email}: {e}")
        return f"Failed to send email to {recipient_email}: {str(e)}"
