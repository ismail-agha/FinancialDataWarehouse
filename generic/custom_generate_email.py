import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def send_email(sender_email, sender_password, receiver_email, subject, body, attachment_path=None):
    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # Attach the body
    msg.attach(MIMEText(body, 'plain'))

    # Attach the file (if provided)
    if attachment_path:
        with open(attachment_path, 'rb') as file:
            attachment = MIMEApplication(file.read(), Name=attachment_path)
        attachment['Content-Disposition'] = f'attachment; filename="{attachment_path}"'
        msg.attach(attachment)

    # Connect to the SMTP server
    server = smtplib.SMTP('smtp.example.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)

    # Send the email
    server.sendmail(sender_email, receiver_email, msg.as_string())

    # Close the connection
    server.quit()

# Example usage
sender_email = 'your_email@example.com'
sender_password = 'your_email_password'
receiver_email = 'recipient@example.com'
subject = 'Test Email'
body = 'This is a test email sent from Python.'
attachment_path = 'example_attachment.txt'  # Path to the attachment file (optional)

send_email(sender_email, sender_password, receiver_email, subject, body, attachment_path)
