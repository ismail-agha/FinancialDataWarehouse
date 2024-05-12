import os
import re
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from configs.config import get_timestamp

def send_email(sender_email, sender_password, receiver_email, subject, body, attachment_paths=None):
    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # Attach the body
    msg.attach(MIMEText(body, 'plain'))

    # Attach the file (if provided)
    if attachment_paths:
        for attachment_path in attachment_paths:
            print(f'attachment_path = {attachment_path}')

            with open(attachment_path, 'rb') as file:
                attachment = MIMEApplication(file.read(), Name=os.path.basename(attachment_path))
            attachment['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
            msg.attach(attachment)

    # Connect to the SMTP server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)

    # Send the email
    server.sendmail(sender_email, receiver_email, msg.as_string())

    # Close the connection
    server.quit()

def email():
    # Example usage
    sender_email = 'ismailaga118@gmail.com'
    sender_password = ''
    receiver_email = 'ismailaga.of@gmail.com'
    subject = f'FDW - Daily Execution Status ({get_timestamp()}) '

    # Filter files based on timestamp pattern
    log_directory = os.path.join(parent_dir, f"logs")  # '../logs'
    timestamp_pattern = get_timestamp()
    attachment_paths = []
    files = os.listdir(log_directory)

    # Read fdw_orchestrator file
    with open(log_directory+f'/fdw_orchestrator_{get_timestamp()}.log', "r") as log_file:
        log_contents = log_file.read()
    body = log_contents + f'\nRegards,\nFDW'

    for file in files:
        match = re.search(timestamp_pattern, file)
        if match:
            timestamp = match.group()
            attachment_paths.append(os.path.join(log_directory, file))

    print(f' attachment_paths = {attachment_paths}')

    # Ensure that attachment_paths is a single string representing a file path
    if attachment_paths:
        attachment_path = attachment_paths[0]  # Take the first file path from the list

    print(f' attachment_paths = {attachment_path}')

    send_email(sender_email, sender_password, receiver_email, subject, body, attachment_paths)


if __name__ == "__main__":
    email()