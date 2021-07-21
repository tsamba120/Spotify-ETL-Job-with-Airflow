import psycopg2
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import email, em_password

port = 465 # For SSL
smtp_server = 'smtp.gmail.com'

receiver_email = 'terencerustia@gmail.com'
message = '''\
    Subject: Hi there\n

    I love Alana
    This message is sent from Python.'''




# Create a secure SSL context connection with server and send email
context = ssl.create_default_context()

with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(email, em_password)
    server.sendmail(email, receiver_email, message)