import psycopg2
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import sender_email, em_password

port = 465 # For SSL
smtp_server = 'smtp.gmail.com'

receiver_email = 'terencerustia@gmail.com'

message = MIMEMultipart('alternative')
message['Subject'] = 'Your Weekly Spotify Metrics'
message['From'] = sender_email
message['To'] = receiver_email

# Create the plain-text and HTML version of your message
text = '''\
    Hi,
    How are you?
    This is Terence'''

html = '''\
    <html>
        <body>
            <div style="background-color: black;">
                <h1 style="color: #1DB954;">Terence's Weekly Spotify Metrics</h1>
                <p style="color: white";>
                    What a week of music! Let's see what you've been listening to.
                </p>
            </div>
        </body>
    </html>
    '''

# Turn these into plain/html MIMEText objects
part1 = MIMEText(text, 'plain')
part2 = MIMEText(html, 'html')

# Add HTML/plain-text parts to MIMEMultipart message
# The email client will try to render the last part first
message.attach(part1)
message.attach(part2)


# Create a secure SSL context connection with server and send email
context = ssl.create_default_context()

with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, em_password)
    server.sendmail(sender_email, receiver_email, message.as_string())