import psycopg2
import smtplib, ssl
from datetime import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
import re
from config import sender_email, em_password, dbname, password


def weekly_email_SQL_calc():
    '''
    Function that calls stored SQL functions that calculate Spotify listen summary statistics
    '''
    # Establish DB Conn
    conn = psycopg2.connect(
        dbname=dbname,
        user='postgres',
        password=password
    )

    curr = conn.cursor()
    
    # Create temp table in backend database
    curr.callproc('create_temp_table')

    # Top Songs
    curr.callproc('past_week_top_5_songs')
    top_5_songs = curr.fetchall()

    # Top Artists
    curr.callproc('past_week_top_5_artists')
    top_5_artists = curr.fetchall()

    # Top Albums
    curr.callproc('top_5_albums')
    top_5_albums = curr.fetchall()

    # Listening Duration
    curr.callproc('get_listen_duration')
    minutes_listened = curr.fetchall()

    # Most Mainstream Songs
    curr.callproc('most_popular_songs')
    popular_songs = curr.fetchall()

    # Least Mainstream Songs
    curr.callproc('most_obscure_songs')
    obscure_songs = curr.fetchall()

    # Drop temp table in backend
    curr.execute('DROP TABLE song_plays_detailed;')
    curr.close()

    return top_5_songs, top_5_artists, top_5_albums, minutes_listened, popular_songs, obscure_songs


top_5_songs, top_5_artists, top_5_albums, minutes_listened, popular_songs, obscure_songs = weekly_email_SQL_calc() 

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
    Here's your weekly Spotify metrics.
    '''

# HTML text for email, unfortunately CSS styling must be done in-line to be Gmail compatible
html = f'''\
    <html>
        <body>
            <div style="background-color: black; color:white;">
                <img style="text-align:center; width:220px; height:75px; margin: 20px auto 20px; display: block;" src="https://storage.googleapis.com/pr-newsroom-wp/1/2018/11/Spotify_Logo_CMYK_Green.png" alt="Spotify Logo">

                <h1 style="color: #1DB954; text-align: center;">Terence's Weekly Spotify Metrics</h1>
                
                <img style="text-align:center; width:250px; height:280px; margin: 10px auto 20px; display: block;" src="https://i.scdn.co/image/ab6775700000ee85bb77683134170c0280a783fa" alt="Terence's Profile Photo">

                <p style="color: white; text-align: center; font-size: 15px;">
                    {dt.today().strftime("%b %d, %Y")} - Here's a summary of your past of week of music listening:<br>

                    <h2 style="text-align: center; color: #1DB954">In the past week you listened to {minutes_listened[0][0]} minutes of music!</h2>
                     <p style="color: white; text-align: center;">
                        _________________________________________________
                    </p>
                </p>

                <p style="color: white; text-align: center;">
                    <h2 style="text-align: center; color: #1DB954">Top Five Songs of the Week:</h2>
                    {re.sub('<table>', '<table align="center">', tabulate(top_5_songs, headers=['Song', 'Artist', 'Play Count'], tablefmt='html'))}
                    <p style="color: white; text-align: center;">
                        _________________________________________________
                    </p>
                </p>

                <p style="color: white; text-align: center;">
                    <h2 style="text-align: center; color: #1DB954">Top Five Artists of the Week:</h2>
                    {re.sub('<table>', '<table align="center">', tabulate(top_5_artists, headers=['Artist', 'Play Count'], tablefmt='html'))}
                    <p style="color: white; text-align: center;">
                        _________________________________________________
                    </p>
                </p>

                <p style="color: white; text-align: center;">
                    <h2 style="text-align: center; color: #1DB954">Top Five Albums of the Week:</h2>
                    {re.sub('<table>', '<table align="center">', tabulate(top_5_albums, headers=['Album', 'Artist', 'Play Count'], tablefmt='html'))}
                    <p style="color: white; text-align: center;">
                        _________________________________________________
                    </p>
                </p>

                <p style="color: white; text-align: center;">
                    <h2 style="text-align: center; color: #1DB954">Most Mainstream Songs:</h2>
                    {re.sub('<table>', '<table align="center">', tabulate(popular_songs, headers=['Song', 'Artist', 'Popularity Rank'], tablefmt='html'))}
                    <p style="color: white; text-align: center;">
                        _________________________________________________
                    </p>
                </p>

                <p style="color: white; text-align: center;">
                    <h2 style="text-align: center; color: #1DB954">Least Mainstream Songs:</h2>
                    {re.sub('<table>', '<table align="center">', tabulate(obscure_songs, headers=['Song', 'Artist', 'Popularity Rank'], tablefmt='html'))}
                    <p style="color: white; text-align: center;">
                        _________________________________________________
                    </p>
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

# Send email
def send_weekly_email():
    '''
    This function will create a context to send the weekly email
    Will be used in our Airflow job's email DAG
    '''
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, em_password)
        server.sendmail(sender_email, receiver_email, message.as_string())

if __name__ == "__main__":
    send_weekly_email()