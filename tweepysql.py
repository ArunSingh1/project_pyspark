import tweepy 
from tweepy import OAuthHandler # to authenticate Twitter API
from tweepy import Stream 
from tweepy.streaming import StreamListener
import socket 
import json 
import mysql.connector
from mysql.connector import Error
from unidecode import unidecode
from dateutil import parser

## db
# cnx = mysql.connector.connect(user='pyspark', password='password',
#                               host='localhost',
#                               database='twitterdb', auth_plugin='mysql_native_password')
# cursor = cnx.cursor()
# query = """CREATE table IF NOT EXISTS dude (task_id INT AUTO_INCREMENT PRIMARY KEY,username VARCHAR(255) NOT NULL,created_at DATE,retweet_count INT NOT NULL,tweet TEXT)"""
# cursor.execute(query)
# cursor.close()
# cnx.close()


    
consumer_key    = 'n4XUxW5VFjygbfn11b0qlQtPN'
consumer_secret = '9K8NhQZU57DlttBk7OpfALtmFzI8LDSCZJKShYwvVUzXaNuxnR'
access_token    = '1394047431281623042-QN1mFN7N88IKIt5OOCpuaGTfLBAIUP'
access_secret   = 'NL0pZvd3Gy7BVZbIAL11qcmCxTtJ2XX2Yafnpro73CGud'

def connect(username, created_at, tweet, retweet_count):
	"""
	connect to MySQL database and insert twitter data
	"""
	try:
		con = mysql.connector.connect(host = 'localhost',
		database='twitterdb', user='pyspark', password = 'password',auth_plugin='mysql_native_password', charset = 'utf8')
		
		if con.is_connected():
			"""
			Insert twitter data
			"""
			cursor = con.cursor()
			# twitter, golf
			query = "INSERT INTO dude (username, created_at, tweet, retweet_count) VALUES (%s, %s, %s, %s)"
			cursor.execute(query, (username, created_at, tweet, retweet_count))
			con.commit()
            
			
			
	except Error as e:
		print(e)

	#cursor.close()
	#con.close()
	return



# Tweepy class to access Twitter API
class Streamlistener(tweepy.StreamListener):
	

	def on_connect(self):
		print("You are connected to the Twitter API")


	def on_error(self):
		if status_code != 200:
			print("error found")
			# returning false disconnects the stream
			return False

	"""
	This method reads in tweet data as Json
	and extracts the data we want.
	"""
	def on_data(self,data):
		
		try:
			raw_data = json.loads(data)

			if 'text' in raw_data:
				 
				username = raw_data['user']['screen_name']
				created_at = parser.parse(raw_data['created_at'])
				tweet = unidecode(raw_data['text'])
				retweet_count = raw_data['retweet_count']

				#insert data just collected into MySQL database
				connect(username, created_at, tweet, retweet_count)
				print("Tweet colleted at: {} ".format(str(created_at)))
		except Error as e:
			print(e)


if __name__== '__main__':

	# authentification so we can access twitter
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
	api =tweepy.API(auth, wait_on_rate_limit=True)

	# create instance of Streamlistener
	listener = Streamlistener(api = api)
	stream = tweepy.Stream(auth, listener = listener)

	track = ['football']
	#track = ['nba', 'cavs', 'celtics', 'basketball']
	# choose what we want to filter by
	stream.filter(track = track, languages = ['en'])