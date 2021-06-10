# from _typeshed import NoneType
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

##Twitter credentials
consumer_key    = 'n4XUxW5VFjygbfn11b0qlQtPN'
consumer_secret = '9K8NhQZU57DlttBk7OpfALtmFzI8LDSCZJKShYwvVUzXaNuxnR'
access_token    = '1394047431281623042-QN1mFN7N88IKIt5OOCpuaGTfLBAIUP'
access_secret   = 'NL0pZvd3Gy7BVZbIAL11qcmCxTtJ2XX2Yafnpro73CGud'


# create_query = """CREATE table IF NOT EXISTS dudes
# task_id INT AUTO_INCREMENT PRIMARY KEY, 
# tweet_id INT,
# user VARCHAR(255),
# user_id VARCHAR(255), 
# user_desc TEXT,
# created_at_str VARCHAR(255),
# date DATE, 
# text TEXT,
# hashtags TEXT,
# lang VARCHAR(255),
# profilepic TEXT,
# location VARCHAR(255),
# retweet_count INT,
# favorite_count INT,
# joined_in VARCHAR(255),
# is_Verfied VARCHAR(255),
# followers_count INT,
# geo INT,
# source VARCHAR(255)"""


create_query = """CREATE table IF NOT EXISTS tests (
task_id INT AUTO_INCREMENT PRIMARY KEY, 
tweet_id INT,
username VARCHAR(255) NOT NULL,
user_id VARCHAR(255) NOT NULL,
user_desc TEXT,
created_at_str VARCHAR(255) NOT NULL,
date DATE, 
text TEXT,
hashtags TEXT,
lang VARCHAR(255) NOT NULL,
profilepic TEXT,
location VARCHAR(255) NOT NULL,
retweet_count INT,
favorite_count INT,
joined_in VARCHAR(255) NOT NULL,
is_Verfied VARCHAR(255) NOT NULL,
followers_count INT,
geo INT,
source VARCHAR(255) NOT NULL)"""



# insert_query ="""INSERT INTO dudes (tweet_id, user, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count,geo,source) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(tweet_id, user, user_id, user_desc, created_at_str, date, text, hashtags,
# lang, profilepic, location, retweet_count, favorite_count,geo,source)


def create_table(create_query):
    cnx = mysql.connector.connect(user='pyspark', password='password',
                              host='localhost',
                              database='twitterdb', auth_plugin='mysql_native_password')
    cursor = cnx.cursor()
    #q-uery = """CREATE table IF NOT EXISTS test (task_id INT AUTO_INCREMENT PRIMARY KEY,username VARCHAR(255) NOT NULL,created_at DATE,retweet_count INT NOT NULL,tweet TEXT)"""
    cursor.execute(create_query)
    cursor.close()
    cnx.close()
    return

# create_table(create_query)


def process_json(raw_data):
    tweet_id =  raw_data['id'] 
    user =  raw_data['user']['screen_name'] 
    user_id =  raw_data['user']['id']
     
    user_desc = raw_data['user']['description']
    if str(type(user_desc)) == 'NoneType':
        user_desc = 'NA'
    else:
        user_desc = unidecode(user_desc)

    created_at_str = raw_data['created_at'] 
    date = parser.parse(raw_data['created_at']) 
    text = unidecode(raw_data['text']) 
    hashtags = [hashtag['text'] for hashtag in raw_data['entities']['hashtags']]


    lang =  raw_data['user']['lang'] 
    profilepic =  raw_data['user']['profile_image_url'] 
    location =  raw_data['user']['location'] 
    retweet_count =  raw_data['retweet_count'] 
    favorite_count =  raw_data['favorite_count'] 
    is_Verfied =  raw_data['user']['verified'] 
    followers_count =  raw_data['user']['followers_count'] 
    joined_in =  raw_data['user']['created_at'] 
    geo =  raw_data['geo'] 
    source =  raw_data['source']
    print('process json called')
    #connect(tweet_id, user, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count, joined_in, is_Verfied, followers_count, geo, source)
    return tweet_id, user, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count,favorite_count, joined_in,is_Verfied,followers_count,geo,source
    #return 


# tweet_id= 1
# username='asd'
# user_id='asda'
# user_desc='asdsad'
# created_at_str='asda'
# date='1994-01-01'
# text='asdas'
# hashtags='asdsa' 
# lang='asdasd'
# profilepic='asdasd' 
# location='asdasd'
# retweet_count=10
# favorite_count=99 
# joined_in='asd'
# is_Verfied='asd'
# followers_count=2
# geo=1
# source='asd'


def connected(tweet_id, username, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count, joined_in, is_Verfied, followers_count, geo, source):	
    con = mysql.connector.connect(host = 'localhost', database='twitterdb', user='pyspark', password = 'password',auth_plugin='mysql_native_password', charset = 'utf8')    
    cursor = con.cursor()

    insert_stmt = (
   "INSERT INTO tests ( tweet_id, username, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count, joined_in, is_Verfied, followers_count, geo, source)"
   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" 
    )
    print('connected function called')
    print(insert_stmt)

    data = (tweet_id, username, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count, joined_in, is_Verfied, followers_count, geo, source)
    print(data)

    #try:
    cursor.executemany(insert_stmt, data)
    con.commit()
    print('data inserted')
        #pass
    # except:
    #     con.rollback()
    #     #pass
    #     print("NOT inserted")

# Closing the connection
    con.close()
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

                tweet_id, username, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count, joined_in, is_Verfied, followers_count, geo, source = process_json(raw_data)                
                #insert data just collected into MySQL database
                #connect(username, created_at, tweet, retweet_count)
                connected(tweet_id, username, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count, joined_in, is_Verfied, followers_count, geo, source)
                print("Tweet colleted at: {} ".format(str(created_at_str)))
        except Error as e:
            print(e)
    
        return


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



#connected(tweet_id, username, user_id, user_desc, created_at_str, date, text, hashtags, lang, profilepic, location, retweet_count, favorite_count, joined_in, is_Verfied, followers_count, geo, source)