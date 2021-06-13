# from sqlite3.dbapi2 import Cursor
import tweepy 
from tweepy import OAuthHandler # to authenticate Twitter API
from tweepy import Stream 
from tweepy.streaming import StreamListener
import socket 
import json 
# import sqlite3 


# # Set up your credentials
# # consumer_key    = 'pNYC6DkaEzm0M5CyIrgiW08K4'
# # consumer_secret = 'PxpJPBxALtkDmUb8LVcJNkHlb5HXBpqWeqmPJAjYzWIzM165qo'
# # access_token    = '2988835149-CKcWUdG5BeoqA93wA3gBAwEdWeKk3Oru4NqiFM4'
# # access_secret   = 'V7eSQ06uZ8w8byspmLlm2u2iOLZaTY4jYHFxKiqF9074o'


consumer_key    = 'n4XUxW5VFjygbfn11b0qlQtPN'
consumer_secret = '9K8NhQZU57DlttBk7OpfALtmFzI8LDSCZJKShYwvVUzXaNuxnR'
access_token    = '1394047431281623042-QN1mFN7N88IKIt5OOCpuaGTfLBAIUP'
access_secret   = 'NL0pZvd3Gy7BVZbIAL11qcmCxTtJ2XX2Yafnpro73CGud'


# ## db connnection
# conn = sqlite3.connect('twitterdb')
# db =  conn.cursor()


class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print(msg['text'].encode('utf-8'))
          self.client_socket.send( msg['text'].encode('utf-8'))
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True




def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['football'])

if __name__ == "__main__":
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         # Create a socket object
  host = "127.0.0.1"     # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print("Received request from: " + str(addr))

  sendData(c)
