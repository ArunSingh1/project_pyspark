import twint
import kafka
from kafka import KafkaProducer, KafkaConsumer
import json


producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = "test1"

def get_tweets():
    list1 =[]
    c = twint.Config()
    #c.Search = "spark"
    c.Username = "adithyabalaji_"    
    # Run
    tweets = twint.run.Search(c)
    list1.append(tweets)
    print('list1', list1)
    for line in list1:
        print("reading tweets", line)
        
        full_tweet = json.dumps(line)
        print(full_tweet)
    # producer.send(topic_name, tweets)
    # print(tweets)
    # print(type(tweets))
    return

# def send_tweets_to_kafka(http_resp):
#     print("########################send_tweets_to_kafka called#################################")
#     for line in http_resp.iter_lines():
#         print("reading tweets")
#         try:
#             full_tweet = json.loads(line)
#             tweet_text = full_tweet['text']
#             print("Tweet Text: " + tweet_text)
#             print ("------------------------------------------")
#             tweet_text = tweet_text + '\n'
#             producer.send_messages(topic_name, tweet_text.encode())
#             #producer.send(twitter_topic, tweet_text.encode())
#             time.sleep(0.2)
#         except:
#             print("Error received")
#             e = sys.exc_info()[0]
#             print("Error: %s" % e)
#     print("Done reading tweets")


get_tweets()
# send_tweets_to_kafka(tweets)

# producer = KafkaProducer(bootstrap_servers='localhost:9092')
# topic_name = "test1"

# def gettweets(producer):
#     # Configure
#     c = twint.Config()
#     #c.Search = "spark"
#     c.Username = "adithyabalaji_"    
#     # Run
#     tweets = twint.run.Search(c)
#     # producer.send(topic_name, tweets)
#     print(tweets)
#     #producer.send(topic_name, tweets)

#     producer.send(topic_name, str(tweets).encode() )
#     return 

# gettweets(producer)

# consumer = KafkaConsumer(topic_name,
#                      bootstrap_servers=['localhost:9092'],
#                      group_id=None,
#                      auto_offset_reset='earliest')


# for message in consumer:
#     print("Message", message)
#     if (message is not None):
#         print(message.offset, message.value)

# print("Quit")
print("i have attained the twitter api credentails")