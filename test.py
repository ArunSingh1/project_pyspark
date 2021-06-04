from twython import Twython



#credentials['CONSUMER_KEY'] 
APP_KEY = "n4XUxW5VFjygbfn11b0qlQtPN"
#credentials['CONSUMER_SECRET'] 
APP_SECRET = "9K8NhQZU57DlttBk7OpfALtmFzI8LDSCZJKShYwvVUzXaNuxnR"
#credentials['ACCESS_TOKEN'] 
OAUTH_TOKEN = "1394047431281623042-QN1mFN7N88IKIt5OOCpuaGTfLBAIUP"
#credentials['ACCESS_SECRET'] 
OAUTH_TOKEN_SECRET= "NL0pZvd3Gy7BVZbIAL11qcmCxTtJ2XX2Yafnpro73CGud"


twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)



# Create our query
query = {'q': 'learn python',
        'result_type': 'popular',
        'count': 10,
        'lang': 'en',
        }


import pandas as pd

# Search tweets
dict_ = {'user': [], 'date': [], 'text': [], 'favorite_count': []}
for status in twitter.search(**query)['statuses']:
    dict_['user'].append(status['user']['screen_name'])
    dict_['date'].append(status['created_at'])
    dict_['text'].append(status['text'])
    dict_['favorite_count'].append(status['favorite_count'])

# Structure data in a pandas DataFrame for easier manipulation
df = pd.DataFrame(dict_)
df.sort_values(by='favorite_count', inplace=True, ascending=False)
df.head(5)