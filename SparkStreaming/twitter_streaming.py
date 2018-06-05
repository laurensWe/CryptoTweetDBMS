from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
import pymongo
from tweepy import OAuthHandler
from tweepy import Stream
import time
import json


client = pymongo.MongoClient("localhost", 27017)
db = client.test_alex
collection=db['tweets']
#I removed the consumer_key, consumer_secret,access_token,access_token_secret for privacy

consumer_key = 'm64Yi6QNBISiadboKvNgENwTR'
consumer_secret='H76e4EObnhPNKDw0ysXG5jkdrZmmqDtfCPWMgCNOqFz2x8MxJl'

access_token='233231082-kTrRMd4Om5omNWrjt8Mbl1cwBuDifjkb2OH6mItC'
access_secret='fC5NmSkbAUtRf7JemyRxATTugf4yUgDl1LehxTXlRgTTM'
keyword_list=['bitcoin, Bitcoin, BITCOIN, BTC, Ethereum, ethereum, ETHEREUM, ETH, Ripple, ripple, RIPPLE, XRP, BitcoinCash, bitcoincash, BITCOINCASH, BCH, Eos, eos, EOS, Litecoin, litecoin, LITECOIN, LTC, Cardano, cardano, CARDANO, ADA, Stellar, STELLAR, stellar, XLM, IOTA, iota, Iota, MIOTA, Tron, tron, TRON, TRX']

class StdOutListener(StreamListener):

    def __init__(self, time_limit=100):
        self.start_time = time.time()
        self.limit = time_limit
        self.saveFile = open('twitterData.json', 'a')
        super(StdOutListener, self).__init__()
		
		
    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            self.saveFile.write(data)
            self.saveFile.write('\n')
            dat = json.loads(data)
            query={
                'created_at':dat['created_at'],
                'id':dat['id_str'],
                'text':dat['text'],
                'user':dat['user']
            }
            collection.insert(query)
            return True
        else:
            self.saveFile.close()
            return False

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    stream = Stream(auth, l)
    stream.filter(track=keyword_list)


