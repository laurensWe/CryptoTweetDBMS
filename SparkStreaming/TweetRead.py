
# coding: utf-8

# In[ ]:


import tweepy


# In[ ]:


from tweepy.auth import OAuthHandler


# In[ ]:


from tweepy import Stream


# In[ ]:


from tweepy.streaming import StreamListener


# In[ ]:


import socket


# In[ ]:


import json


# In[ ]:


# Set up your credentials from http://apps.twitter.com
consumer_key = ''
consumer_secret=''

access_token=''
access_secret=''


# In[ ]:


keyword_list=['bitcoin, Bitcoin, BITCOIN, BTC, Ethereum, ethereum, ETHEREUM, ETH, Ripple, ripple, RIPPLE, XRP, BitcoinCash, bitcoincash, BITCOINCASH, BCH, Eos, eos, EOS, Litecoin, litecoin, LITECOIN, LTC, Cardano, cardano, CARDANO, ADA, Stellar, STELLAR, stellar, XLM, IOTA, iota, Iota, MIOTA, Tron, tron, TRON, TRX']


# In[ ]:


class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket
        self.saveFile = open('TweetsCoin.json', 'a')

    def on_data(self, data):
        try:
            msg = json.loads( data )
            print( msg['text'].encode('utf-8') )
            self.client_socket.send( msg['text'].encode('utf-8') )
            self.saveFile.write(data)
            self.saveFile.write('\n')
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


# In[ ]:


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=keyword_list)


# In[ ]:


if __name__ == "__main__":
    s = socket.socket()        
    host = "127.0.0.1"    
    port = 5555                
    s.bind((host, port))      
    print("Listening on port: %s" % str(port))

    s.listen(5)                 
    c, addr = s.accept()       

    print("Received request from: " + str(addr))

    sendData(c)

