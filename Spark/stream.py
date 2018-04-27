from filtertweet import filterWords
import json 
import tweepy
import socket
import json


ACCESS_TOKEN = '4504794703-tArLr84qaaUCPmmKRKqMAd7ntQy07AU2Uf1Ww8G'
ACCESS_SECRET = 'Eqq19A7ve6lmFwMnmwZHpH6fwH4d1qTs4Gatljg4xIYAb'
CONSUMER_KEY = 'L65nVAdmsgZGBMVX3nVINUtJs'
CONSUMER_SECRET = '04v1sdTKEy4FrmaPP7kFiuYkWxzHrxKQjdppah6euI5m5RTM62'

#GOOGLE API KEY  AIzaSyAe2rB_I7v0mZEn6tTNlUodvZYkZGIy2As 
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


TCP_IP = 'localhost'
TCP_PORT = 8038


# create socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()


"""class MyStreamListener(tweepy.StreamListener):
    def on_data(self,data):
        jsonObj = json.loads(data)
        print jsonObj['text']
        tweet = filterWords(jsonObj['text'])
        #conn.send(status.text.encode('utf-8'))
        #conn.send(tweet.encode('utf-8'))
        conn.send(data)
        
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
print "Started tweepy"
myStream.filter(track=[hashtag],async=True)
"""
exit_flag = ''
flag_status = ['y','yes','no','n']
class MyStreamListener(tweepy.StreamListener):
    def on_data(self,data):
        jsonObj = json.loads(data)
        if exit_flag.lower() in flag_status:
            return False
        #tweet = filterWords(jsonObj['text'])
        #conn.send(status.text.encode('utf-8'))
         #conn.send(tweet.encode('utf-8'))
        conn.send(data)
    	
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
             print(status_code)

try:
    while exit_flag != 'n':
        
        myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
        key_input = raw_input("Enter hashtag: ")
        exit_flag = ''
        myStream.filter(track=[key_input],async=True)
        exit_flag = raw_input("Do you want to enter another hashtag?(y/n)")

        
except(KeyboardInterrupt,SystemExit):
        print "Keyboard Interrupt..closing connection! "
finally:
    "Closing connection clean"
    s.close()

    


