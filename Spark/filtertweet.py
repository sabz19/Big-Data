# - *- coding: utf-8 -*-
from emoji import UNICODE_EMOJI


#words = unicode(words,"utf-8")
#words = unicode(words)

def filterWords(tweet):
	count = 0
	stripped_tweet = ""
	for word in tweet:
		if word not in UNICODE_EMOJI:
			stripped_tweet += word
		
	return stripped_tweet
