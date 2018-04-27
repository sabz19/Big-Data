from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
#nltk.download('vader_lexicon')


def analyzeSentiment(string):

	sid = SentimentIntensityAnalyzer()
	sentimentMap = sid.polarity_scores(string)
	del sentimentMap['compound']
	sentiment = max(sentimentMap,key = sentimentMap.get)
	wordMap = {'pos':'positive','neg':'negative','neu':'neutral'}
	return wordMap[sentiment]
	#print sorted_values[0],ss[0]


if __name__ == '__main__':
	
	string = "Hello I am really really happy !"
	analyzeSentiment(string)