from pycorenlp import StanfordCoreNLP


def parseText(tweet):

	nlp = StanfordCoreNLP('http://localhost:9000')
	res = nlp.annotate(tweet,
	properties = {

		'annotators':'sentiment',
		'outputFormat':'json'
	})

	resultString = ''
	for word in res['sentences']:
	#	print "%d: '%s': %s" %(
	#		word['index'],
	#		"".join([t["word"] for t in word['tokens']]),
	#		word['sentiment']
	#)
		resultString += "".join([t['word'] for t in word['tokens']]) + word['sentiment']
	print resultString
	return resultString


if __name__ == '__main__':
	parseText("Hi i am a boy")