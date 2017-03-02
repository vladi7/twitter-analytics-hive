#reference http://adilmoujahid.com/posts/2014/07/twitter-analytics/
#          http://www.laurentluce.com/posts/twitter-sentiment-analysis-using-python-and-nltk/
import json
import pandas
from matplotlib import pyplot
import nltk
import re
from nltk.corpus import twitter_samples
from nltk.corpus import stopwords
import sys
def tweet_classifier(answer):
	if answer is 'negative':
		return False
	else:
		return True
	
def createDictionary(words):
	myDict = dict([("", False)])
	if words != None:
		words = [word for word in words if word not in stopwords.words("english")]
		mDict = dict([(word, True) for word in words])
		return mDict
	return myDict

def processTweet(tweet):
    tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))','URL',tweet)
    tweet = re.sub('[\s]+', ' ', tweet)
    tweet = re.sub('@[^\s]+','',tweet)    
    tweet = tweet.strip('\'"')
    tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
    return tweet

def wordFinder(word, text):
    if text == None:
    	return 0
    match = re.search(word.lower(), text.lower())
    if match:
        return 1
    return 0

def main():
	
	negativeInput = twitter_samples.strings('negative_tweets.json')
	negativeTweets = []
	for string in negativeInput:
		{
		negativeTweets.append((createDictionary(string.replace(":", "").replace(")", "").replace("(", "").split()), "negative"))
		}	
	positiveInput = twitter_samples.strings('positive_tweets.json')
	positiveTweets = []
	for string in positiveInput:
		{
		positiveTweets.append((createDictionary(string.replace(":", "").replace(")", "").replace("(", "").split()), "positive"))
		}	
		trainer = negativeTweets + positiveTweets
	#trainSet = negativeTweets[1000:] + positiveTweets[1000:]
	tester =  negativeTweets[:1000] + positiveTweets[:1000] 
	classifier = nltk.NaiveBayesClassifier.train(trainer)
	accuracy = nltk.classify.util.accuracy(classifier, tester)
	print(accuracy * 100)
	print 'classified\n'

	data = []
	posData = []
	negData= []
	file = open('data1.json', "r")
	for string in file:
	    try:
	    	tweet = json.loads(processTweet(string))
	        data.append(tweet)
	    except:
		    continue
	tweets = pandas.DataFrame()
	tweets['text'] = map(lambda tweet: tweet.get('text', None), data)
	print 'data loaded\n'

	#Setting up data frames
	tweets['mercedes'] = tweets['text'].apply(lambda tweet: wordFinder('mercedes', tweet))
	tweets['bmw'] = tweets['text'].apply(lambda tweet: wordFinder('bmw', tweet))
	tweets['audi'] = tweets['text'].apply(lambda tweet: wordFinder('audi', tweet))
	tweets['sentiment'] = tweets['text'].apply(lambda tweet: tweet_classifier(classifier.classify(createDictionary(tweet))))

	print '3\n'
	manufactures = ['MercedesPos','MercedesNeg', 'BmwPos', 'BmwNeg','AudiPos', 'AudiNeg']
	tweetsWithManufacturers = [tweets[tweets['sentiment'] == True]['mercedes'].value_counts()[1],
						  tweets[tweets['sentiment'] == False]['mercedes'].value_counts()[1], 
						  tweets[tweets['sentiment'] == True]['bmw'].value_counts()[1],
						  tweets[tweets['sentiment'] == False]['bmw'].value_counts()[1],
						  tweets[tweets['sentiment'] == True]['audi'].value_counts()[1],  
						  tweets[tweets['sentiment'] == False]['audi'].value_counts()[1]]
	templist = list(range(len(manufactures)))
	x = templist
	width = 0.8
	figure, graph = pyplot.subplots()
	graph.tick_params(axis='x', labelsize=10)
	graph.tick_params(axis='y', labelsize=15)
	graph.set_title('Ranking by the manufacturer', fontsize=10, fontweight='heavy')
	pyplot.bar(x, tweetsWithManufacturers, width, alpha=1, color='k')
	graph.set_xticks([position + 0.5 * width for position in x]) #put names of the manufacturer in the middle
	graph.set_xticklabels(manufactures)
	graph.set_ylabel('# of tweets', fontsize=12)
	pyplot.savefig('tweetByManufacturer4', format='png')

if __name__=='__main__':
	main()





