# ******************************************************************
# 
#   FILE:			predict.py
#   DATE:			Semester 1 2017
#   AUTHOR:			Geoff Cunliffe
#   STUDENT ID:		gcunliffe
#   STUDENT NUMBER:	741315
#   DESCRIPTION:		Library that builds a decision tree model to predict the use of 
#				a location based on textual social media content
#   REQUIREMENTS: 	sklearn numpy scipy ntlk shapely
#
# ******************************************************************

from sklearn import tree
from nltk.corpus import wordnet as wn
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from collections import Counter
import re, json

regex = re.compile('[^a-zA-Z ]')
stops = stopwords.words("english")
lemmatizer = WordNetLemmatizer()

# Set comparison words
food = wn.synset('food.n.01')
sport = wn.synset('sport.n.01')
park = wn.synset('park.n.01')

# Load training dataset and train model
X=[]
Y=[]
with open('package/sci_train.txt') as data_file:
	for line in data_file:
		data = line.split(',')
		X.append(list(map(int,data[:3])))
		Y.append(data[3].strip())
clf = tree.DecisionTreeClassifier()
clf = clf.fit(X, Y)

# Calculates numerical parameters for each tweet
def get_ranks(js):
	text = str(js.get('text'))
	text = regex.sub('', text)
	text = re.sub( '\s+', ' ', text ).strip()
	filtered_text = [word for word in text.split(' ') if word not in stops]
	lemmatized_text = [lemmatizer.lemmatize(word) for word in filtered_text]
	if len(text) > 0: 
		ranks = [0.0,0.0,0.0]
		for word in lemmatized_text:
			try:
				test = wn.synset(word.lower() + '.n.01')
				f = food.path_similarity(test)
				s = sport.path_similarity(test)
				p = park.path_similarity(test)
				if f > 0.145:
					ranks[0] = ranks[0] + f
				if s > 0.145:
					ranks[1] = ranks[1] + s
				if p > 0.145:
					ranks[2] = ranks[2] + p
			except:
				pass
		
		ranks = [i * 10 for i in ranks]
		ranks = [int(i) for i in ranks]
		# only use if any parameters is non-zero
		if any(i != 0 for i in ranks[:3]):
			print (ranks)
			return ranks

# Predicts using new dataset of parameterised tweets
def predict_venue_type(ranks):
	if len(ranks) > 0:
		predictions = clf.predict(ranks)
		ranked = Counter(predictions).most_common()
		return str((ranked[0][0]))
	else:
		return "no data"

# Returns single predicted use of a location based on most common label
def predict_from_tweets(tweets):
	all_ranks=[]
	for tweet in tweets:
		ranks = get_ranks(tweet.get('value'))
		if ranks is not None:
			all_ranks.append(ranks)
	return predict_venue_type(all_ranks)

