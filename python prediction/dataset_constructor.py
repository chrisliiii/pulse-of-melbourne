# ******************************************************************
# 
#   FILE:			dataset_constructor.py
#   DATE:			Semester 1 2017
#   AUTHOR:			Geoff Cunliffe
#   STUDENT ID:		gcunliffe
#   STUDENT NUMBER:	741315
#   DESCRIPTION:		Builds a dataset of manually identified locations
#   REQUIREMENTS: 	json ntlk shapely
#
# ******************************************************************

from nltk.corpus import wordnet as wn
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from shapely.geometry import shape, Point
import re, json

regex = re.compile('[^a-zA-Z ]')
stops = [word.encode('ascii') for word in stopwords.words("english")]
lemmatizer = WordNetLemmatizer()

food = wn.synset('food.n.01')
sport = wn.synset('sport.n.01')
park = wn.synset('park.n.01')

mcg = Point(-37.820018, 144.983460).buffer(0.001878)
rod_laver = Point(-37.821419, 144.979023).buffer(0.001878)
ami = Point(-37.825045, 144.983797).buffer(0.001878)
etihad = Point(-37.816477, 144.947622).buffer(0.001878)
stadiums = [mcg,rod_laver,ami,etihad]

vue = Point(-37.818486, 144.957499).buffer(0.000346)
matcha = Point(-37.866772, 144.978368).buffer(0.000171)
centre = Point(-37.816440, 144.965447).buffer(0.000355)
tattersalls = Point(-37.811890, 144.965556).buffer(0.000355)
lygon = Point(-37.783531, 144.969853).buffer(0.001908)
acland = Point(-37.868812, 144.979945).buffer(0.001448)
chapel = Point(-37.853375, 144.993109).buffer(0.002287)
restaurants = [vue,matcha,centre,tattersalls,lygon,acland,chapel]

botanical = Point(-37.829178, 144.976798).buffer(0.005281)
royal = Point(-37.790897, 144.953848).buffer(0.003700)
carlton = Point(-37.806332, 144.971204).buffer(0.001238)
flagstaff = Point(-37.810484, 144.954433).buffer(0.001489)
parks = [botanical,royal,carlton,flagstaff]

def get_ranks(js,place):
	if place == None:
		return
	date = js.get('date')
	text = js.get('text')
	text = regex.sub('', text)
	text = re.sub( '\s+', ' ', text ).strip()
	filtered_text = [word for word in text.split(' ') if word not in stops]
	lemmatized_text = [lemmatizer.lemmatize(word) for word in [word.encode('ascii') for word in filtered_text]]
	if len(text) > 0: 
		ranks = [0.0,0.0,0.0]
		for word in lemmatized_text:
			try:
				js.get('twitter')
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
			except Exception as e:
				pass
		
		ranks = [i * 10 for i in ranks]
		ranks = [int(i) for i in ranks]
		# ranks = ranks + get_time(date) + [place]
		ranks = ranks + [place]

		if any(i != 0 for i in ranks[:3]):
			return (',').join(map(str,ranks)) + '\n'

def get_day(date):
	days = [0,0,0,0,0,0]
	if date[6] in [6,7]:
		days = [1,0,0,0,0,0]
		return days
	else:
		days[date[6]] = 1
		return days

def get_time(date):
	hour = date[3]
	if hour in range(6,11): # 6am - 11am
		return [1,0,0,0]
	if hour in range(11,14): # 11am - 2pm
		return [0,1,0,0]
	if hour in range(14,18): # 2pm - 6pm
		return [0,0,1,0]
	if hour in range(18,23): # 6pm - 11pm
		return [0,0,0,1]

def check_zone(point):
	for shape in stadiums:
		if shape.contains(point):
			return "stadium"
	for shape in parks:
		if shape.contains(point):
			return "park"
	for shape in restaurants:
		if shape.contains(point):
			return "restaurant"

with open('dump.json') as data_file:  
	with open('sci_train.txt','w') as train:
		# train.write("@RELATION venue\n\n@ATTRIBUTE food NUMERIC\n@ATTRIBUTE sport NUMERIC\n@ATTRIBUTE park NUMERIC\n@ATTRIBUTE place {restaurant,stadium,park}\n\n@DATA\n")
		for line in data_file:
			try:
				js = json.loads(line)
				point = Point(*tuple(js.get('coordinates')))
				train.write(get_ranks(js,check_zone(point)))
			except Exception as e:
				pass

