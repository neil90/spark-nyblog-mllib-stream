import math
import string
from nltk.corpus import stopwords
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from nltk import stem

def question_headline( headline):
	keywords = [
		'?', 'should', 'can', 'if', 
		'is', 'would', 'why', 'how', 
		'when', 'where'
		]

	if any(word in headline for word in keywords):
		return 1
	else:
		return 0

def religion_headline( headline):
	keywords = [
		"secular","humanist","humanism","god",
		"religion", "atheis", "islam", "church",
		"jesus", "christ", "catholic", "pope",
		"imam", "isis", "muslim","gay", "marriage",
		"israel","jew","extremist", "fundamentalism",
		"terror", "hindu"
		]

	if any(word in headline for word in keywords):
		return 1
	else:
		return 0

def word_log( wordcount):
	return math.log1p(float(wordcount))

def political_headline( headline):
	keywords = [
		'obama', 'clinton', 'trump', 'election', 
		'president', 'senate', 'supreme court'
		]

	if any(word in headline for word in keywords):
		return 1
	else:
		return 0   

def socialmedia_headline( headline):
	keywords = [
		'facebook', 'instagram', 'snapchat', 
		'twitter', 'tweet', 'hashtag'
		]

	if any(word in headline for word in keywords):
		return 1
	else:
		return 0 

def dayofweek( date):
	return date.day

def preprocess_headline(headline):
	snowball = stem.snowball.EnglishStemmer()
	headline = headline.lower()

	headline = "".join(l for l in headline if l not in string.punctuation)
	headline = ' '.join([word for word in headline.split() if word not in stopwords.words("english")])
	headline = ' '.join([snowball.stem(word) for word in headline.split()])

	return headline


def registerFunctions(sqlContext):
	sqlContext.udf.register("question_headline", question_headline, IntegerType())
	sqlContext.udf.register("religion_headline", religion_headline, IntegerType())
	sqlContext.udf.register("word_log", word_log, FloatType())
	sqlContext.udf.register("political_headline", political_headline, IntegerType())
	sqlContext.udf.register("socialmedia_headline", socialmedia_headline, IntegerType())
	sqlContext.udf.register("dayofweek", dayofweek, IntegerType())
	sqlContext.udf.register("preprocess_headline", preprocess_headline, StringType())


def featurecreation(df, tblname, sqlContext):


	df.registerTempTable(tblname)

	if 'popular' in df.columns:

		return sqlContext.sql("""SELECT
									uniqueid,
									preprocess_headline(headline) as process_headline,
									CAST(popular as double) as label,
									CASE WHEN newsdesk='' THEN 'NA' ELSE newsdesk END newsdesk,
									CASE WHEN sectionname='' THEN 'NA' ELSE sectionname END sectionname,
									CASE WHEN subsectionname='' THEN 'NA' ELSE subsectionname END subsectionname,
									question_headline(headline) as question,
									religion_headline(headline) as religion,
									political_headline(headline) as political,
									socialmedia_headline(headline) as socialmedia,
									word_log(wordcount) as wordcount_norm,
									dayofweek(pubdate) as dowpub,
									hour(pubdate) as hourpub
								FROM {0}""".format(tblname))
	else:
	
		return sqlContext.sql("""SELECT
									preprocess_headline(headline) as process_headline,
									CAST(uniqueid as int) as uniqueid,
									CAST(wordcount as int) as wordcount,
									headline,
									snippet,
									abstract,
									CAST(pubdate as timestamp) as pubdate,
									CASE WHEN newsdesk='' THEN 'NA' ELSE newsdesk END newsdesk,
									CASE WHEN sectionname='' THEN 'NA' ELSE sectionname END sectionname,
									CASE WHEN subsectionname='' THEN 'NA' ELSE subsectionname END subsectionname,
									question_headline(headline) as question,
									religion_headline(headline) as religion,
									political_headline(headline) as political,
									socialmedia_headline(headline) as socialmedia,
									word_log(wordcount) as wordcount_norm,
									dayofweek(CAST (pubdate as timestamp)) as dowpub,
									hour(pubdate) as hourpub
								FROM {0} """.format(tblname))























