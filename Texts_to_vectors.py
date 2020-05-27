
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix

import re

#creation of tokenize function that splits the text/word and make the all lower case
def tokenize(s):
  return re.split("\\W+", s.lower())

#creation of a remove of stopwords function
def remove_stopwords(wordlist, stopwords):
    return [w for w in wordlist if w not in stopwords]

#we initialize Spark in Python (PySpark)
sc =SparkContext()
print("Spark initialized")

#Load the collection of documents
collection_data = sc.wholeTextFiles("20news-18828/*")

#for x in collection_data.collect():
#	print x

# pulling the collected data into a cluster-wide in-memory cache
collection_data.cache()
print ("number of documents = " + str(collection_data.count()))

#create a RDD with the pair: file name + its tokenized text
text_tokenized_rdd = collection_data.map(lambda a : (a[0].split("/")[-1], tokenize(a[1])))
files = collection_data.map(lambda a: a[0]).collect() #we collect the document paths (files)

#for x in text_tokenized_rdd.collect():
#	print x

#load the collection of stopwords
stopwords_rdd= sc.wholeTextFiles('stop-words-english/*')

#for x in stopwords_rdd.collect():
#	print x

#tokenized them in the same way as our text
stopwords_list_rdd = stopwords_rdd.flatMap(lambda a: tokenize(a[1]))
stopwords_list = (stopwords_list_rdd.distinct().collect())

#for x in stopwords_list:
#	print x

#remove stopwords list from the text
documents = text_tokenized_rdd.map(lambda a: (remove_stopwords(a[1], stopwords_list)))

# pulling the documents into a cluster-wide in-memory cache
documents.cache()

#for x in documents.collect():
#	print x

hashingTF = HashingTF()
tf = hashingTF.transform(documents)
print("The TF")
#for x in tf.collect():
#	print x

tf.cache()
idf = IDF().fit(tf)
print("The IDF")
#for x in idf.collect():
#	print x

tfidf = idf.transform(tf)
print("The TF-IDF")
#for x in tfidf.collect():
#	print x


final_rdd = tfidf.zipWithIndex().map(lambda s: IndexedRow(s[1],s[0]))   
print("The final_RDD")
for x in final_rdd.collect():
	print x

final_rdd.saveAsTextFile("/home/fustalieri/Python/outputs2/Vectors.txt")

	