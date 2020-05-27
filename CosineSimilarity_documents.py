from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.sql import SparkSession

import os
import re

#creation of tokenize function that splits the text/word and make the all lower case
def tokenize(s):
  return re.split("\\W+", s.lower())

#creation of a remove of stopwords function
def remove_stopwords(wordlist, stopwords):
    return [w for w in wordlist if w not in stopwords]

#we initialize Spark in Python (PySpark)
conf = (SparkConf()
         .setMaster("local[*]")
         .setAppName("Data_Project")
         .set("spark.executor.memory", "5g")
         .set("spark.driver.maxResultSize","5g")
         .set("spark.python.worker.memory", "2g")
       )
sc =SparkContext(conf=conf)
print("Spark initialized")
#creation of a list with elements the names of the folders


#name of the parent folder
parent_folder = "20news-18828/"

parent_folder_list = os.listdir(parent_folder)
print parent_folder_list

print "The total nember of folders is = " + str(len(parent_folder_list)) #20

################################################################################################################

def wholeTextInput(folder1,folder2):
    return parent_folder+folder1+"/,"+ parent_folder+folder2+"/"

results= [];

for m in range(0,len(parent_folder_list)):
    for n in range(m+1,len(parent_folder_list)):
        #Load the collection of documents
        collection_data = sc.wholeTextFiles(wholeTextInput(parent_folder_list[m],parent_folder_list[n]))

        # pulling the collected data into a cluster-wide in-memory cache
        collection_data.cache()
        print ("number of documents = " + str(collection_data.count()))

        #create a RDD with the pair: file name + its tokenized text
        text_tokenized_rdd = collection_data.map(lambda a : (a[0].split("/")[-1], tokenize(a[1])))
        files = collection_data.map(lambda a: a[0]).collect() #we collect the document paths (files)

        #load the collection of stopwords
        stopwords_rdd= sc.wholeTextFiles('stop-words-english/*')

        #tokenized them in the same way as our text
        stopwords_list_rdd = stopwords_rdd.flatMap(lambda a: tokenize(a[1]))
        stopwords_list = (stopwords_list_rdd.distinct().collect())

        #remove stopwords list from the text
        documents = text_tokenized_rdd.map(lambda a: (remove_stopwords(a[1], stopwords_list)))

        # pulling the documents into a cluster-wide in-memory cache
        documents.cache()

        hashingTF = HashingTF()
        tf = hashingTF.transform(documents)
        tf.cache()
        idf = IDF().fit(tf)
        tfidf = idf.transform(tf)      
        final_rdd = tfidf.zipWithIndex().map(lambda s: IndexedRow(s[1],s[0]))
        final_rdd.cache()     

        spark = SparkSession(sc)
        matrix1 = IndexedRowMatrix(final_rdd)
        
        matrix2=matrix1.toCoordinateMatrix().transpose().toIndexedRowMatrix().columnSimilarities()

        joiner = "/"
        def last_2_elem(x):
            seq=(x.split("/")[-2],x.split("/")[-1])
            return seq

        # union of matrices
        matrix3=matrix2.entries.map(lambda m:(m.i,[m.j,m.value]))
        matrix4=matrix2.entries.map(lambda m:(m.j,[m.i,m.value]))
        matrix3 = matrix3.union(matrix4)
        temp = matrix3.reduceByKey(lambda a,b: a if a[1]>b[1] else b)\
            .map(lambda i:(joiner.join(last_2_elem(files[i[0]])), joiner.join(last_2_elem(files[i[1][0]])),i[1][1])).collect()

        #merge of similarities results for all files in one array
#################################################################################################
        print  "the type of results is = " + str(type(results))
        print  "the type of temp is = " + str(type(temp))
        len_temp=len(temp)
        len_results=len(results)
        for i in range(0,len_temp):
            if len_results != 0:
                is_new = True
                for j in range(0,len_results):
                    if results[j][0] == temp[i][0]:
                        if temp[i][2]>results[j][2]:
                            results[j]=temp[i]
                        is_new = False
                        break
                if(is_new):
                    results.append(temp[i])
            else:
                results.append(temp[i])
####################################################################################################

sorted_results = sorted(results,key=lambda x: -x[2])
file_object = open("/home/fustalieri/Python/outputs/allFilesCosineSimilarities.csv", "w")
file_object.write("file_a;file_b;similarity\n") 
for x in sorted_results:
    file_object.write(str(x[0]) + ";" + str(x[1]) + ";" + str(x[2]) + "\n") 
file_object.close()