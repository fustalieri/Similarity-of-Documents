from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
import re

# sc =SparkContext()
def tokenize(s):
    return re.split("\\W+", s.lower())

#creation of a remove of stopwords function
def remove_stopwords(wordlist, stopwords):
    return [w for w in wordlist if w not in stopwords]

root = "/data_spark_bk/"
stopWordsPath = "/stop-words-english/*"
stopwords= sc.wholeTextFiles(stopWordsPath).flatMap(lambda a: tokenize(a[1])).distinct().collect()

folders = os.listdir(root) 
folders
for f in range(0,len(folders)):
    print(f)
    print(root+folders[f])
    data = sc.wholeTextFiles(root+folders[f])
    data.cache()
    documents = data.map(lambda s: tokenize(s[1])).map(lambda s: remove_stopwords(s,stopwords))
    files = data.map(lambda s: s[0]).collect()
    documents.cache()
    hashingTF = HashingTF()
    featurizedData = hashingTF.transform(documents)
    idf = IDF()
    idfModel = idf.fit(featurizedData)
    featurizedData.cache()
    tfidfs = idfModel.transform(featurizedData)
    tfidfs.cache()
    final_rdd = tfidfs.zipWithIndex().map(lambda s: IndexedRow(s[1],s[0]))      
    final_rdd.cache()
    sims =  IndexedRowMatrix(final_rdd).toCoordinateMatrix().transpose().toIndexedRowMatrix().columnSimilarities()
    pairs = sims.entries.map(lambda m:[m.i,m.j,m.value]).collect()
    for p in range(0,len(pairs)):
        pairs.append([pairs[p][1],pairs[p][0],pairs[p][2]])
    results = []
    for p in range(0,len(files)):
        results.append([p,0,0.0])

    for p in range(0,len(pairs)):
        index = pairs[p][0]
        if pairs[p][2] > results[index][2]:
            results[index]=[index,pairs[p][1],pairs[p][2]]
    file_object = open("/home/sofos/out/" + folders[f] + ".csv", "w")
    for i in range(0,len(files)):
        file_object.write(str(results[i][0]) + ";" + str(results[i][1]) + ";" + str(results[i][2]) + ";" + files[results[i][0]].replace(root,"").replace("file:","") + ";" + files[results[i][1]].replace(root,"").replace("file:","") + "\n") 
    file_object.close()
