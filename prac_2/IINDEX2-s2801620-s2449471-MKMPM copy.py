"""
This computes the inverted index for a document base in  /data/doina/Gutenberg-EBooks.
This program is written in Python2

To execute on a machine:
    time spark-submit IINDEX-s2801620-s2449471-MKMPM.py 2> /dev/null
"""

from pyspark import SparkContext

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="IINDEX-MKMPM")
sc.setLogLevel("ERROR")

# read all input files
rdd = sc.wholeTextFiles("/data/doina/Gutenberg-EBooks")

# using flatMapValues to map file, contents of RDD to file, set(unique words)
words_iindex = rdd.flatMapValues(lambda contents: set(contents.lower().split()))
# swapping (file, word) to (word, set(file)) because of inverse index
words_iindex =  words_iindex.map(lambda (file, word): (word,{file}))
# reducing by key to get desired result
words_iindex =  words_iindex.reduceByKey(lambda a,b:a|b)
# filter and process all the above steps
words_iindex_collected = words_iindex.filter(lambda item: len(item[1]) > 12).collect()

# print the words which occur at least 13 docs
for (word, docs) in words_iindex_collected:
    print word,