"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :
real	0m7.825s
user	0m13.693s
sys	0m1.030s

This computes the inverted index for a document base in  /data/doina/Gutenberg-EBooks.
This program is written in Python3

To execute on a machine:
    time spark-submit --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" IINDEX3-s2801620-s2449471-MKMPM.py 2> /dev/null
"""

from pyspark import SparkContext
import re

sc = SparkContext(appName="IINDEX-MKMPM")
sc.setLogLevel("ERROR")

rdd = sc.wholeTextFiles("/data/doina/Gutenberg-EBooks")

# using flatMapValues to map file, contents of RDD to file, set(unique words)
# convert all letters to lower case and remove punctuations using regex
words_iindex = rdd.flatMapValues(lambda contents: set((re.sub(r'[^\w\s]', '', contents)).lower().split()))
# swapping (file, word) to (word, set(file)) because of inverse index
words_iindex = words_iindex.map(lambda item: (item[1], {item[0]}))
# reducing by key to get desired result
words_iindex = words_iindex.reduceByKey(lambda a, b: a | b)

# filter and process all the above steps
filtered_words_iindex = words_iindex.filter(lambda item: len(item[1]) > 12).collect()

[print(word, end=' ') for (word, _) in filtered_words_iindex]