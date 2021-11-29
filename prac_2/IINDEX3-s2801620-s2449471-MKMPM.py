"""
This computes the inverted index for a document base in  /data/doina/Gutenberg-EBooks.
This program is written in Python3

To execute on a machine:
    time spark-submit IINDEX3-s2801620-s2449471-MKMPM.py --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" 2> /dev/null
"""

from pyspark import SparkContext

sc = SparkContext(appName="IINDEX-MKMPM")
sc.setLogLevel("ERROR")

rdd = sc.wholeTextFiles("/data/doina/Gutenberg-EBooks")

# using flatMapValues to map file, contents of RDD to file, set(unique words)
words_iindex = rdd.flatMapValues(lambda contents: set(contents.lower().split()))
# swapping (file, word) to (word, set(file)) because of inverse index
words_iindex =  words_iindex.map(lambda item: (item[1],{item[0]}))
# reducing by key to get desired result
words_iindex =  words_iindex.reduceByKey(lambda a,b:a|b)

# filter and process all the above steps
filtered_words_iindex = words_iindex.filter(lambda item: len(item[1]) > 12).collect()

for (word, documents_list) in filtered_words_iindex:
    print(word)

print(len(filtered_words_iindex))