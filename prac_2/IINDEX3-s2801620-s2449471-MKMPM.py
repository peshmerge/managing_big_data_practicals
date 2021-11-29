"""
This computes the inverted index for a document base in  /data/doina/Gutenberg-EBooks.
This program is written in Python3

To execute on a machine:
    time spark-submit word_count.py --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" 2> /dev/null
"""

from pyspark import SparkContext

sc = SparkContext(appName="IINDEX")
sc.setLogLevel("ERROR")

# uncomment this if you want to run it on the server
#rdd = sc.wholeTextFiles("/data/doina/Gutenberg-EBooks")

# This is just a test to test stuff locally on your pc
rdd = sc.parallelize(
    [
        ("doc1", "scala pesho "),
        ("doc2", "java pesho "),
        ("doc3", "hadoop can make a lot of stuff "),
        ("doc4", "spark why are you going there Aleppo"),
        ("doc5", "akka aleppo Syria hadoop aleppo again syria where is Efrin "),
        ("doc6", "spark vs hadoop vs python vs nothing oder niks oder alles"),
        ("doc7", "pyspark efrin is een mooie stad wel aleppo is ook mooie en gave stad "),
        ("doc8", "pyspark and spark")
    ]
)

word_iindex = rdd.flatMap(lambda text_file: [(text_file[0], word) for word in text_file[1].lower(
).split()]).map(lambda text_file: (text_file[1], [text_file[0]])).reduceByKey(lambda a, b: a+b)

# change 12 to 2 to test locally, because we have not too much data 
filtered_words_iindex = word_iindex.filter(
    lambda iindex_item: len(iindex_item[1]) > 12)

for (word, documents_list) in filtered_words_iindex.collect():
    print(word, " ")
