"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :

This computes the inverted index for a document base in  /data/doina/Gutenberg-EBooks.
This program is written in Python3

To execute on a machine:
    time spark-submit --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" KNN3-s2801620-s2449471-MKMPM.py 2> /dev/null
"""

from pyspark import SparkContext

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="KNN-MKMPM")
sc.setLogLevel("ERROR")

# read all input files
rdd = sc.textFile("/data/doina/xyvalue.csv")

