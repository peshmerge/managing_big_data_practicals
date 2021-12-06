"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :
real    0m15.776s
user    1m28.079s
sys     0m3.443s

This computes the top 20 hashtags for first 10 min from tweet database in /data/doina/Twitter-Archive.org/2020-01/01/00/.
This program is written in Python3

To execute on a machine:
    time spark-submit --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" HASHTAGS3-s2801620-s2449471-MKMPM.py 2> /dev/null
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="HASHTAGS-MKMPM")
sc.setLogLevel("ERROR")

# read all input files
tweets_loc = '/data/doina/Twitter-Archive.org/2020-01/01/00/0*.json.bz2'
spark = SparkSession.builder.getOrCreate()
df = spark.read.json(tweets_loc)

hashtags = df.select(func.explode(df.entities.hashtags.text).alias('hashtags'))
hashtags.groupBy('hashtags').count().orderBy('count', ascending=False).show()