"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

NOTE: due to time limitations on our side, we could not generate plots for this assignment. There is another file 'BENCH3-s2801620-s2449471-DRIVERFILE.sh' which is a wrapper bash file to run this python on 3 different datasets

This computes the hashtag counts for the tweets in the database at /data/doina/Twitter-Archive.org/2020-01/.

This program is written in Python3

To execute on a machine:
time spark-submit --deploy-mode cluster \
--master yarn \
--executor-memory 6G \
--conf "spark.pyspark.python=/usr/bin/python3.6" \
--conf "spark.pyspark.driver.python=/usr/bin/python3.6" \
--conf "spark.dynamicAllocation.maxExecutors=16" \
BENCH3-s2801620-s2449471-MKMPM.py ${INPUT_REGEX} ${OUTPUT_DIR} 2> /dev/null
"""
import sys
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

executors = sys.argv[3]
# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="BENCH-HASHTAGS-" + executors)
sc.setLogLevel("ERROR")

# read all input files
tweets_loc = sys.argv[1]
spark = SparkSession.builder.getOrCreate()
df = spark.read.json(tweets_loc)

# collect all text from the 'entities.hashtags' column using explode() and put new column alias as 'hashtags'
hashtags = df.select(func.explode(df.entities.hashtags.text).alias('hashtags'))

# count hashtag groups and show the count
hashtags = hashtags.groupBy('hashtags').count().orderBy('count', ascending=False)

# always creates a new folder to write in (clean up your old folders though)
now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
output_dir = '' + sys.argv[2] + '_' + executors + '_' + str(now)
print('saving output to ' + output_dir)
hashtags.coalesce(10).write.format('json').save(output_dir)