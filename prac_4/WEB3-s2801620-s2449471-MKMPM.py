"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :
real    3m50.355s
user    0m14.584s
sys     0m3.170s

This program loads the web crawl JSON data from /data/doina/WebInsight/2020-07-13/ and /data/doina/WebInsight/2020-09-14/*,
gathers the unique 'url' for each of the crawl and calculates the difference in 'fetch.textSize' and saves only this information
to /user/s2801620/wk4_WEB_out.

This program is written in Python3

To execute on a machine:
    time spark-submit --deploy-mode cluster \
    --master yarn \
    --executor-memory 6G \
    --conf "spark.pyspark.python=/usr/bin/python3.6" \
    --conf "spark.pyspark.driver.python=/usr/bin/python3.6" \
    --conf "spark.dynamicAllocation.maxExecutors=16" \
    WEB3-s2801620-s2449471-MKMPM.py 2> /dev/null
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="WEB-MKMPM")
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# crawl locations
first_crawl_loc = '/data/doina/WebInsight/2020-07-13/*'
last_crawl_loc = '/data/doina/WebInsight/2020-09-14/*'

# reading the data
df1 = spark.read.json(first_crawl_loc)
df2 = spark.read.json(last_crawl_loc)

## on analysing the data, we find the required columns as
# fetch.textSize <- length of crawl
# url <- page URL

# join condition 
cond = [df1.url == df2.url, (df1.fetch.textSize > 0) | (df2.fetch.textSize > 0)]
# joining the 2 loaded dataframes and only picking 'url', and 'textSize' from each of them
dff = df1.join(df2, cond) \
.select(df1.url, df1.fetch.textSize.alias('first_size'), df2.fetch.textSize.alias('last_size'))

# subtracting the textSizes to get the difference and ordering the output
dff = dff.withColumn('sizediff', (dff.last_size - dff.first_size)).orderBy('sizediff')

# we dont need the 'first_size' and 'last_size' columns so selecting only 'url' and 'sizediff'
# and removing pages without any difference (sizediff == 0)
dff = dff.select(dff.url, dff.sizediff) \
.filter(dff.sizediff != 0)

# save the output in 10 coalesced partitioned JSON files.
output_dir = '/user/s2801620/wk4_WEB_out'
dff.coalesce(10).write.format('json').save(output_dir)