"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :
real    0m10.088s
user    0m24.185s
sys     0m2.072s

This program sorts a large number of integers from /data/doina/integers.txt and saves the sorted output into 10 different files.
This program is written in Python3

To execute on a machine:
    time spark-submit --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" DSORT3-s2801620-s2449471-MKMPM.py 2> /dev/null

This program will generate the files under /user/s2801620/wk3_DSORT_out/
"""

from pyspark import SparkContext

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="DSORT-MKMPM")
sc.setLogLevel("ERROR")

# read the input file
integers_loc = '/data/doina/integers.txt'
rdd = sc.textFile(integers_loc)

# split the lines into individual integers
rdd = rdd.flatMap(lambda line: [int(point) for point in line.split(',')])
# sort the input and divide into 10 partitions
rdd = rdd.sortBy(lambda x:x, ascending=True, numPartitions=10)

# save the output files into '/user/s2801620/wk3_DSORT_out/'
output_dir = '/user/s2801620/wk3_DSORT_out/'
rdd.saveAsTextFile(output_dir)
print('10 output files in order present in ' + output_dir)