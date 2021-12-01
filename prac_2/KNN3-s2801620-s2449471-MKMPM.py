"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :



This computes KNN for any given point based on the data in the csv file located in /data/doina/xyvalue.csv.
This program is written in Python3

To execute on a machine:
    time spark-submit --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" KNN3-s2801620-s2449471-MKMPM.py 2> /dev/null
"""

from pyspark import SparkContext

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="KNN-MKMPM")
sc.setLogLevel("ERROR")
rdd = sc.textFile("/data/doina/xyvalue.csv")

def calc_dist(p1,p2):
    return ((p1[0]-p2[0])**2+(p1[1]-p2[1])**2)**(1/2)
input = (50, 50)
k = 100

values = rdd \
    .map(lambda line: line.split(",")) \
    .map(lambda line: [float(point) for point in line]) \
    .map(lambda point: (point[0], point[1], point[2], round(calc_dist(input, (point[0], point[1]))),4))\
    .map(lambda point: (point[3], point[2])).sortByKey()
value = sum(value[1] for value in values.take(k)) /k
print(value)
