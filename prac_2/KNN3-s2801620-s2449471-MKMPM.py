"""
Written by
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :
real    0m11.944s
user    0m16.043s
sys     0m1.549s


This computes KNN for any given point based on the data in the csv file located in /data/doina/xyvalue.csv.
This program is written in Python3

To execute on a machine:
    time spark-submit --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" KNN3-s2801620-s2449471-MKMPM.py 2> /dev/null
"""
from pyspark import SparkContext

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="KNN-MKMPM")
sc.setLogLevel("ERROR")
# Load the data from the csv file
rdd = sc.textFile("xyvalue.csv")

# Calculate the euclidean distance between two points.
def calc_dist(point1, point2):
    return ((point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2) ** (1 / 2)
# Our input (hardcoded)
input_point = (208.0,292.3)
# The amount of K nearest neighbours for our KNN algorithm
k = 100

# converting all points in our data set to float to for arithmetic operations
values = rdd.map(lambda line: [float(point) for point in line.split(",")])
# calculating the Euclidean distance between each data point in the data set and the given input_point
# we create new RDD of tuples (the value, the euclidean distance) and return its elements (collect)
values = values.map(lambda point: (point[2], calc_dist(input_point, [point[0], point[1]]))).collect()
# Sort the returned element from the previous step based on the Euclidean distances
values = sorted(values, key=lambda point: point[1])
# Get the first K nearest neighbours.
values = values[:k]
# If the distance is zero (the given point is already in the dataset)
if values[0][1] == 0.:
    # Return the value from the data set
    print(values[0][0])
else:
    # Calculate the Inverse Distance Weight for the first K nearest neighbours (the predicted value).
    print(sum(val[0]/val[1] for val in values)/sum(1/val[1] for val in values))