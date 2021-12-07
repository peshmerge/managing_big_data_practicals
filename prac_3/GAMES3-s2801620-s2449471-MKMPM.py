"""
written by 
Manish Mishra s2801620
Peshmerge Morad s2449471

Our time is :
real    0m16.574s
user    0m51.801s
sys     0m3.139s

This computes the most also-bought games from amazon database in /data/doina/UCSD-Amazon-Data/meta_Video_Games.json.gz.
This program is written in Python3

To execute on a machine:
    time spark-submit --conf "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" GAMES3-s2801620-s2449471-MKMPM.py 2> /dev/null

Turns out this is the PlayStation 2 slim console! :D ## https://www.amazon.com/PlayStation-2-Console-Slim-PS2/dp/B000TLU67W
at the price of US$999 and the year being 2021, it is for sure not on sale :P
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# fetch spark context and set log level to ERROR only
sc = SparkContext(appName="GAMES-MKMPM")
sc.setLogLevel("ERROR")

# read all input files
gamesdata_loc = '/data/doina/UCSD-Amazon-Data/meta_Video_Games.json.gz'
spark = SparkSession.builder.getOrCreate()
df = spark.read.json(gamesdata_loc)

# gather all 'also_bought' products asin
also_bought = df.select(func.explode(df.related.also_bought).alias('asin'))

# count also_bought product asin -> order in descending ordedr to get the most also_bought -> take the first row's asin value for the top also_bought product asin
top_also_bought = also_bought.groupBy('asin').count().orderBy('count', ascending=False).take(1)[0]['asin']

# get the top also_bought product record from the dataframe
top_record = df.filter(df.asin == top_also_bought).collect()[0]

# # pretty print the product record with useful information only
# print('Below are the details for the top also_bought product:')
# print("Title:\t" + str(top_record['title']))
# print("Brand:\t" + str(top_record['brand']))
# print("ASIN:\t" + str(top_record['asin']))
# print("Description:\t" + str(top_record['description']))
# print("Categories:\t" + ', '.join(top_record['categories'][0]))
# print("Price:\t" + str(top_record['price']))
# print("Top sales rank:\t" + ', '.join(x[0]+" - "+str(x[1]) for x in top_record['salesRank'].asDict().items() if x[1] != None))

# printing everything in the record
for key, value in top_record.asDict().items():
    print(str(key).encode('UTF-8') + ': ' + str(value).encode('UTF-8'))

# # another way of pretty printing everything in the record using pprint library
# import pprint
# pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(top_record.asDict())
