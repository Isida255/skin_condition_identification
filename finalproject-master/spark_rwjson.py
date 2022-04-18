from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import requests
import json
from pyspark.sql import functions as F
from urllib.request import Request, urlopen

my_spark = SparkSession.builder.master("local[*]").appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://localhost/newtestdb.mydata") \
    .config("spark.mongodb.output.uri", "mongodb://localhost/newtestdb.mydata") \
    .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

url = "http://127.0.0.1:5000/"

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Python Spark Mongo DB write").getOrCreate()

    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

    # Online data source
    onlineData = 'http://127.0.0.1:5000/'

    # read the online data file
    httpData = urlopen(onlineData).read().decode('utf-8')
    print(httpData)
    # convert into RDD
    rdd = spark.sparkContext.parallelize([httpData])

    # create a Dataframe
    jsonDF = spark.read.json(rdd)
    jsonDF.write.format('com.mongodb.spark.sql.DefaultSource').mode("append").save()
