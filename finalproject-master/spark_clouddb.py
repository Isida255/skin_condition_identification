# program to write (insert/upsert) JSON to MongoDB using Spark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import requests
import json
from pyspark.sql import functions as F
from kafka import KafkaProducer
from kafka import KafkaConsumer
from urllib.request import Request, urlopen

#Set variables
mongodburi = "mongodb+srv://admin:testAdmin@cluster0.k5ld4.mongodb.net/client.test"
topic = "userdetails"

my_spark = SparkSession.builder.master("local[*]").appName("myApp") \
    .config("spark.mongodb.input.uri", mongodburi) \
    .config("spark.mongodb.output.uri", mongodburi) \
    .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

spark = SparkSession.builder.appName("Python Spark Mongo DB write").getOrCreate()
logger = spark._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)


#Function to fetch json from provided url and insert into mongodb
def write_db(url):
    # Online data source
    onlineData = url

    # read the online data file
    httpData = urlopen(onlineData).read().decode('utf-8')
    print(httpData)
    # convert into RDD
    rdd = spark.sparkContext.parallelize([httpData])

    # create a Dataframe
    jsonDF = spark.read.json(rdd)
    jsonDF.write.format('com.mongodb.spark.sql.DefaultSource').mode("append").save()

    return("write successful")

#writes the input json value to db
def write_json(jsonval):
    print(jsonval)
    # convert into RDD
    rdd = spark.sparkContext.parallelize([jsonval])

    # create a Dataframe
    jsonDF = spark.read.json(rdd)
    jsonDF.write.format('com.mongodb.spark.sql.DefaultSource').mode("append").save()
    userdetails_producer(topic, jsonval)
    return ("write successful")

#writes the upsert json value in db
#_id should be part of JSON provided
def update_results(jsonval):
    print(jsonval)
    # convert into RDD
    rdd = spark.sparkContext.parallelize([jsonval])

    # create a Dataframe
    jsonDF = spark.read.json(rdd)
    jsonDF.write.format('com.mongodb.spark.sql.DefaultSource').mode("append").option("replaceDocument", "false").save()
    return ("write successful")

def userdetails_producer(topic,message):
    producer = KafkaProducer(bootstrap_servers=
                             ['localhost:29092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    future = producer.send(topic, message)  # r.json())
    result = future.get(timeout=60)
    # producer.flush('userdetails_producer')
    return (result)

def resultread(var_userid):
    userid = var_userid
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
    df.createOrReplaceTempView("temp")
    result = spark.sql("SELECT  resulttext FROM temp WHERE userid = '{}'".format(userid))
    result.show()


if __name__ == "__main__":

    #url test
    # url = "http://127.0.0.1:5000/"
    # write_db(url)


    #Json load test

    x = '{ "_id":"Gary","username":"Gary", "age":30, "city":"New York","photo":"/Users/giridharangovindan/PycharmProjects/finalprojectPHOTO.jpg","resulttext":""}'
    y = json.loads(x)
    write_json(y)


    #update test

    # x_updated = '{"_id":"Gary","userid":"Gary", "age":30, "city":"New York","photo":"/Users/giridharangovindan/PycharmProjects/finalprojectPHOTO.jpg","resulttext":"This doesnot look like melanoma probably"}'
    # y_updated = json.loads(x_updated)
    # update_results(y_updated)


    #read test
    # df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
    # df.createOrReplaceTempView("temp")
    # result = spark.sql("SELECT  resulttext FROM temp WHERE userid = 'dave'")
    # result.show()


    #result fetch test passing user id
    # resultread('dave')