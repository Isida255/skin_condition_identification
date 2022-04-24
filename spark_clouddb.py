# program to write (insert/upsert) JSON to MongoDB using Spark
#program used confluent for cloud kafka services
import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import requests
from pyspark.sql import functions as F
#from kafka import KafkaProducer
#from kafka import KafkaConsumer
from urllib.request import Request, urlopen
from confluent_kafka import Producer
import socket
import json
import base64
from pyspark.sql.types import IntegerType,BooleanType,DateType

#Set variables
mongodburi = "mongodb+srv://admin:testAdmin@cluster0.k5ld4.mongodb.net/SkinConditionIdentification.UserInformation"
topic = "new_user"

my_spark = SparkSession.builder.master("local[*]").appName("myApp") \
    .config("spark.mongodb.input.uri", mongodburi) \
    .config("spark.mongodb.output.uri", mongodburi) \
    .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

spark = SparkSession.builder.appName("Python Spark Mongo DB write").getOrCreate()
logger = spark._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)

#writes the input json value to db
def write_json(jsonval):
    print(jsonval)
    # convert into RDD
    rdd = spark.sparkContext.parallelize([jsonval])

    # create a Dataframe
    jsonDF = spark.read.json(rdd)
    jsonDF.write.format('com.mongodb.spark.sql.DefaultSource').mode("append").save()
    #userdetails_producer(topic, jsonval)
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


#function to send the user json as a message for the topic"new_user"
def userdetails_producer(topic,message):
    conf = {'bootstrap.servers': 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': '75JYP7BH76CYFKC7',
            'sasl.password': 'rRErWkw8ZmMR/A00IuODq0CXvCk/NYGB5rXaO9oc93162oWfCZ4f0kD8fE+Q9SJC'}

    producer = Producer(conf)

    x = message
    y = json.dumps(x)

    producer.produce('new_user', key="test", value=y)
    producer.flush()

    return ("success")

#function to read the result data from mongodb for the _id provided"
def resultread(var_userid):
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
    df.createOrReplaceTempView("temp")
    result = spark.sql("SELECT  results FROM temp WHERE _id = '{}'".format(var_userid))
    print(result.collect()[0][0])
    returnresult = result.collect()[0][0]
    return(returnresult)

#function to read the entire data from database
def resultread_total():
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
    df.createOrReplaceTempView("temp")
    result = spark.sql("SELECT username, image_name,result, uploadDate FROM temp")
    print(result.collect())
    print(result)
    returnresult = result.collect()
    return(returnresult)

#function to read the entire data from database for given user id
def result_byuser(var_userid):
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
    df.createOrReplaceTempView("temp")
    result = spark.sql("SELECT  * FROM temp WHERE _id = '{}'".format(var_userid))
    print(result)
    return(result)

def resultread_total_grouped():
        df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
        df.createOrReplaceTempView("temp")
        result = spark.sql("SELECT count(*) as totalInsterted, username FROM temp group by username")
        #print(result.collect())
        #print(result)
        returnresult = result.collect()
        return(returnresult)

#function to read the entire data from database for given username
def result_byusername(var_username):
    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
    df.createOrReplaceTempView("temp")
    result = spark.sql("SELECT username, image_name,result, uploadDate FROM temp WHERE username = '{}'".format(var_username))
    #print(result.collect())
    #print(result)
    returnresult = result.collect()
    return(returnresult)



if __name__ == "__main__":
   print(result_byusername('isidaTest@outlook.com'))
