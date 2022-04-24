# program to write to send new messages when new evaluations are added to mongodb
#program uses confluent for cloud kafka services
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import requests
from pyspark.sql import functions as F
from kafka import KafkaProducer
from kafka import KafkaConsumer
from urllib.request import Request, urlopen
from confluent_kafka import Producer
import socket
import json
import base64
from pyspark.sql.types import IntegerType,BooleanType,DateType

#Set variables
mongodburi = "mongodb+srv://admin:testAdmin@cluster0.k5ld4.mongodb.net/SkinConditionIdentification.UserInformation"
#mongodburi = "mongodb+srv://admin:testAdmin@cluster0.k5ld4.mongodb.net/Client.Test"
topic = "new_user"

my_spark = SparkSession.builder.master("local[*]").appName("myApp") \
    .config("spark.mongodb.input.uri", mongodburi) \
    .config("spark.mongodb.output.uri", mongodburi) \
    .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

spark = SparkSession.builder.appName("Python Spark Mongo DB write").getOrCreate()
logger = spark._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)


if __name__ == "__main__":

    # block to send the new data to physicians for further review
        conf = {'bootstrap.servers': 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': '75JYP7BH76CYFKC7',
                'sasl.password': 'rRErWkw8ZmMR/A00IuODq0CXvCk/NYGB5rXaO9oc93162oWfCZ4f0kD8fE+Q9SJC'}

        producer = Producer(conf)
        producer.flush()
        df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
        df.createOrReplaceTempView("temp")
        result = spark.sql("SELECT  * FROM temp where uploaddate is not null and result is not null and image is not null and review is null ")
        # result = spark.sql("SELECT  * FROM temp where to_date(uploadDate,'yyyy-MM-dd') = current_date()")
        print(result.count())
        dataColl = result.collect()

        for row in dataColl:
            jsonrow = json.dumps(row.asDict())
            print(jsonrow)
            # print(type(jsonrow))
            producer.produce(topic, key="test", value=jsonrow)
            producer.flush()



