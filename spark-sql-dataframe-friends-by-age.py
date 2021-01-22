# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 13:34:40 2021

@author: luis.vanezian
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("file:///Users/luis.vanezian/Documents/spark-learning-handson/fakefriends-header.csv")
    
friendsByAge = people.select("age", "friends") 
# friendsByAge.groupBy("age").avg("friends").show()
# Sorted by age
# friendsByAge.groupBy("age").avg("friends").sort("age").show()
# Formatted more nicely ;)
# friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()
# With a custom column name ;))
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")) \
    .sort("age").show()

spark.stop()


    