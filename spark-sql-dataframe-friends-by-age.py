# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 13:34:40 2021

@author: luis.vanezian
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("file:///Users/luis.vanezian/Documents/spark-learning-handson/fakefriends-header.csv")
    
people.select("age", "friends").groupBy("age").avg("friends").show()
spark.stop()


    