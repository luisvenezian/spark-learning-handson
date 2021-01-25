# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 17:44:56 2021

@author: luis.vanezian
"""

from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

schema = StructType([\
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True),\
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Loading movies dataframe    
movies_df = spark.read.option("sep", "\t").schema(schema).csv("file:///Users/luis.vanezian/Documents/spark-learning-handson/ml-100k/u.data")

top_movies = movies_df.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab top 10 popular movies
top_movies.show(10)

spark.stop()