# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 14:19:02 2021

@author: luis.vanezian
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as fn 

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of Frank's book into a dataframe
df = spark.read.text("file:///Users/luis.vanezian/Documents/spark-learning-handson/book.txt")

# Split using REGEX thar extracts the words W+
book = df.select(fn.explode(fn.split(df.value, "\\W+")).alias("word"))
book.filter(book.word != "")

# Normalize everything to lowercase 
book = book.select(fn.lower(book.word).alias("word"))

# Count up the occurrences of each word and sort by 
book_sorted = book.groupBy("word").count().sort("count")

book_sorted.show(book_sorted.count())