# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 11:36:23 2021

@author: luis.vanezian
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("LessPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/luis.vanezian/Documents/spark-learning-handson/marvel_names.txt")

lines = spark.read.text("file:///Users/luis.vanezian/Documents/spark-learning-handson/marvel_graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
connections_filtered = connections.filter(connections.connections == 1)
connections_filtered.join(names, "id").select("name").show(connections_filtered.count())

