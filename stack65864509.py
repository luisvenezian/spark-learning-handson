# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 23:30:14 2021

@author: luis.vanezian
"""

from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.appName("StackoverflowQuestion").getOrCreate()

schema = StructType([\
                     StructField("data", StringType(), True), \
                     StructField("modified", LongType(), True)])
    
df = spark.read.option("sep", "|").schema(schema).csv("file:///Users/luis.vanezian/Documents/spark-learning-handson/stack65864509.txt")
df2 = df.withColumn("parsed_date", func.from_unixtime(func.col("modified")/1000).cast("date"))
df2.show()
                     
