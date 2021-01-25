# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 16:22:21 2021

@author: luis.vanezian
"""

from pyspark.sql import SparkSession 
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("TotalAmountByCustomer").getOrCreate()

schema = StructType([\
                     StructField("customerID", StringType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("total", StringType(), True)])

    # Read the file as dataframe
df = spark.read.schema(schema).csv("file:///Users/luis.vanezian/Documents/spark-learning-handson/customer-orders.csv")
amount_by_costumer = df.select("customerID","total").groupBy("customerID").agg(func.round(func.sum("total"),2).alias("total")).sort("total")
amount_by_costumer.show(amount_by_costumer.count())

spark.stop()