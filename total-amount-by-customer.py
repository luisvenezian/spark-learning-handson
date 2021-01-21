# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 17:58:39 2021

@author: luis.vanezian
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountByCustomer")
sc = SparkContext(conf = conf)

def get_data(line):
    line = line.split(",")
    costumer_id = int(line[0])
    value = float(line[2])
    return (costumer_id, value)

data_object = sc.textFile("file:///Users/luis.vanezian/Documents/spark-learning-handson/customer-orders.csv")
total_by_costumer = data_object.map(get_data).reduceByKey(lambda x, y: x + y)    

total_by_costumer_sorted = total_by_costumer.map(lambda x: (x[1], x[0])).sortByKey()
results = total_by_costumer_sorted.collect()

print("CostumerID\tTotal")
for value, costumer in results:
    print("{}\t{:.2f}".format(costumer, value))