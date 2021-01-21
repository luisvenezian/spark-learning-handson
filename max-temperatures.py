# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 16:25:51 2021

@author: luis.vanezian
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0] 
    entryType = fields[2] 
    # Converting to Fahrenheit
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/luis.vanezian/Documents/spark-learning-handson/1800.csv")
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2])) # Getting stationId and Temperature
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect();

print("StationID\tTemperature")
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))