# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 16:36:19 2021

@author: luis.vanezian
"""
from __future__ import print_function
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisionTreeRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    inputData = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv("file:///Users/luis.vanezian/Documents/spark-learning-handson/realestate.csv")
    
    # We will use Age, Distance to MRT (Public transportation) 
    # and number of nearby convenience stores to predict the Price Per Unit
    # Let's prepare our df with the format that desicion trees expect 
    assembler = VectorAssembler().setInputCols(["HouseAge","DistanceToMRT", "NumberConvenienceStores"]).setParams(outputCol="features")
    df = assembler.transform(inputData).select("PriceOfUnitArea", "features")
    # df.show(5)
    

    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our linear regression model
    # lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    dtr = DecisionTreeRegressor().setLabelCol("PriceOfUnitArea")
    
    # Train the model using our training data
    model = dtr.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our linear regression model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()

    # Extract the predictions and the "known" correct labels.
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])
    
    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Stop the session'''
    spark.stop()