from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///Users/luis.vanezian/Documents/spark-learning-handson/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: int(x[0] / x[1]))
results = averagesByAge.collect()
sortedResults = collections.OrderedDict(sorted(results))

print('Age\tAvgFriends')
for age, avgFriends in sortedResults.items():
    print(age,'\t',avgFriends)
