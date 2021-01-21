# simple code to count how many words are in the book.txt file
# code from course: Taming Big Data with Apache Spark and Python by Frank Kane, thanks a lot
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/luis.vanezian/Documents/spark-learning-handson/book.txt")
words = input.flatMap(lambda x: x.upper().split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
