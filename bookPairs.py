# set up SparkContext for WordCount application
from pyspark import SparkContext
import itertools
sc = SparkContext("local", "WordCount")

# the main map-reduce task
lines = sc.textFile("/home/cs143/data/goodreads.dat")
books = lines.map(lambda line: list(
    map(int, list(line.partition(":")[2].split(",")))))
# line.partition(":")[2].split(",") to get book numString
# list(map(int, StringList)) to convert string list to int list
pairs = books.flatMap(lambda book: list(itertools.combinations(book, 2)))
pairs1s = pairs.map(lambda pair: (pair, 1))
pairCounts = pairs1s.reduceByKey(lambda count1, count2: count1+count2)
freqPairCounts = pairCounts.filter(lambda pair: pair[1] > 20)
freqPairCounts.saveAsTextFile("/home/cs143/output1")
