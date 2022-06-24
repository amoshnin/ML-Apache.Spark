

# import library for pyspark
import pyspark



# To create SparkContext
from pyspark import SparkContext




sc = SparkContext()





text_file = sc.textFile("sample.txt")





print(text_file.collect())





new1 = text_file.flatMap(lambda line: line.split("@"))





print(new1.collect())





text_file = sc.textFile("sample.txt")





counts = text_file.flatMap(lambda line: line.split(" "))





print(counts.collect())





counts1 = counts.map(lambda word: (word, 1))





print(counts1.collect())





counts3 = counts1.reduceByKey(lambda x, y: x + y)





print(counts3.collect())




print(counts3.filter(lambda x: x[0] not in ["for", "and", "not", "on", "the", "as", "of", "is"]).collect())







