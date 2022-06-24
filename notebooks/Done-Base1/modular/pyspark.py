
#importing pyspark
import pyspark


#sc.stop()




from pyspark import SparkContext
sc =SparkContext()


#Create RDD from parallelize    
data = [1,2,3,4,5,6,7,8,9,10]
rdd=sc.parallelize(data)

print(rdd.collect())

#Create RDD from external Data source with the help of textfile you can read any kind of file like csv, json excel etc
rdd2 = sc.textFile("airlines1.csv")

print(rdd2.collect())


# Creates empty RDD with no partition    
rdd = sc.emptyRDD 

print(rdd)

#Create empty RDD with partition
rdd2 = sc.parallelize([],10) #This creates 10 partitions


# to check no of partition
print(rdd2.getNumPartitions())


# apply repartition on rdd for decrease or increase the no of partition
r1 = rdd2.repartition(5)


print(r1.getNumPartitions())


#coalesce() is used only to reduce the number of partitions
r2 = rdd2.coalesce(3)


print(r2.getNumPartitions())


# count
print(sc.parallelize((1, 2, 3, 4)).count())


# sum
print(sc.parallelize((1, 2, 3, 4)).sum())


# groupByKey
x = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
y = x.groupByKey()
print(x.collect())
print(list((j[0], list(j[1])) for j in y.collect()))



# Intersection
rdd1 = sc.parallelize([1, 10, 2, 3, 4, 5])
rdd2 = sc.parallelize([1, 6, 2, 3, 7, 8])
print(rdd1.intersection(rdd2).collect())


# Cartesian
rdd = sc.parallelize((1, 2))
print(rdd.cartesian(rdd).collect())


# countByKey
x = sc.parallelize([('J', 'James'), ('F','Fred'), ('A','Anna'), ('J','John')])
y = x.countByKey()
print(y)


# Filter
x = sc.parallelize([1,2,3])
y = x.filter(lambda x: x%2 == 1) #keep odd values
print(x.collect())
print(y.collect())





