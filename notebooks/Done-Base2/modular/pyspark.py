# import library for pyspark
import pyspark

# To create SparkContext
from pyspark import SparkContext


sc = SparkContext()

#sc1 =SparkContext()



#sc1.stop()


# # How to create RDDs

# # Methods of RDDs
# 1. from variable
# 2. from RDD
# 3. from External Datasets


#from passing variables use parallelize 
x = [1,2,3,4,5]
y = sc.parallelize(x)

print(y)



print(y.collect()) # Action


#print(x.collect())


# # Transformation Functions

# # map()


#It applies to each element of RDD and it returns the result as new RDD. ... 
#Map transforms an RDD of length N into another RDD of length N. 
#The input and output RDDs will typically have the same number of records.




a1 = ["b","a","c"]
x = sc.parallelize(a1)

y = x.map(lambda z: (z, 1))




print(y.collect())


# # flatmap()



#It applies to each element of RDD and it returns the result as new RDD. 
#It is similar to Map, but FlatMap allows returning 0, 1 or more elements from map function.



x = sc.parallelize([1,2,3])
y = x.flatMap(lambda x: (x, x*100, 42))




print(y.collect())




d1 = ["This is a FlatMap operation in PySpark"] 
rdd1 = sc.parallelize(d1)
rdd2 = rdd1.flatMap(lambda x: x.split(" "))




print(rdd2.collect())




y = rdd2.map(lambda z: (z, 1))




print(y.collect())




print(rdd2.collect())


# # filter()



#It returns an RDD that only has element that pass the condition mentioned in input function.




x = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
y = x.filter(lambda x: x%2 == 1) #keep odd values





print(y.collect())




print(rdd2.collect())




y = rdd2.filter(lambda x: x == 'operation')




print(y.collect())


# # distinct()



#It returns a new dataset that contains the distinct elements of the source dataset. 
# It is helpful to remove duplicate data.
# For example, if RDD has elements (Spark, Spark, Hadoop, Flink), 
# then rdd.distinct() will give elements (Spark, Hadoop, Flink).




print(sc.parallelize(('a','r','a','h','h')).distinct().collect())




# Find max flight in year 2000


# # Actions Functions

# # count()



#Action count() returns the number of elements in RDD.
print(sc.parallelize((1, 2, 3, 4)).count())


# # sum()



# It adds up the value in an RDD.
print(sc.parallelize((1, 2, 3, 4)).sum())


# # max()



#Return the maximum value from the dataset.
x = sc.parallelize([2,4,1])




print(x.max())




print(x.min())


# # mean()




#Alias for Avg. Returns the average of the values in a column.
x = sc.parallelize([2,4,1])
y = x.mean()




print(x.mean())


