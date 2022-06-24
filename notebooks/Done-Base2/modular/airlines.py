
import pandas as pd
df = pd.read_csv("airlines1.csv")

print(df.columns.values)

#importing pyspark
import pyspark

#importing sparksession
from pyspark.sql import SparkSession

#creating a sparksession object and providing appName 
spark=SparkSession.builder.appName("airline").getOrCreate()

#spark.stop()

#To create dataframe form External datasets
df = spark.read.option("header", "true").csv("airlines1.csv")


print(df.show())

df1 = df.select("Year", "Month", "DayofMonth" , "FlightDate","Tail_Number", "Flight_Number_Reporting_Airline" )

# To display dataframe use show() function it display first 20 rows
print(df1.show())

print(df1.collect())

# show all colunms names
print(df1.columns)

#to count total no for rows in dataframe
print(df1.count())

# to display the statistical properties of all the columns in the dataset
print(df1.describe().show())

#group data by using group() function
df1.select('Year').groupBy('Year').count().show()

d1 = df1.select('Year')
d2 = d1.groupBy('Year')
d3 = d2.count()


print(d3.show(25))


#group by month
print(df1.select('Month').groupBy('Month').count().show())


# filter data using filter() function
print(df1.select('Year').filter("Month = 6").groupBy( 'Year').count().show())




#to print schema of columns use printSchema() function
print(df1.printSchema())


