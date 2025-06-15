
# RDD version
from pyspark import SparkContext, SparkConf

# Create an RDD of tuples (name, age)
conf = SparkConf().setAppName("AverageAge").setMaster("local")
sc = SparkContext(conf=conf)

dataRDD = sc.parallelize([("Brooke", 20),
                          ("Denny", 31),
                          ("Jules", 30),
                          ("Thompson", 35),
                          ("Brooke", 25)])
# Use map and reduceByKey transformations with their lambda expressions to 
# aggregate and then compute average
agesRDD = (dataRDD
           .map(lambda x: (x[0], (x[1], 1)))
           .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
           .map(lambda x: (x[0], x[1][0] / x[1][1])))

# Collect the results
print(agesRDD.collect())
sc.stop()

# DataFrame version
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
# Create a Spark session
spark = (SparkSession
         .builder
         .appName("AuthorsAges")
         .getOrCreate())
# Create a DataFrame from the data
dataDF = spark.createDataFrame([("Brooke", 20),
                                 ("Denny", 31),
                                 ("Jules", 30),
                                 ("Thompson", 35),
                                 ("Brooke", 25)],
                                ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avgDF = (dataDF
         .groupBy("name")
         .agg(avg("age").alias("average_age"))
         .orderBy("name", ascending=True))
# Show the results
avgDF.show()
# Stop the Spark session
spark.stop()