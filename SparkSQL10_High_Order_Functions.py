from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQL_HighOrderFunc_App")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
) # pyspark --conf spark.sql.catalogImplementation=hive

spark.catalog.setCurrentDatabase("learn_spark_db")
spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

schema = StructType([StructField("celcius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

t_c.show()

# With t_c DataFrame, you can run the following higher-order function queries

# transform(), produces an array by applying a function to each element, similar to map()
print("transform() example: Please convert celcius to fahrenheit")
spark.sql("""
          SELECT celcius, transform(celcius, t -> ((t*9) div 5) + 32) as fahrenheit
          FROM tC
          """).show()

# filter(), produces an array consisting of only elements with True condition
print("filter() example: Lets see temperatures higher than 38C")
spark.sql("""
          SELECT celcius, 
            filter(celcius, t -> t > 38) as high
          FROM tC
          """).show()

# exists(), returns True if the Boolean function holds for any element in the input array
print("exists() example: Is there a temperature of 38C in the array?")
spark.sql("""
          SELECT celcius, 
            exists(celcius, t -> t = 38) as threshold
          FROM tC
""").show()

# reduce(), reduces the element of the array to a single value by merging the elements into a buffer B
print("reduce() example: Calculate average temperature and convert to F")
spark.sql("""
          SELECT celcius,
            reduce(
                celcius,
                0,
                (t, acc) -> t + acc,
                acc -> (acc div size(celcius) * 9 div 5) + 32      
            ) as avgFahrenheit
          FROM tC
          """).show()

# End the session
spark.stop()