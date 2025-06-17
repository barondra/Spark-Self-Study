from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import os
import shutil

# Create a Spark session
spark = SparkSession.builder.appName("DataFrameReadAndWrite").getOrCreate()

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                StructField('UnitID', StringType(), True),
                StructField('IncidentNumber', IntegerType(), True),
                StructField('CallType', StringType(), True),
                StructField('CallDate', StringType(), True),
                StructField('WatchDate', StringType(), True),
                StructField('CallFinalDisposition', StringType(), True),
                StructField('AvailableDtTm', StringType(), True),
                StructField('Address', StringType(), True),
                StructField('City', StringType(), True),
                StructField('Zipcode', IntegerType(), True),
                StructField('Battalion', StringType(), True),
                StructField('StationArea', StringType(), True),
                StructField('Box', StringType(), True),
                StructField('OriginalPriority', StringType(), True),
                StructField('Priority', StringType(), True),
                StructField('FinalPriority', IntegerType(), True),
                StructField('ALSUnit', BooleanType(), True),
                StructField('CallTypeGroup', StringType(), True),
                StructField('NumAlarms', IntegerType(), True),
                StructField('UnitType', StringType(), True),
                StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                StructField('FirePreventionDistrict', StringType(), True),
                StructField('SupervisorDistrict', StringType(), True),
                StructField('Neighborhood', StringType(), True),
                StructField('Location', StringType(), True),
                StructField('RowID', StringType(), True),
                StructField('Delay', FloatType(), True)])

# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "./sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# Show the DataFrame  
fire_df.show(5)  # Display the first 5 rows of the DataFrame

# Print the schema of the DataFrame
print(fire_df.printSchema())  # Print the schema in a tree format
#print(fire_df.schema)  # Print the schema object definition

# Incidents that are not medical incidents
few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)  # Display the first 5 rows of the filtered DataFrame

# How many distinct CallTypes were recorded as the causes of the fire calls
(fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())  # Filter out null CallTypes
    .agg(count_distinct("CallType").alias("DistinctCallTypes"))
    .show()) # Display the count of distinct CallTypes

# Rename columns in the DataFrame
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayinMins")
(new_fire_df
    .select("IncidentNumber", "AvailableDtTm", "CallType", "ResponseDelayinMins")
    .where(col("responseDelayinMins") > 5)
    .show(5, truncate=False))

fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
              .drop("CallDate")
              .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
              .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
              .drop("AvailableDtTm"))

# select and show the transformed DataFrame with timestamp columns
(fire_ts_df
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, truncate=False))

# explore data further based on the timestamp columns
(fire_ts_df
    .select(year("IncidentDate").alias("IncidentYear"))
    .distinct()
    .orderBy("IncidentYear")
    .show())


# Grouping: What were the most common CallTypes?
(fire_ts_df
    .select("CallType")
    .where(col("CallType").isNotNull())  # Filter out null CallTypes
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show(10, truncate=False)  # Display the top 10 most common CallTypes
 )

# Compute sum of alarms, average response time, minimum and maximum response time
(fire_ts_df
    .select(F.sum("NumAlarms").alias("TotalAlarms"),
            F.avg("ResponseDelayinMins").alias("AvgResponseDelay"),
            F.min("ResponseDelayinMins").alias("MinResponseDelay"),
            F.max("ResponseDelayinMins").alias("MaxResponseDelay"))
    .show())  # Display the computed statistics

# to save as Parquet file
parquet_path = "./sf-few-fire-calls.parquet"
if os.path.exists(parquet_path):
    shutil.rmtree(parquet_path)  # Remove the existing directory if it exists
few_fire_df.write.format("parquet").save(parquet_path)

# alternatively, to save as a table in the Hive metastore
parquet_table = "sf_few_fire_calls_parquet_table"
if os.path.exists("spark-warehouse/" + parquet_table):
    shutil.rmtree("spark-warehouse/" + parquet_table)
few_fire_df.write.format("parquet").saveAsTable(parquet_table)

# stop the Spark session
spark.stop()