from pyspark.sql import SparkSession

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
)

print(spark.conf.get("spark.sql.warehouse.dir")) # file:/Users/macbook/Documents/Learning_Spark/spark-warehouse

# Check databases
spark.sql("SHOW DATABASES").show()

# Create a database
try:
    print("Deleting database learn_spark_db...")
    spark.sql("DROP DATABASE learn_spark_db CASCADE")
except Exception:
    print("Database does not exist, creating a new one.")    
    spark.sql("CREATE DATABASE learn_spark_db")
finally:
    spark.sql("USE learn_spark_db")
    print("Using database learn_spark_db")

# Create a managed table using SQL queries 
# good for data that is generated or processed within Spark and not shared with other systems
spark.sql("""
CREATE TABLE managed_us_delay_flights_tbl (
        date STRING, delay INT, distance INT, origin STRING, destination STRING
)"""
)

# Create an unmanaged table using SQL queries (good for external data)
# since data already exists outside Spark, we use the `USING CSV` clause
spark.sql("""CREATE TABLE unmanaged_us_delay_flights_tbl (
          date STRING, delay INT, distance INT, origin STRING, destination STRING)
          USING CSV OPTIONS ( PATH 
          '/Users/macbook/Documents/Learning_Spark/departuredelays.csv')
""")

# Show the tables in the current database
spark.sql("""SHOW TABLES""").show()

# Show the data in the table using SQL queries
spark.sql("""SELECT * FROM managed_us_delay_flights_tbl""").show()
spark.sql("""SELECT * FROM unmanaged_us_delay_flights_tbl""").show()

# Alternatively, you can create a managed table using DataFrame API
print("Deleting tables...")
spark.sql("DROP TABLE managed_us_delay_flights_tbl")
spark.sql("DROP TABLE unmanaged_us_delay_flights_tbl")
# Show the tables in the current database
spark.sql("""SHOW TABLES""").show()

# Path to our US delay flights dataset
csv_filepath = "/Users/macbook/Documents/Learning_Spark/departuredelays.csv"
# Schema is defined as per the dataset
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_filepath, schema=schema, header=True)

# Save the DataFrame as a managed table
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# Save the DataFrame as an unmanaged table
(flights_df
    .write
    .option("path", "/tmp/data/us_flights_delay")
    .saveAsTable("unmanaged_us_delay_flights_tbl")
)

# Show the tables in the current database
spark.sql("""SHOW TABLES""").show()

# Show the data in the table using DataFrame API
spark.table("managed_us_delay_flights_tbl").show()
spark.table("unmanaged_us_delay_flights_tbl").show()

# Stop the Spark session
spark.stop()