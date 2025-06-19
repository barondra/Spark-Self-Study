from pyspark.sql import SparkSession

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
)

# Use Catalog API to examine databases and tables
print("\nAvailable databases:")
spark.createDataFrame(spark.catalog.listDatabases()).show() # spark.sql("SHOW DATABASES").show()
# print(spark.catalog.listDatabases())

# use a specific database
print("Currently active database:", spark.catalog.currentDatabase())
spark.catalog.setCurrentDatabase("learn_spark_db") # spark.sql("USE learn_spark_db")
print("Currently active database:", spark.catalog.currentDatabase())

# Show the tables in the current database
print("\nTables in current database:")
spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()
# print(spark.catalog.listTables()) # spark.sql("""SHOW TABLES""").show()

# Show the tables in the global_temp database
print("\nTables in global_temp database:")
spark.createDataFrame(spark.catalog.listTables("global_temp"), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()
# print(spark.catalog.listTables("global_temp")) # spark.sql("""SHOW TABLES IN global_temp""").show()

# Case we only want to use data with origin of New York (JFK) and San Francisco (SFO)
# we set SFO as global temporary view and JFK as session-scope temporary view.

# Create a global temporary view using SQL queries, extracting data from script SparkSQL2_Database_&_Tables.py
try:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW JFK_tmp_view AS
        SELECT date, delay, distance, origin, destination
        FROM unmanaged_us_delay_flights_tbl
        WHERE origin = 'JFK'
    """)
    spark.sql("""
        CREATE OR REPLACE GLOBAL TEMP VIEW SFO_global_tmp_view AS
        SELECT date, delay, distance, origin, destination
        FROM unmanaged_us_delay_flights_tbl
        WHERE origin = 'SFO'
    """)
except Exception as e:
    print("Error creating views:", e)
finally:
    print("Global temporary view SFO_global_tmp_view and session-scope temporary view JFK_tmp_view created or replaced successfully.")

# Show the tables in the current database
print("\nTables in current database:")
spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Show the tables in the global_temp database
print("\nTables in global_temp database:")
spark.createDataFrame(spark.catalog.listTables("global_temp"), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Alternatively, we can create a global temporary view using DataFrame API
try:
    jfk_df = spark.sql("SELECT date,delay,distance,origin,destination FROM unmanaged_us_delay_flights_tbl WHERE origin = 'JFK'")
    jfk_df.createOrReplaceTempView("JFK_tmp_view")

    sfo_df = spark.sql("SELECT date,delay,distance,origin,destination FROM unmanaged_us_delay_flights_tbl WHERE origin = 'SFO'")
    sfo_df.createOrReplaceGlobalTempView("SFO_global_tmp_view")

except Exception as e:
    print("Error creating views:", e)
finally:
    print("Global temporary view SFO_global_tmp_view and session-scope temporary view JFK_tmp_view created or replaced successfully.")


# spark catalog list column
print("\nColumns in session-scope temporary view JFK_tmp_view:")
spark.createDataFrame(spark.catalog.listColumns("JFK_tmp_view"), schema="`name` STRING, `description` STRING, `datatype` STRING, `nullable` BOOLEAN, `isPartition` BOOLEAN, `isBucket` BOOLEAN, `isCluster` BOOLEAN").show()
print("\nColumns in global temporary view SFO_global_tmp_view:")
spark.createDataFrame(spark.catalog.listColumns("global_temp.SFO_global_tmp_view"), schema="`name` STRING, `description` STRING, `datatype` STRING, `nullable` BOOLEAN, `isPartition` BOOLEAN, `isBucket` BOOLEAN, `isCluster` BOOLEAN").show()

# Accessing the session-scope temporary view does not require the prefix
print("\nAccesing session-scope temporary view JFK_tmp_view:")
# spark.sql("SELECT * FROM JFK_tmp_view").show() # using SQL queries
spark.table("JFK_tmp_view").show() # using DataFrame API

# by contrast, to access the global-scope temporary view must use the global_temp database
print("\nAccessing global temporary view SFO_global_tmp_view:")
# spark.sql("SELECT * FROM global_temp.SFO_global_tmp_view").show() # using SQL queries
spark.table("global_temp.SFO_global_tmp_view").show() # using DataFrame API

# End session
spark.stop()

# The difference between global temporary views and session-scope temporary views:
# - Global temporary views are shared across all sessions and can be accessed using the `global_temp` database prefix.
# - Session-scope temporary views are only accessible within the session they were created in and do not require any prefix.