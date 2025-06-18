from pyspark.sql import SparkSession

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
)

# Path to dataset
spark.sql("USE learn_spark_db")

# Case we only want to use data with origin of New York (JFK) and San Francisco (SFO)
# we set SFO as global temporary view and JFK as session-scope temporary view.

# Create a global temporary view using SQL queries, extracting data from script SparkSQL2_Database_&_Tables.py
spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW SFO_global_tmp_view AS
    SELECT date, delay, distance, origin, destination
    FROM unmanaged_us_delay_flights_tbl
    WHERE origin = 'SFO'
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW JFK_tmp_view AS
    SELECT date, delay, distance, origin, destination
    FROM unmanaged_us_delay_flights_tbl
    WHERE origin = 'JFK'
""")

# Show the tables in the global database
spark.sql("""SHOW TABLES IN global_temp""").show()

# Show the tables in the current database
spark.sql("""SHOW TABLES""").show()


# Drop views if they already exist
# actually, this is not necessary, as `CREATE OR REPLACE` will overwrite existing views
# try:
#     spark.sql("DROP VIEW global_temp.SFO_global_tmp_view")
#     spark.sql("DROP VIEW JFK_tmp_view")
# except Exception as e:
#     print("Views do not exist. Skipping drop operation.")


# Alternatively, we can create a global temporary view using DataFrame API
sfo_df = spark.sql("SELECT date,delay,distance,origin,destination FROM unmanaged_us_delay_flights_tbl WHERE origin = 'SFO'")
sfo_df.createOrReplaceGlobalTempView("SFO_global_tmp_view")

jfk_df = spark.sql("SELECT date,delay,distance,origin,destination FROM unmanaged_us_delay_flights_tbl WHERE origin = 'JFK'")
jfk_df.createOrReplaceTempView("JFK_tmp_view")

# queries accessing the global temporary view must use the global_temp database
spark.sql("SELECT * FROM global_temp.SFO_global_tmp_view").show()

# by contrast, queries accessing the NOT-global temporary view does not require the prefix
spark.sql("SELECT * FROM JFK_tmp_view").show()

# End session
spark.stop()

# The difference between global temporary views and session-scope temporary views:
# - Global temporary views are shared across all sessions and can be accessed using the `global_temp` database prefix.
# - Session-scope temporary views are only accessible within the session they were created in and do not require any prefix.