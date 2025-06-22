from pyspark.sql import SparkSession

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQLOtherFormatsApp")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
) # pyspark --conf spark.sql.catalogImplementation=hive

spark.catalog.setCurrentDatabase("learn_spark_db")

# Reading JSON file into a DataFrame, similar to like how to read JSON files
json_file = """./flights/summary-data/json/*"""
try:
    df = spark.read.format("json").load(json_file)
    df.show()
    print("Succesfully load json file into DataFrame API")
except Exception as e:
    print("Failed to read json file")
    
# Creating Spark SQL TABLE directly from JSON files (also possible for VIEW)
try:
    (spark.sql("""CREATE OR REPLACE TABLE countryOriginDestination_count_tbl
           USING json
           OPTIONS (path "./flights/summary-data/json/*")
           """)
           )
    # and then see the VIEW
    spark.table('countryOriginDestination_count_tbl').show(5)
    print("Succesfully load json file into countryOriginDestination_count_tbl")
except Exception as e:
    print("Failed to read json file")
finally:
    spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Writing DataFrames to JSON files
(df.write.format("json")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("/temp/data/json/df_json")
    )



# end the session
spark.stop()