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
json_dir = """./flights/summary-data/json/*"""
try:
    json_df = spark.read.format("json").load(json_dir)
    json_df.show()
    print("Succesfully load json file into DataFrame API")
except:
    raise Exception("Failed to read json file")

# Reading CSV file into a DataFrame
csv_dir = """./flights/summary-data/csv/*"""
try:
    schema = "`DEST_COUNTRY_NAME` STRING, `ORIGIN_COUNTRY_NAME` STRING, `count` INT"
    csv_df = (spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .option("mode", "FAILFAST") # Exit if any error
        .option("nullValue", "") # Replace any null data field with quotes
        .load(csv_dir)
        )
    csv_df.show()
    print("Succesfully load csv file into DataFrame API")
except:
    raise Exception("Failed to read csv file")

# Reading avro file into a DataFrame
avro_dir = """./flights/summary-data/avro/*"""
# avro_dir = "part-00000-tid-7128780539805330008-467d814d-6f80-4951-a951-f9f7fb8e3930-1434-1-c000.avro"
try:
    avro_df = (spark.read.format("avro")
            .load(avro_dir)
            )
    avro_df.show(truncate=False)
    print("Succesfully load avro file into DataFrame API")
except:
    raise Exception("Failed to read avro file")

# Creating Spark SQL TABLE directly from JSON files (also possible for VIEW)
try:
    (spark.sql("""CREATE OR REPLACE TABLE flights_summary_tbl
           USING json
           OPTIONS (path "./flights/summary-data/json/*")
           """)
           )
    # and then see the VIEW
    spark.table('flights_summary_tbl').show(5)
    print("Succesfully load json file into flights_summary_tbl")
except Exception as e:
    print("Failed to read json file")
# finally:
#     spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Creating Spark SQL TABLE directly from CSV files
try:
    (spark.sql("""CREATE OR REPLACE TABLE flights_summary_tbl
           USING csv
           OPTIONS (
               path "./flights/summary-data/csv/*",
               header "true",
               inferSchema "true",
               mode "FAILFAST"
               )
           """)
           )
    # and then see the TABLE
    spark.table('flights_summary_tbl').show(5)
    print("Succesfully load csv file into flights_summary_tbl")
except Exception as e:
    print("Failed to read csv file")
# finally:
#     spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Creating Spark SQL TEMP VIEW directly from avro files
try:
    (spark.sql("""CREATE OR REPLACE TEMP VIEW episode_view
           USING avro
           OPTIONS (path "./flights/summary-data/avro/*")
           """)
           )
    # and then see the TABLE
    spark.table('episode_view').show(5, truncate=False)
    print("Succesfully load avro file into episode_view")
except Exception as e:
    print("Failed to read avro file")
finally:
    spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Writing DataFrames to JSON files
(json_df.write.format("json")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("/tmp/data/json/df_json")
    )

# Writing DataFrames to CSV files
(csv_df.write.format("csv")
    .mode("overwrite")
    .save("/tmp/data/csv/df_csv")
    )

# Writing DataFrames to avro files
(avro_df.write.format("avro")
    .mode("overwrite")
    .save("/tmp/data/avro/df_avro")
    )

# end the session
spark.stop()