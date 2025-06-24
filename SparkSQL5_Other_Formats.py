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
    print("Succesfully load json file into DataFrame API")
    json_df.show()
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
    print("Succesfully load csv file into DataFrame API")
    csv_df.show()
except:
    raise Exception("Failed to read csv file")

# Reading avro file into a DataFrame
avro_dir = """./flights/summary-data/avro/*"""
try:
    avro_df = (spark.read.format("avro")
            .load(avro_dir)
            )
    print("Succesfully load avro file into DataFrame API")
    avro_df.show(truncate=False)
except:
    raise Exception("Failed to read avro file")

# Reading orc file into a DataFrame
orc_dir = """./flights/summary-data/orc/*"""
try:
    orc_df = (spark.read.format("orc")
            .load(orc_dir)
            )
    print("Succesfully load orc file into DataFrame API")
    orc_df.show(truncate=False)
except:
    raise Exception("Failed to read orc file")

# Creating Spark SQL TABLE directly from JSON files (also possible for VIEW)
try:
    (spark.sql("""CREATE TABLE IF NOT EXISTS flights_summary_tbl
           USING json
           OPTIONS (path "./flights/summary-data/json/*")
           """)
           )
    # and then see the VIEW
    print("Succesfully load json file into flights_summary_tbl")
    spark.table('flights_summary_tbl').show(5)
except:
    raise Exception("Failed to read json file")

# Creating Spark SQL TABLE directly from CSV files
try:
    (spark.sql("""CREATE TABLE IF NOT EXISTS flights_summary_tbl
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
    print("Succesfully load csv file into flights_summary_tbl")
    spark.table('flights_summary_tbl').show(5)
except:
    raise Exception("Failed to read csv file")

# Creating Spark SQL TEMP VIEW directly from avro files
try:
    (spark.sql("""CREATE OR REPLACE TEMP VIEW avro_episode_view
           USING avro
           OPTIONS (path "./flights/summary-data/avro/*")
           """)
           )
    # and then see the TABLE
    print("Succesfully load avro file into avro_episode_view")
    spark.table('avro_episode_view').show(5, truncate=False)
except:
    raise Exception("Failed to read avro file")

# Creating Spark SQL TEMP VIEW directly from orc files
try:
    (spark.sql("""CREATE OR REPLACE TEMP VIEW orc_delay_flights_view
           USING orc
           OPTIONS (path "./flights/summary-data/orc/*")
           """)
           )
    # and then see the TABLE
    print("Succesfully load avro file into orc_delay_flights_view")
    spark.table('orc_delay_flights_view').show(5, truncate=False)
except:
    raise Exception("Failed to read orc file")
finally:
    spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

try:
    print("saving JSON, CSV, avro, and ORC files ... ")
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

    # Writing DataFrames to orc files
    (avro_df.write.format("orc")
        .mode("overwrite")
        .option("compression", "snappy")
        .save("/tmp/data/orc/flights_orc")
        )

except:
    raise Exception ("failed saving")
finally:
    print("Saving successful")

# Furthermore, you can read image files and binary files, shown below as examples

# from pyspark.ml import image
# image_dir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
# images_df = spark.read.format("image").load(image_dir)
# images_df.printSchema()
# images.df.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, truncate=False)

# binary_path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
# binary_files_df = (spark.read.format("binaryFile")
#                    .option("pathGlobFilter", "*.jpg")
#                    .load(binary_path))
# binary_files_df.show(5)


# end the session
spark.stop()