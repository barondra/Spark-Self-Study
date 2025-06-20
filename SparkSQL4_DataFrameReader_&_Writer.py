from pyspark.sql import SparkSession

# Recommended  pattern of using DataFrameReader
# (spark.read
#     .format() # insert the format of the data source, e.g., "csv", "parquet", etc.
#     .option() # insert any options you want to set, e.g., "header", "inferSchema", etc.
#     .schema() # insert the schema if needed
#     .load()   # insert the path to the data source
# )

# Recommended pattern for writing DataFrame to a data source
# pattern no.1: to save as a data source (e.g., CSV, Parquet, etc.)
# (df.write
#     .format() # insert the format of the data source, e.g., "csv", "parquet", etc.
#     .option() # insert any options you want to set, e.g., "header", "mode", etc.
#     .bucketBy() # insert the number of buckets and the column(s) to bucket by
#     .partitionBy() # insert the column(s) to partition by
#     .save() # insert the path to the data source
# )
# pattern no.2: to save as table in the Hive metastore
# (df.write
#  .format() # insert the format of the data source, e.g., "csv", "parquet", etc.
#  .option() # insert any options you want to set, e.g., "header", "mode", etc.
#  .sortBy() # insert the column(s) to sort by
#  .saveAsTable()  # insert the name of the table to save as
# )

# Create a Spark session
spark = (SparkSession.builder
         .appName("DataFrameReaderAndWriter")
         .enableHiveSupport()
         .getOrCreate()
)

spark.catalog.setCurrentDatabase("learn_spark_db")
spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Load parquet file into DataFrame API       
parquet_file = """./flights/summary-data/parquet/2010-summary.parquet/"""
(spark.read
      .format("parquet")
      .load(parquet_file)
      ).show(5)  # Display the first 5 rows of the DataFrame

# Creating Spark SQL TEMP VIEW directly from parquet files (also possible for tables)
(spark.sql("""CREATE OR REPLACE TEMPORARY VIEW countryOriginDestionation_view
           USING parquet
           OPTIONS (path "./flights/summary-data/parquet/2010-summary.parquet/")
           """)
           )
# and then see the VIEW
spark.table('countryOriginDestionation_view').show(5)

# End spark session
spark.stop()