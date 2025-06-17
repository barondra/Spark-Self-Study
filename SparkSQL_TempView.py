from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, to_timestamp, date_format, lpad, col

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate()
)

# Path to dataset
csv_filepath = "departuredelays.csv"

# Read and create temporary view
# Infer schema (note for larger files, you may want to specify the schema explicitly)
df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csv_filepath)
)
df.createOrReplaceTempView("us_delays_flights_tbl")

# Query to find flights that are more than 1,000 miles distance
query = """
SELECT distance, origin, destination 
FROM us_delays_flights_tbl
WHERE distance > 1000
ORDER BY distance DESC
"""
spark.sql(query).show(10)

# Query to find all flights between San Fransisco (SFO) and Chicago (ORD) with at least two-hour delays
query = """
SELECT date, delay, origin, destination
FROM us_delays_flights_tbl
WHERE origin = 'SFO' AND destination = 'ORD' and delay >= 120
ORDER BY delay DESC
"""
spark.sql(query).show(10)

# As an excercise, change the date into readable format, and find the days or months when these delays were most common. Were the delays related to winter months or holidays?
# Assume 'year' is 2015
exercise_df = (spark.sql(query)
    .withColumn("date", lpad(col("date"), 8, "0"))  # Ensure date is zero-padded to 8 digits
    .withColumn("month", col("date").substr(1, 2))
    .withColumn("day", col("date").substr(3, 2))               
)
exercise_df = exercise_df.withColumn("datetime_full", concat(lit("2015"), col("date"))) 
exercise_df = exercise_df.withColumn("datetime_actual", to_timestamp(col("datetime_full"), "yyyyMMddHHmm"))
# Get day of the week as string (e.g., 'Monday')
exercise_df = exercise_df.withColumn("day_of_week", date_format(col("datetime_actual"), "EEEE"))
exercise_df.show()
