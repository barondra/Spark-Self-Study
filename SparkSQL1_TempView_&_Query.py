from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, to_timestamp, date_format, lpad, col, desc, when

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
query = """SELECT distance, origin, destination 
FROM us_delays_flights_tbl
WHERE distance > 1000
ORDER BY distance DESC
"""
spark.sql(query).show(10)

# Alternatively, you can use the DataFrame API to achieve the same result
(df.select("distance", "origin", "destination").where(col("distance") > 1000).orderBy(desc("distance"))).show(10)


# Query to find all flights between San Fransisco (SFO) and Chicago (ORD) with at least two-hour delays
query = """SELECT date, delay, origin, destination
FROM us_delays_flights_tbl
WHERE origin = 'SFO' AND destination = 'ORD' and delay >= 120
ORDER BY delay DESC
"""
spark.sql(query).show(10)

# Alternatively, you can use the DataFrame API to achieve the same result
(df.select("date", "delay", "origin", "destination")
   .where((col("origin") == 'SFO') & (col("destination") == 'ORD') & (col("delay") >= 120))
   .orderBy(desc("delay"))
).show(10)

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
exercise_df.createOrReplaceTempView("exercise_table")

query = """SELECT day_of_week, COUNT(*) AS delay_count
FROM exercise_table
GROUP BY day_of_week
ORDER BY delay_count DESC
"""
spark.sql(query).show()

query = """SELECT month, COUNT(*) AS delay_count
FROM exercise_table
GROUP BY month
ORDER BY delay_count DESC
"""
spark.sql(query).show()

# we want to label all US flights with a human-readable column that indicates the delay experience
query = """SELECT delay, origin, destination, 
CASE 
    WHEN delay = 0 THEN 'No Delays'
    WHEN delay > 0 AND delay <= 60 THEN 'Tolerable Delays'
    WHEN delay > 60 AND delay <= 120 THEN 'Short Delays'
    WHEN delay > 120 AND delay <= 360 THEN 'Long Delays'
    WHEN delay > 360 THEN 'Very Long Delays'
    ELSE 'Early'
END AS Flight_Delays
FROM us_delays_flights_tbl
ORDER BY origin, delay DESC
"""
spark.sql(query).show(10)

# Alternatively, you can use the DataFrame API to achieve the same result
(df.select("delay", "origin", "destination")
 .withColumn("Flight_Delays", when(col("delay") == 0, "No Delays")
             .when((col("delay") > 0) & (col("delay") <= 60), "Tolerable Delays")
             .when((col("delay") > 60) & (col("delay") <= 120), "Short Delays")
             .when((col("delay") > 120) & (col("delay") <= 360), "Long Delays")
             .when(col("delay") > 360, "Very Long Delays")
             .otherwise("Early"))
    .orderBy("origin", desc("delay"))
 ).show(10)
# Stop the Spark session
spark.stop()