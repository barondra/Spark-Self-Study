from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQL_HighOrderFunc_App")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
) # pyspark --conf spark.sql.catalogImplementation=hive

spark.catalog.setCurrentDatabase("learn_spark_db")
spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Step 1. Import two files and create two DataFrames, one for airport (airportsna) information and one for US flight delays (departureDelays)

# Set file paths
tripdelaysFilePath = "departuredelays.csv"
airportsnaFilePath = "airport-codes-na.txt"

# Obtain airports dataset
airportsna = (spark.read
              .format("csv")
              .options(header="true", inferSchema="true", sep="\t")
              .load(airportsnaFilePath))

airportsna.createOrReplaceTempView("airports_na")

# Obtain the departure delays dataset
departureDelays = (spark.read
                   .format("csv")
                   .options(header="true")
                   .load(tripdelaysFilePath))

# Step 2. Using expr(), convert delay and distance columns from STRING to INT
departureDelays = (departureDelays
                   .withColumn("delay", expr("CAST(delay as INT) as delay"))
                   .withColumn("distance", expr("CAST(distance as INT) as distance")))

departureDelays.createOrReplaceTempView("departureDelays")

# Step 3. Create a smaller table, foo, that we can focus on for our demo examples; it contains only information on three flights originating from Seattle (SEA) to the destination of San Fransisco (SFO) for a small time range.

foo = (departureDelays
       .filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")))
foo.createOrReplaceTempView("foo")

print("Showing airports_na table")
spark.sql("SELECT * FROM airports_na LIMIT 10").show()
print("Showing departureDelays table")
spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
print("Showing foo table")
spark.sql("SELECT * FROM foo").show()

# Operations no. 1 : Unions and joins
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")

# Show the union (filtering for SEA and SFO in a specific time range)
print("Union example: filter bar view union from departureDelays and foo")
print("using DF API for filtering")
bar.filter(expr("""origin == 'SEA' 
                AND destination == 'SFO' 
                AND date LIKE '01010%'
                AND delay > 0""")).show()
# Alternatively in SQL:
print("using Spark SQL for filtering")
spark.sql("""
          SELECT *
          FROM bar
          WHERE origin == 'SEA'
            AND destination == 'SFO'
            AND date LIKE '01010%'
            AND delay > 0
""").show()

# Join between airportsna and foo DataFrames
print("Join example: airportsna and foo DataFrames")
print("using DF API")
foo.join(
    airportsna,
    airportsna.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()
# Alternatively in SQL:
print("using Spark SQL")
spark.sql("""
          SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
          FROM foo f
          JOIN airports_na a
              ON a.IATA = f.origin
""").show()

# Operation no. 2: Windowing
# Windowing with dense_rank() function
# Start with review of Total Delays for specified origin and destination flights
windowing_query_a = """
       DROP VIEW IF EXISTS departureDelaysWindow;
"""
windowing_query_b = """
       CREATE TEMP VIEW departureDelaysWindow AS
       SELECT origin, destination, SUM(delay) AS TotalDelays
       FROM departureDelays
       WHERE origin IN ("SEA", "SFO", "JFK")
       AND destination IN ("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL")
       GROUP BY origin, destination;
"""
windowing_query_c = """
       SELECT * FROM departureDelaysWindow ORDER BY origin
"""
spark.sql(windowing_query_a)
spark.sql(windowing_query_b)
print("Window Table: to find most delays for each origin")
spark.sql(windowing_query_c).show()

# Then, for each of these origin airports you wanted to find the three destinations most delays
# You could achieve by running the following query three times for each [ORIGIN], and union them
delay_query_a = """
       SELECT origin, destination, TotalDelays
       FROM departureDelaysWindow
       WHERE origin = 'JFK' 
       ORDER BY TotalDelays DESC
       LIMIT 3
"""
delay_query_b = """
       SELECT origin, destination, TotalDelays
       FROM departureDelaysWindow
       WHERE origin = 'SFO' 
       ORDER BY TotalDelays DESC
       LIMIT 3
"""
delay_query_c = """
       SELECT origin, destination, TotalDelays
       FROM departureDelaysWindow
       WHERE origin = 'SEA' 
       ORDER BY TotalDelays DESC
       LIMIT 3
"""
print("using Union and SparkSQL for each origin")
spark.sql(delay_query_a).union(spark.sql(delay_query_b).union(spark.sql(delay_query_c))).show()

# A better approach would be using window function, here dense_rank() is given as example
print("comparison using dense_rank() and window function OVER (PARTITION BY)")
spark.sql("""
       SELECT origin, destination, TotalDelays, rank
          FROM (
              SELECT origin, destination, TotalDelays, dense_rank()
                     OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
                     FROM departureDelaysWindow
          ) t
          WHERE rank <= 3
""").show()

# Operations no. 3: Modifications, create new DataFrame born from existing DF with some changes
# Adding new column
print("Modification #1 : Adding new column")
foo2 = (foo.withColumn(
    "status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    ))
foo2.show()

# Dropping existing column
print("Modification #2 : Dropping existing column")
foo3 = foo2.drop("delay")
foo3.show()

# Renaming column
print("Modification #3 : Renaming column")
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# Pivoting, i.e., swap the column for rows
# first, let grab some data
print("Let's grab data to be pivoted")
spark.sql("""
       SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
          FROM departureDelays
          WHERE origin = 'SEA'
""").show()
# with pivoting, names of each month can be used for column (instead of all months in single column)
# furthermore, you can perform aggregate calculations on them
print("Pivoted example")
spark.sql("""
       SELECT * FROM (
          SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
              FROM departureDelays WHERE origin = 'SEA'
          )
       PIVOT (
          CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
          FOR month in (1 JAN, 2 FEB, 3 MAR)
          )
       ORDER BY destination
""").show()

# End the session
spark.stop()