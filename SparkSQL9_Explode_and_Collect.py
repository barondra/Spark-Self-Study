from pyspark.sql import SparkSession

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQL_Explode&Collect_App")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
) # pyspark --conf spark.sql.catalogImplementation=hive

spark.catalog.setCurrentDatabase("learn_spark_db")
spark.createDataFrame(spark.catalog.listTables(), schema="`name` STRING, `catalog` STRING, `namespace` ARRAY<STRING>, `description` STRING, `tableType` STRING, `isTemporary` BOOLEAN").show()

# Generate temporary view
spark.sql("""
    CREATE OR REPLACE TEMP VIEW base_table AS
    SELECT * FROM VALUES
        (1, array(1, 2, 3)),
        (2, array(4, 5))
    AS t(id, values)"""
)
# See the VIEW
spark.table('base_table').show(truncate=False)

# Nested SQL: Explode and Collect Query 
query = """
SELECT id, collect_list(value + 1) AS values
FROM (
  SELECT id, EXPLODE(values) AS value
  FROM base_table
) x
GROUP BY id
"""

# Step 1: EXPLODE, turns each array element into a separate row
print("Step 1: EXPLODE, turns each array element into a separate row")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW explode_table AS
    SELECT * FROM VALUES
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 4),
        (2, 5)
    AS t(id, value)"""
)
# See the VIEW
spark.table('explode_table').show(truncate=False)

# Step 2: value + 1, adds 1 to each value
print("Step 2: value + 1, adds 1 to each value")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW plusOne_table AS
    SELECT * FROM VALUES
        (1, 2),
        (1, 3),
        (1, 4),
        (2, 5),
        (2, 6)
    AS t(id, value)"""
)
# See the VIEW
spark.table('plusOne_table').show(truncate=False)

# Step 3: collect_list and GROUP BY, Groups by id and collects the incremented values into an aray
print("Step 3: collect_list and GROUP BY, Groups by id and collects the incremented values into an aray")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW final_table AS
    SELECT * FROM VALUES
        (1, array(2, 3, 4)),
        (2, array(5, 6))
    AS t(id, values)"""
)
# See the VIEW
spark.table('final_table').show(truncate=False)

# Compare with the query:
print("Compare with the query")
spark.sql(query).show()

# Compare with using transform function (Recommended in Spark 3.1+)
print("Compare with using transform function (Recommended in Spark 3.1+)")
from pyspark.sql.functions import expr
spark.table("base_table").withColumn("values", expr("transform(values, x -> x+ 1 )")).show()

# Compare using Python UDF (More efficient than using nested SQL)
print("Compare using Python UDF (More efficient than using nested SQL)")
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType

def add_one(arr):
    return [x + 1 for x in arr]

add_one_udf=udf(add_one, ArrayType(IntegerType()))
spark.table("base_table").withColumn("values", add_one_udf("values")).show() # base_table[values] goes into the udf

# End the session
spark.stop()