from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQL_UDF_App")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
) # pyspark --conf spark.sql.catalogImplementation=hive

spark.catalog.setCurrentDatabase("learn_spark_db")

# Create an UDF
def cubed(s):
    return s*s*s

# Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# Query the cubed UDF using SparkSQL API
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# End the session
spark.stop()