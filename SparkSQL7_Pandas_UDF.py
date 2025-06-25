from pyspark.sql import SparkSession
import pandas as pd

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQL_Pandas_UDF_App")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
) # pyspark --conf spark.sql.catalogImplementation=hive

spark.catalog.setCurrentDatabase("learn_spark_db")

# Step 1: Declare the local cubed function
def cubed(a: pd.Series) -> pd.Series:
    return a*a*a

# Step 2: Create the vectorized pandas UDF for the cubed function
cubed_udf = pandas_udf(cubed, returnType=LongType())

# local Pandas function execution
# Create a Pandas Series
x = pd.Series([1, 2, 3])

# the function for a pandas_udf executed with local pandas data
print(cubed(x))

# Switch to a Spark DataFrame and execute as Spark vectorized UDF
# Create a Spark DataFrame
df = spark.range(1, 4)

# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()

# End the session
spark.stop()