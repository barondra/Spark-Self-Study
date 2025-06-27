from pyspark.sql import SparkSession

# Create a Spark Session
spark = (SparkSession
         .builder
         .appName("SparkSQL_JDBC_App")
         .config("spark.jars", "./jars/postgresql-42.7.7.jar")
         .enableHiveSupport()  # Enable Hive support for database and table management
         .getOrCreate()
) # pyspark --conf spark.sql.catalogImplementation=hive

spark.catalog.setCurrentDatabase("learn_spark_db")

# Load from PostgreSQL

# Read Option 1: Loading data from a JDBC source using load method
jdbcDF1 = (spark.read
           .format("jdbc")
           .option("url", "jdbc:postgresql://[IP]:[HOST]/[DATABASE]")
           .option("driver", "org.postgresql.Driver") 
           .option("dbtable", "[SCHEMA].[TABLENAME]")
           .option("user", "********")
           .option("password", "********")
           .load()
           )

jdbcDF1.show(5, truncate=False)


# Read Option 2: Loading data from a JDBC source using jdbc method
jdbcDF2 = spark.read.jdbc(
    "jdbc:postgresql://[IP]]:[HOST]/[DATABASE]",
    "[SCHEMA].[TABLENAME]",
    properties={
        "user": "********",
        "password": "********",
        "driver": "org.postgresql.Driver"})

jdbcDF2.show(5, truncate=False)

# end the session
spark.stop()