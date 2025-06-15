from pyspark.sql import SparkSession

# Define schema for our data using DDL
schemaDDL = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

# Create our static data
data = [[1, "Jules", "Denny", "https://www.julesdenny.com", "2023-10-01", 100, ["twitter", "Linkedin"]],
        [2, "Brooke", "Thompson", "https://www.brookethompson.com", "2023-10-02", 200, ["Google", "Facebook"]],
        [3, "Denny", "Brooke", "https://www.dennybrooke.com", "2023-10-03", 150, ["Facebook", "Twitter"]],
        [4, "Thompson", "Jules", "https://www.thompsonjules.com", "2023-10-04", 250, ["Linkedin", "Google"]],
        [5, "Jules", "Brooke", "https://www.julesbrooke.com", "2023-10-05", 300, ["Twitter", "Facebook"]],
        [6, "Brooke", "Denny", "https://www.brookedenny.com", "2023-10-06", 400, ["Google", "Linkedin"]]]

# Main program
if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("SchemaColRow").getOrCreate()

    # Create a DataFrame using the schema and data
    blogs_df = spark.createDataFrame(data, schemaDDL)

    # Show the DataFrame
    blogs_df.show()

    # Print the schema of the DataFrame
    print(blogs_df.printSchema()) # Print the schema in a tree format
    print(blogs_df.schema) # Print the schema object definition

    # Use an expression to compute big hitters (hits > 200)
    from pyspark.sql.functions import expr
    blogs_df.withColumn("Big Hitters", (expr("Hits > 200"))).show()

    # Concatenate three columns, create a new column, and select it
    from pyspark.sql.functions import concat, col
    blogs_df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(col("AuthorsId")).show()

    # These statements return the same value, showing that expr is the same as col method call
    blogs_df.select(expr("Hits")).show(2)
    blogs_df.select(col("Hits")).show(2)
    blogs_df.select("Hits").show(2)

    # Sort by column "Id" in descending order
    blogs_df.sort(col("Id").desc()).show()
    blogs_df.orderBy(col("Id").desc()).show()
    # blogs_df.sort($"Id".desc()).show()  # Using dollar sign notation for Scala Spark

    # create a new row and add it to the DataFrame    
    from pyspark.sql import Row
    blog_row = Row(Id=7, First="New", Last="Author", Url="https://www.newauthor.com", Published="2023-10-07", Hits=500, Campaigns=["NewCampaign"])
    blogs_df = blogs_df.union(spark.createDataFrame([blog_row], schemaDDL))
    # Show the updated DataFrame with the new row
    blogs_df.show()

    # Stop the Spark session
    spark.stop()