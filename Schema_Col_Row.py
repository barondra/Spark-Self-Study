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
    print(blogs_df.printSchema())
    print(blogs_df.schema)

    # Stop the Spark session
    spark.stop()