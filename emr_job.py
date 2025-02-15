from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SimpleEMRJob").getOrCreate()

# S3 paths (update these)
INPUT_PATH = "s3://s3-test-bucket-307946644079/input/data.csv"
OUTPUT_PATH = "s3://s3-test-bucket-307946644079/output/"

# Read CSV
df = spark.read.option("header", "true").csv(INPUT_PATH)

# Example Transformation: Filter rows where column 'value' > 50
filtered_df = df.filter(df["value"] > 50)

# Write result to S3
filtered_df.write.mode("overwrite").csv(OUTPUT_PATH)

# Stop Spark session
spark.stop()
