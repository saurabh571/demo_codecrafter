from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
 
def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("EMRComplexJob").getOrCreate()
 
    # Define S3 input/output paths
    customers_path = "s3://s3-test-bucket-307946644079/input/customers.csv"
    transactions_path = "s3://s3-test-bucket-307946644079/input/transactions.csv"
    output_path = "s3://s3-test-bucket-307946644079/output/high_value_customers"
 
    # Read Customers CSV file
    customers_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(customers_path)
    )
 
    # Read Transactions CSV file
    transactions_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(transactions_path)
    )
 
    # Perform an inner join on "customer_id"
    joined_df = customers_df.join(transactions_df, "customer_id", "inner")
 
    # Aggregate total spending per customer
    spending_df = (
        joined_df.groupBy("customer_id", "name", "age")
        .agg(sum("amount").alias("total_spent"))
    )
 
    # Filter customers who spent more than $5000
    high_value_customers_df = spending_df.filter(col("total_spent") > 5000)
 
    # Write results back to S3 in Parquet format
    high_value_customers_df.write.format("parquet").mode("overwrite").save(output_path)
 
    # Stop Spark session
    spark.stop()
 
if __name__ == "__main__":
    main()