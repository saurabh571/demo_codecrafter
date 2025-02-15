import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from sagemaker.pyspark.estimator import SageMakerEstimator
from sagemaker.session import Session
 
# Step 1: Initialize Spark Session
spark = SparkSession.builder \
  .appName("PySpark-SageMaker-Regression") \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .getOrCreate()
 
# Step 2: Set S3 bucket details
s3_bucket = "your-bucket-name" # Change this
input_data_path = f"s3a://
{
s3_bucket
}
/input/housing.csv"
output_model_path = f"s3a://
{
s3_bucket
}
/output/housing_model"
 
# Step 3: Read dataset from S3
df = spark.read.csv(input_data_path, header=True, inferSchema=True)
 
# Step 4: Data Processing
# Define input features (excluding 'medv' which is the target)
feature_columns = [col for col in df.columns if col != "medv"]
 
# Convert features into a single vector column
vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df = vector_assembler.transform(df).select("features", col("medv").alias("label"))
 
# Step 5: Split data into training and test sets
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
 
# Step 6: Train Linear Regression Model
lr = LinearRegression(featuresCol="features", labelCol="label", maxIter=100)
model = lr.fit(train_data)
 
# Step 7: Evaluate Model
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="label", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
 
# Step 8: Save model to S3
model.write().overwrite().save(output_model_path)
 
# Step 9: Set up SageMaker Estimator
sagemaker_session = Session(boto3.Session())
 
sagemaker_estimator = SageMakerEstimator(
  trainingImage="763104351884.dkr.ecr.us-east-1.amazonaws.com/sagemaker-sparkml-serving:3.1", # Update region if needed
  trainingInstanceType="ml.m5.large",
  trainingInstanceCount=1,
  trainingSparkDataFrame=df,
  sagemakerSession=sagemaker_session
)
 
# Step 10: Train the model in SageMaker
sagemaker_estimator.fit()
 
# Stop Spark session
spark.stop()
