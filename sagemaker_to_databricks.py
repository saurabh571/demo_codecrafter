from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySpark-Databricks-Regression") \
    .getOrCreate()

# Step 2: Set local file path and Delta table path
output_table_name = "housing_predictions"  # Delta table name
model_name = "housing_model" # Registered model name

# Step 2: Set local data paths and Delta table name
input_data_path = "file:///Workspace/Shared/sagemaker_to_databricks/BostonHousing.csv"  # Assuming data is in CSV format

# Step 3: Read dataset from local file
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

# Step 8: Save model to Delta table
predictions.write.mode("overwrite").saveAsTable(output_table_name)

# Step 9: MLflow integration
with mlflow.start_run() as run:
    # Log parameters
    mlflow.log_param("maxIter", 100)

    # Log model
    signature = infer_signature(train_data.toPandas().drop("label", axis=1), predictions.toPandas().drop("label", axis=1))
    mlflow.spark.log_model(model, "linear_regression_model", signature=signature)

    # Log metrics
    mlflow.log_metric("rmse", rmse)

    # Register the model
    model_uri = f'runs:/{run.info.run_id}/linear_regression_model'
    mlflow.register_model(model_uri, "housing_price_model")

# Stop Spark session
spark.stop()