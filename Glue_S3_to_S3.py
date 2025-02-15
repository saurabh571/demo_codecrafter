import sys

from awsglue.transforms import *

from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from awsglue.context import GlueContext

from awsglue.job import Job

from awsgluedq.transforms import EvaluateDataQuality

 

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)

 

# Default ruleset used by all target nodes with data quality enabled

DEFAULT_DATA_QUALITY_RULESET = """

    Rules = [

        ColumnCount > 0

    ]

"""

 

# Script generated for node Amazon S3

AmazonS3_node1738959240896 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://s3-test-bucket-307946644079/industry.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1738959240896")

 

# Script generated for node Change Schema

ChangeSchema_node1738959393778 = ApplyMapping.apply(frame=AmazonS3_node1738959240896, mappings=[("industry", "string", "industry_target", "string")], transformation_ctx="ChangeSchema_node1738959393778")

 

# Script generated for node Amazon S3

EvaluateDataQuality().process_rows(frame=ChangeSchema_node1738959393778, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738959238337", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

AmazonS3_node1738959426911 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1738959393778, connection_type="s3", format="glueparquet", connection_options={"path": "s3://s3-test-bucket-307946644079/output-dir/industry_curated/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1738959426911")

 

job.commit()