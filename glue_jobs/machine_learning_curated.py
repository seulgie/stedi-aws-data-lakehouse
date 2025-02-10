import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1739200770871 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://seulgie-lake-house/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1739200770871")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1739200773873 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://seulgie-lake-house/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1739200773873")

# Script generated for node Join
Join_node1739200981051 = Join.apply(frame1=AccelerometerTrusted_node1739200773873, frame2=StepTrainerTrusted_node1739200770871, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1739200981051")

# Script generated for node SQL Query
SqlQuery0 = '''
select
    sensorReadingTime,
    serialNumber,
    distanceFromObject,
    user,
    x,
    y,
    z
from myDataSource

'''
SQLQuery_node1739201032376 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1739200981051}, transformation_ctx = "SQLQuery_node1739201032376")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1739201032376, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739200505138", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739201098729 = glueContext.getSink(path="s3://seulgie-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739201098729")
AmazonS3_node1739201098729.setCatalogInfo(catalogDatabase="seulgie",catalogTableName="step_trainer_curated")
AmazonS3_node1739201098729.setFormat("glueparquet", compression="uncompressed")
AmazonS3_node1739201098729.writeFrame(SQLQuery_node1739201032376)
job.commit()