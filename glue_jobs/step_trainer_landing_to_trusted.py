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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1739199599395 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://seulgie-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1739199599395")

# Script generated for node Customer Curated
CustomerCurated_node1739199601617 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://seulgie-lake-house/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1739199601617")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT s.*
FROM s
WHERE s.serialNumber IN (SELECT DISTINCT c.serialnumber FROM c)

'''
SQLQuery_node1739198780195 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":StepTrainerLanding_node1739199599395, "c":CustomerCurated_node1739199601617}, transformation_ctx = "SQLQuery_node1739198780195")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1739198780195, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739197727734", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739197915680 = glueContext.getSink(path="s3://seulgie-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739197915680")
AmazonS3_node1739197915680.setCatalogInfo(catalogDatabase="seulgie",catalogTableName="step_trainer_trusted")
AmazonS3_node1739197915680.setFormat("glueparquet", compression="uncompressed")
AmazonS3_node1739197915680.writeFrame(SQLQuery_node1739198780195)
job.commit()