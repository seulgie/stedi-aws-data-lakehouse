import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Customer Trusted
CustomerTrusted_node1739196860476 = glueContext.create_dynamic_frame.from_catalog(database="seulgie", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1739196860476")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1739196864009 = glueContext.create_dynamic_frame.from_catalog(database="seulgie", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1739196864009")

# Script generated for node Join
Join_node1739197024983 = Join.apply(frame1=AccelerometerTrusted_node1739196864009, frame2=CustomerTrusted_node1739196860476, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1739197024983")

# Script generated for node Drop Fields
DropFields_node1739197128804 = DropFields.apply(frame=Join_node1739197024983, paths=["z", "y", "x", "timestamp", "user"], transformation_ctx="DropFields_node1739197128804")

# Script generated for node Drop Duplicates
DropDuplicates_node1739197323164 =  DynamicFrame.fromDF(DropFields_node1739197128804.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1739197323164")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1739197323164, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739196812112", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1739197345817 = glueContext.getSink(path="s3://seulgie-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1739197345817")
CustomerCurated_node1739197345817.setCatalogInfo(catalogDatabase="seulgie",catalogTableName="customer_curated")
CustomerCurated_node1739197345817.setFormat("glueparquet", compression="uncompressed")
CustomerCurated_node1739197345817.writeFrame(DropDuplicates_node1739197323164)
job.commit()