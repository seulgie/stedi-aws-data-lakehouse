import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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
AmazonS3_node1739192027994 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://seulgie-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1739192027994")

# Script generated for node Filter
Filter_node1739192130105 = Filter.apply(frame=AmazonS3_node1739192027994, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1739192130105")

# Script generated for node Trusted Customer Zone
EvaluateDataQuality().process_rows(frame=Filter_node1739192130105, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739190538259", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomerZone_node1739192181344 = glueContext.getSink(path="s3://seulgie-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1739192181344")
TrustedCustomerZone_node1739192181344.setCatalogInfo(catalogDatabase="seulgie",catalogTableName="customer_trusted")
TrustedCustomerZone_node1739192181344.setFormat("json")
TrustedCustomerZone_node1739192181344.writeFrame(Filter_node1739192130105)
job.commit()