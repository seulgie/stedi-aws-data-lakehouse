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

# Script generated for node Customer Trusted
CustomerTrusted_node1739195076136 = glueContext.create_dynamic_frame.from_catalog(database="seulgie", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1739195076136")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1739195074453 = glueContext.create_dynamic_frame.from_catalog(database="seulgie", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1739195074453")

# Script generated for node Join
Join_node1739195080086 = Join.apply(frame1=CustomerTrusted_node1739195076136, frame2=AccelerometerLanding_node1739195074453, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1739195080086")

# Script generated for node Drop Fields
SqlQuery0 = '''
SELECT
    user, 
    timestamp,
    x,
    y,
    z
FROM myDataSource
'''
DropFields_node1739195223073 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1739195080086}, transformation_ctx = "DropFields_node1739195223073")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1739195223073, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739195034057", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739195438063 = glueContext.getSink(path="s3://seulgie-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739195438063")
AmazonS3_node1739195438063.setCatalogInfo(catalogDatabase="seulgie",catalogTableName="accelerometer_trusted")
AmazonS3_node1739195438063.setFormat("glueparquet", compression="uncompressed")
AmazonS3_node1739195438063.writeFrame(DropFields_node1739195223073)
job.commit()