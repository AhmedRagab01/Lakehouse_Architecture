import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node landing_s3
landing_s3_node1740589729476 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://test-ragabola/customer/landing/"], "recurse": True}, transformation_ctx="landing_s3_node1740589729476")

# Script generated for node PrivacyFilter
SqlQuery2783 = '''
select * from landing_s3
where shareWithResearchAsOfDate is not null
'''
PrivacyFilter_node1740589944046 = sparkSqlQuery(glueContext, query = SqlQuery2783, mapping = {"landing_s3":landing_s3_node1740589729476}, transformation_ctx = "PrivacyFilter_node1740589944046")

# Script generated for node Amazon S3
AmazonS3_node1740590076827 = glueContext.getSink(path="s3://test-ragabola/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1740590076827")
AmazonS3_node1740590076827.setCatalogInfo(catalogDatabase="ragabl4",catalogTableName="customer_trusted")
AmazonS3_node1740590076827.setFormat("json")
AmazonS3_node1740590076827.writeFrame(PrivacyFilter_node1740589944046)
job.commit()