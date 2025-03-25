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

# Script generated for node customer_curated
customer_curated_node1741028840851 = glueContext.create_dynamic_frame.from_catalog(database="ragabl4", table_name="customer_curated", transformation_ctx="customer_curated_node1741028840851")

# Script generated for node step_landing_table
step_landing_table_node1741103314116 = glueContext.create_dynamic_frame.from_catalog(database="ragabl4", table_name="step_trainer_landing", transformation_ctx="step_landing_table_node1741103314116")

# Script generated for node trusted_step_trainer
SqlQuery2682 = '''
SELECT 
    step_landing_table.sensorReadingTime, 
    step_landing_table.serialNumber, 
    step_landing_table.distanceFromObject
FROM step_landing_table 
JOIN customer_curated_table 
ON step_landing_table.serialNumber = customer_curated_table.serialNumber;
'''
trusted_step_trainer_node1741103275014 = sparkSqlQuery(glueContext, query = SqlQuery2682, mapping = {"customer_curated_table":customer_curated_node1741028840851, "step_landing_table":step_landing_table_node1741103314116}, transformation_ctx = "trusted_step_trainer_node1741103275014")

# Script generated for node write_step_trusted
write_step_trusted_node1741028890727 = glueContext.getSink(path="s3://test-ragabola/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="write_step_trusted_node1741028890727")
write_step_trusted_node1741028890727.setCatalogInfo(catalogDatabase="ragabl4",catalogTableName="step_trainer_trusted")
write_step_trusted_node1741028890727.setFormat("json")
write_step_trusted_node1741028890727.writeFrame(trusted_step_trainer_node1741103275014)
job.commit()