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

# Script generated for node step_trusted
step_trusted_node1741028840851 = glueContext.create_dynamic_frame.from_catalog(database="ragabl4", table_name="step_trainer_trusted", transformation_ctx="step_trusted_node1741028840851")

# Script generated for node acc_trusted
acc_trusted_node1741103314116 = glueContext.create_dynamic_frame.from_catalog(database="ragabl4", table_name="accelerometer_trsuted", transformation_ctx="acc_trusted_node1741103314116")

# Script generated for node join_ml_curated
SqlQuery2864 = '''
SELECT * FROM step_trainer_trusted join 
accelerometer_trsuted on 
step_trainer_trusted.sensorReadingTime 
=  accelerometer_trsuted.timestamp;
'''
join_ml_curated_node1741103275014 = sparkSqlQuery(glueContext, query = SqlQuery2864, mapping = {"step_trainer_trusted":step_trusted_node1741028840851, "accelerometer_trsuted":acc_trusted_node1741103314116}, transformation_ctx = "join_ml_curated_node1741103275014")

# Script generated for node write_ml_curated
write_ml_curated_node1741028890727 = glueContext.getSink(path="s3://test-ragabola/step_trainer/ml_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="write_ml_curated_node1741028890727")
write_ml_curated_node1741028890727.setCatalogInfo(catalogDatabase="ragabl4",catalogTableName="machine_learning_curated")
write_ml_curated_node1741028890727.setFormat("json")
write_ml_curated_node1741028890727.writeFrame(join_ml_curated_node1741103275014)
job.commit()