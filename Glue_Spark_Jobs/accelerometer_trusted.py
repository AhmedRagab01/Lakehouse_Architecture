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

# Script generated for node customer_trusted
customer_trusted_node1741028840851 = glueContext.create_dynamic_frame.from_catalog(database="ragabl4", table_name="customer_trusted", transformation_ctx="customer_trusted_node1741028840851")

# Script generated for node acc_land_table
acc_land_table_node1741103314116 = glueContext.create_dynamic_frame.from_catalog(database="ragabl4", table_name="accelerometer_landing", transformation_ctx="acc_land_table_node1741103314116")

# Script generated for node trusted_acc_join
SqlQuery2767 = '''
SELECT user,timestamp,x,y,z FROM acc_land_table join customer_trusted_table on 
customer_trusted_table.email = acc_land_table.user;
'''
trusted_acc_join_node1741103275014 = sparkSqlQuery(glueContext, query = SqlQuery2767, mapping = {"customer_trusted_table":customer_trusted_node1741028840851, "acc_land_table":acc_land_table_node1741103314116}, transformation_ctx = "trusted_acc_join_node1741103275014")

# Script generated for node write_trusted
write_trusted_node1741028890727 = glueContext.getSink(path="s3://test-ragabola/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="write_trusted_node1741028890727")
write_trusted_node1741028890727.setCatalogInfo(catalogDatabase="ragabl4",catalogTableName="accelerometer_trsuted")
write_trusted_node1741028890727.setFormat("json")
write_trusted_node1741028890727.writeFrame(trusted_acc_join_node1741103275014)
job.commit()