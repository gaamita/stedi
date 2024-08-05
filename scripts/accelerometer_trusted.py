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
customer_trusted_node1722304166832 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1722304166832")

# Script generated for node accelerometer_landing
accelerometer_landing_node1722304218753 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1722304218753")

# Script generated for node accelerometer_transform
SqlQuery0 = '''
select myDataSource.*
from myDataSource
join otherSource
on myDataSource.user = otherSource.email

'''
accelerometer_transform_node1722304231712 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometer_landing_node1722304218753, "otherSource":customer_trusted_node1722304166832}, transformation_ctx = "accelerometer_transform_node1722304231712")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1722304528999 = glueContext.write_dynamic_frame.from_catalog(frame=accelerometer_transform_node1722304231712, database="stedi", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1722304528999")

job.commit()
