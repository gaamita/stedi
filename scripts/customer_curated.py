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

# Script generated for node customer_landing
customer_landing_node1722305207180 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customer_landing_node1722305207180")

# Script generated for node accelerometer_landing
accelerometer_landing_node1722305235068 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1722305235068")

# Script generated for node customer_transform
SqlQuery0 = '''
select distinct otherSource.*
from otherSource
join myDataSource
on otherSource.email = myDataSource.user
where shareWithResearchAsOfDate is not null

'''
customer_transform_node1722305255623 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometer_landing_node1722305235068, "otherSource":customer_landing_node1722305207180}, transformation_ctx = "customer_transform_node1722305255623")

# Script generated for node customer_curated
customer_curated_node1722305368622 = glueContext.write_dynamic_frame.from_catalog(frame=customer_transform_node1722305255623, database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1722305368622")

job.commit()
