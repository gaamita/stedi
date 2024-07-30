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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1722302717633 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1722302717633")

# Script generated for node customer_transform
SqlQuery0 = '''
select * from myDataSource where sharewithresearchasofdate is not null

'''
customer_transform_node1722302930521 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1722302717633}, transformation_ctx = "customer_transform_node1722302930521")

# Script generated for node customer_trusted
customer_trusted_node1722302991723 = glueContext.write_dynamic_frame.from_catalog(frame=customer_transform_node1722302930521, database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1722302991723")

job.commit()
