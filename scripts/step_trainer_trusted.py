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
customer_curated_node1722307442852 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1722307442852")

# Script generated for node step_trainer_landing
step_trainer_landing_node1722307428733 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1722307428733")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct otherSource.*
from otherSource
join myDataSource
on otherSource.serialNumber = myDataSource.serialNumber

'''
SQLQuery_node1722307480866 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_curated_node1722307442852, "otherSource":step_trainer_landing_node1722307428733}, transformation_ctx = "SQLQuery_node1722307480866")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1722308453235 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1722307480866, database="stedi", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="step_trainer_trusted_node1722308453235")

job.commit()
