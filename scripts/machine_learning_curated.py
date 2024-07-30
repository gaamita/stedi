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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1722309365264 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1722309365264")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1722309396178 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1722309396178")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from otherSource
left join myDataSource
on otherSource.sensorReadingTime = myDataSource.timestamp
'''
SQLQuery_node1722309415652 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometer_trusted_node1722309396178, "otherSource":step_trainer_trusted_node1722309365264}, transformation_ctx = "SQLQuery_node1722309415652")

# Script generated for node machine_learning_curated
machine_learning_curated_node1722309675920 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1722309415652, database="stedi", table_name="machine_learning_curated", transformation_ctx="machine_learning_curated_node1722309675920")

job.commit()
