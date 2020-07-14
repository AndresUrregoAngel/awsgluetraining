from awsglue.transforms import *  
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from awsglue.job import Job
# both python shell and spark
from awsglue.utils import getResolvedOptions
import sys
import logging
from errors import JobExecutionsError
from datetime import datetime


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','TempDir','process'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



try:
    
    
    df = spark.read.parquet("s3://pipelines-glue-andresurrego/master/shooting/date_update=2020-7-13/*.parquet")
    
    if 'rds' in args['process']:
        df.write .format("jdbc") \
        .option("url", "jdbc:mysql://namecluster.us-west-2.rds.amazonaws.com:3306/databasename") \
        .option("dbtable", "tbl_sparkv2_shooting") \
        .option("user", "") \
        .option("password", "") \
        .save()
        
    else:
        df.write .format("jdbc") \
        .option("url", "jdbc:redshift://clustername.us-west-2.redshift.amazonaws.com:5439/databasename") \
        .option("dbtable", "public.tbl_sparkv2_shooting") \
        .option("user", "") \
        .option("password", "") \
        .save()
        
except Exception as error:
    raise JobExecutionsError(error)