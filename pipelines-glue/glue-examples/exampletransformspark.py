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
args = getResolvedOptions(sys.argv, ['JOB_NAME','bucket','key'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



try:
    
    updatets = datetime.now()
    
    fields = [ StructField("id",IntegerType()),StructField("name",StringType()),StructField("date",StringType()),StructField("manner_of_death",StringType()),
               StructField("armed",StringType()),StructField("age",IntegerType()),StructField("gender",StringType()),StructField("race",StringType()),
               StructField("city",StringType()),StructField("state",StringType()),StructField("signs_of_mental_illness",StringType()),StructField("threat_level",StringType()),
               StructField("flee",StringType()),StructField("body_camera",StringType())]
    
    
    schema = StructType(fields)
    
    df = spark.read.csv(f"s3://{args['bucket']}/{args['key']}",header=True,schema=schema,sep=",")
    
    dfc = df.withColumn("date_update", lit(f"{updatets.year}-{updatets.month}-{updatets.day}"))
   
    
    dfc.write.partitionBy('date_update').parquet(f"s3://{args['bucket']}/master/shooting",partitionBy='date_update')

    
    job.commit()

except Exception as error:
    raise JobExecutionsError(error)