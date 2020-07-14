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

    dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                        connection_options={"path" : f"s3://{args['bucket']}/{args['key']}"},
                                                        format="csv")
    
    dfupdate = dyf.toDF().withColumn("date_update",lit("2020-07-11"))#lit(f"{updatets.year}-{updatets.month}-{updatets.day}"))
    
    
    dyf_output = DynamicFrame.fromDF(dfupdate,glueContext,"dyf_output")
    
   
    
    datasink_output = glueContext.write_dynamic_frame.from_options(frame = dyf_output,connection_type="s3",
                                                        connection_options={"path" : "s3://pipelines-glue-andresurrego/master/shooting",
                                                        "partitionKeys": "date_update" },
                                                        format="parquet")
    
    
    job.commit()

except Exception as error:
    raise JobExecutionsError(error)