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
    
    df = spark.sql('select * from pipelines.tbl_shooting')
    
    dyf = DynamicFrame.fromDF(df,glueContext,'dyf')
    
    connection_options_redshift ={ "dbtable": "tbl_spark_shooting", "database": "dbname"}
    connection_options_rds ={ "dbtable": "tbl_spark_shooting", "database": "dbname"}                                        
    
    
    if 'redshift' in args['process']:
        datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "redshiftwh-pipelines",
                                                connection_options = connection_options_redshift,
                                                redshift_tmp_dir = args['TempDir'], transformation_ctx = "datasink5")
        print("process  redshift completed")
    
    else:
        datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "jdbcrds",
                                                connection_options = connection_options_rds,
                                                redshift_tmp_dir = args['TempDir'], transformation_ctx = "datasink5")     
                                                
        print("process  rds completed")
    
    job.commit()
    
except Exception as error:
    raise JobExecutionsError(error)