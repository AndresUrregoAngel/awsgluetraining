import pandas 
from awsglue.utils import getResolvedOptions
import sys
import boto3
from databasegw import athenagw
from errors import JobExecutionsError,ReadingCatalogTables
from datetime import datetime

args = getResolvedOptions(sys.argv, ['catalog','process'])


try:
    athena = athenagw(args)
    df = athena.callingtablescontent()
    
    if 'rds' in args['process']:
        athena.storerds(df)
        print("process  rds completed")
    else:
        athena.storeredshift(df)
        print("process redshift completed")

except Exception as error:
    raise JobExecutionsError(error)