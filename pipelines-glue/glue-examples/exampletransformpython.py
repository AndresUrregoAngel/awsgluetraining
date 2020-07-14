import pandas 
from awsglue.utils import getResolvedOptions
import sys
import pandas as pd
import awswrangler as wr
import boto3
from errors import JobExecutionsError
from datetime import datetime


args = getResolvedOptions(sys.argv, ['key','bucket']) 


class s3reader:
    def __init__(self):
        self._s3 = boto3.resource('s3')
    
    def readfilecsv(self,args):
        try:
            
            updatets = datetime.now()
            df = wr.s3.read_csv([f"s3://{args['bucket']}/{args['key']}"])
            # obj = self._s3.Object(args['bucket'], args['key'])
            # content = obj.get()['Body'].read().decode('cp1252') 
            # parsecontent = content.split('\n')
            # headers = [ h for h in parsecontent[0].split(',')]
            # headers.append("col")
            # lstcontent = []
                        
            # for line in content.split('\n'):
            #     if "id" not in line:
            #         col = line.split(',')
            #         lstcontent.append(col)
         
            # df = pd.DataFrame(lstcontent,columns=headers)
            # df.drop('col',axis=1)
            df['date_update'] = f"{updatets.year}-{updatets.month}-{updatets.day}"
         
           
            return df
        except Exception as error:
            raise JobExecutionsError(error)
            
    def storefile(self,args,df):
        try:
            
            wr.s3.to_parquet(
            df=df,
            path=f"s3://{args['bucket']}/master/shooting",
            mode="append",
            dataset=True,
            partition_cols=["date_update"]
            )
            
        except Exception as error:
            raise JobExecutionsError(f"storefile: {error}")

        
   
try:
    s3gw = s3reader()
    df = s3gw.readfilecsv(args)
    s3gw.storefile(args,df)
    
    
except Exception as error:
    raise JobExecutionsError(error)