import pandas 
from awsglue.utils import getResolvedOptions
import sys
import boto3
from databasegw import athenagw
from errors import JobExecutionsError,ReadingCatalogTables, notificationsManager
from datetime import datetime


snsgw = notificationsManager()


try:
    a = '1'
    b =2
    print (a+b)    
    

except Exception as error:
    snsgw.sendNotification("practice-failure-job","shooting",error)
    raise JobExecutionsError(error)