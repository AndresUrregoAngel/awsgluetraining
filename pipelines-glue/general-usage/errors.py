import boto3
import logging

class JobExecutionsError(Exception):
    def __init__(self,message):
        super().__init__(message)
        
        
class ReadingCatalogTables(Exception):
    def __init__(self,message):
        super().__init__(message)
        
        
class notificationsManager:
    def __init__(self):
        self._sns = boto3.client('sns')
       
        
    def sendNotification(self,jobName,tablename,message):
        try:
            self._sns.publish(
            TopicArn="arn:aws:sns:{NAME}",
            Subject=f"ERROR: process error: {tablename} ",
            Message= f"The job process {jobName} for the ingested file {tablename} with the message: {message}"
        )
        except Exception as error:
            logging.error(f" sendNotification: {error}")