import boto3
from logging import config
import logging as log
import time
import json
from botocore.exceptions import ClientError

log.getLogger().setLevel(log.INFO)

print('Loading function')

glue = boto3.client('glue')

def etl_run():
    ##### bronze #####
    download_status = source2bronze()
    if download_status == 'SUCCEEDED':
        bronze_status = glue_catalog('bronze-crawler')
        if bronze_status in ['READY', 'STOPPING']:
            print(f'Bronze fully updated!')
            ##### silver #####
            print('Silver updated triggered')
            id = bronze2silver('silver_etl_run')
            # run crawler
            print(f'Silver updated finished: {id}')
    else:
        print(f'ETL not finalized: {download_status}.')


def bronze2silver(lambda_name):
    lambda_client = boto3.client('lambda')
    payload = '{"key": "value"}'
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='Event',
        Payload=payload
    )
    return response['ResponseMetadata']['RequestId']
    

def glue_catalog(crawler_name):
    trigger = glue.start_crawler(Name=crawler_name)
    while True:
        response = glue.get_crawler(Name=crawler_name)
        job_status = response['Crawler']['State']
        # Check status
        if job_status in ['READY', 'STOPPING', 'STOPPED']:
            break
        print('Cralwer not finished, waiting 15 seconds')
        time.sleep(15)
    print(f'Crawler job Finished! Status: {job_status}.')
    return job_status
    

def source2bronze():
    gluejobname="bronze"
    try:
        # trigger glue job
        trigger = glue.start_job_run(JobName=gluejobname)
        while True:
            response = glue.get_job_run(JobName=gluejobname, RunId=trigger['JobRunId'])
            job_status = response['JobRun']['JobRunState']
            # Check status
            if job_status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break
            print('Download not finished, waiting 15 seconds')
            time.sleep(15)
        print(f'Download job Finished! Status: {job_status}')
        
        return job_status

    except Exception as e:
        log.error(f'Error 233: {e}')
    


def lambda_handler(event, context):
    etl_run()

 