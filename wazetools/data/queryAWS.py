import boto3
import time
import datetime
from datetime import datetime as dt
from datetime import timedelta as td
from io import StringIO
import pandas as pd

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + td(n)

def date_filter(start_date: datetime.date, end_date: datetime.date, hour_interval: tuple=(0,23)) -> str:
    '''(datetime.date, datetime.date, tuple(int, int)) -> list(tuple)
    
    Return a string which is a sql date conditional for data partioned by year, month, day and hour. 
    It filters data from start_date to end_date (inclusive) in the specified hour_interval.
    
    '''
    return 'OR '.join(
        [f"(year={d.year} AND month={d.month} AND day={d.day} AND hour BETWEEN {hour_interval[0]} AND {hour_interval[1]})\n" + " "*18 
        for d in daterange(start_date, end_date + td(days=1))]).rstrip()

def wait_query(query_id, athena):
    waiting = True
    while waiting:
        response = athena.get_query_execution(QueryExecutionId=query_id)
        state = response['QueryExecution']['Status']['State']
        
        print('Waiting')
        
        if state == 'SUCCEEDED':
            waiting = False
            print('Athena query is Done')
        elif state == 'FAILED' or state == 'CANCELLED':
            raise Exception('Failed to process Athena query')
        else:
            time.sleep(10)
            
def get_file(bucket, key):
    
    s3 = boto3.client('s3')
    
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    return content

def get_df(bucket, key):
    
    return pd.read_csv(StringIO(get_file(bucket, key)))

def query_athena(bucket, prefix, database, template_fp, workgroup, **template_args):
    
    athena = boto3.client('athena')
    
    S3_OUTPUT = "s3://" + bucket +  "/" + prefix
    
    with open(template_fp, 'r') as template:
        query = template.read()
    
    template_args['date_filter'] = date_filter(start_date=template_args['start_date'], end_date=template_args['end_date'])
    
    query = query.format(**template_args)
    
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': S3_OUTPUT},
        WorkGroup=workgroup)
    
    wait_query(response['QueryExecutionId'], athena)
    
    return get_df(bucket, prefix + response['QueryExecutionId'] + '.csv')

def query_aws(template_fp, s3output_bucket='athena-fgv', s3output_prefix='', database='cities', workgroup='primary', **template_args):
    
    return query_athena(bucket=s3output_bucket, prefix=s3output_prefix, database=database, 
                        template_fp=template_fp, workgroup=workgroup, **template_args)
    