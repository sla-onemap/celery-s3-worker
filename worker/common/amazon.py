"""
@author: Weijian
"""
import boto3

SEARCH_REQUEST_QUEUE = "rds-SearchRequestQueue"

def connect_search_queue():
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=SEARCH_REQUEST_QUEUE)
    return queue


def save_to_s3(user_id, filepath):
    s3 = boto3.resource('s3')
    rpd = s3.Bucket('rpd-files')
    key = "export/%s/%s" % (user_id, filepath)
    rpd.put_object(Key=key, Body=open(filepath, 'rb'))
