import boto3
import botocore

class S3Error(Exception):
    """ Exception used when S3 interactions fail"""
    pass

def _get_s3_client():
    return boto3.client('s3')

def _get_s3_resource():
    return boto3.resource('s3')

def key_exists(bucket_name, key_name):
    s3 = _get_s3_resource()

    try:
        s3.Object(bucket_name, key_name).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            exists = False
        else:
            raise e
    else:
        exists = True

    return exists

def download_file_from_s3(bucket_name, key_name, local_file_fqn):
    try:
        client = _get_s3_client()
        if key_exists(bucket_name, key_name):
            client.download_file(bucket_name, key_name, local_file_fqn)
    except (botocore.exceptions.ClientError) as e:
        raise S3Error("Problem downloading meta file from s3: s3://{}/{} to {} [{}]".format(bucket_name, key_name, local_file_fqn, str(e)))
    return