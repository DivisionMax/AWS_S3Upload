import logging
import traceback
import yaml
import boto3
import botocore
from s3_aws_client import S3Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = None

with open("config.yml", 'r') as stream:
    try:
        config = yaml.load(stream)
    except yaml.YAMLError as exc:
        logger.error(exc)

AWS_ACCESS_KEY_ID = config['aws_credentials']['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws_credentials']['aws_secret_key_id']
S3_HOST = config['s3']['aws_s3_host']
S3_REGION = config['aws_credentials']['aws_region']

client = S3Client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_HOST, S3_REGION)

filename = 'temporary.txt'

client.upload_file_to_bucket('tupal', filename , filename)

