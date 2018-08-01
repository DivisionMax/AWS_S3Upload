import logging
import traceback
import yaml
import boto3
import botocore

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

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION,
)

s3 = session.resource('s3')


def download_file_from_bucket(bucket, filepath):
    """
    Download a file relative to a bucket
    File-path must include a leading forward slash
    :param bucket:
    :param filepath:
    :return:
    """
    try:
        s3.Bucket(bucket).download_file(filepath, 'bloop.csv')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


def upload_file_to_bucket(bucket, local_file, destination):

    try:
        data = open(local_file, 'rb')
        s3.Bucket(bucket).put_object(Key=destination, Body=data)
    except botocore.exceptions.ClientError as e:
        traceback.print_exc()


upload_file_to_bucket('tupal', 'config_template.yml', 'config_template.yml')
upload_file_to_bucket('tupal', 'config_template.yml', 'blip/config_template.yml')

