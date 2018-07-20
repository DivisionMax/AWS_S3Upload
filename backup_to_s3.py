import os
import sys
from datetime import date
import logging
import traceback
import yaml
import boto
from boto.s3.key import Key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = None

with open("config.yml", 'r') as stream:
    try:
        config = yaml.load(stream)
    except yaml.YAMLError as exc:
        logger.error(exc)

num_of_args = len(sys.argv)

if num_of_args < 2:
    logger.error('No file or folder provided to be uploaded')
    sys.exit(0)

file_or_folder = sys.argv[1]

AWS_ACCESS_KEY_ID = config['aws_credentials']['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws_credentials']['aws_secret_key_id']
S3_HOST = config['s3']['aws_s3_host']
S3_REGION = config['aws_credentials']['aws_region']
BUCKET_NAME = config['s3']['aws_s3_bucket']        
S3_DESTINATION_DIR = config['s3']['aws_s3_bucket_dir']

if num_of_args < 3:
    logger.error('No S3 destination folder provided, the default will be used: {}'.format(S3_DESTINATION_DIR))
else:
    S3_DESTINATION_DIR = sys.argv[2]


def upload_file(filepath):
    # filename = file_or_folder.split('/')[-1]
    try:
        upload_path = os.path.join(S3_DESTINATION_DIR, filepath)
        logger.debug('Uploading file to: {}'.format(filepath))
        k.key = upload_path
        k.set_contents_from_filename(filepath)
    except Exception:
        # Broad Exception to catch if anything goes wrong, internet drops, there is no internet, files are moved etc
        traceback.print_exc()
import os

# Establish a connection to the destination document
try:
    s3 = boto.s3.connect_to_region(S3_REGION,
                                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    bucket = s3.get_bucket(BUCKET_NAME)
    k = Key(bucket)
    if os.path.isdir(file_or_folder):
        # Walk through all the files in the folder and upload everything
        for root, dirs, files in os.walk(file_or_folder, topdown=False):
            for name in files:
                upload_file(os.path.join(root, name))
    else:
        # Upload a single file
        upload_file(file_or_folder)
except Exception:
    # Broad Exception to catch if anything goes wrong, internet drops, there is no internet, files are moved etc
    traceback.print_exc()
