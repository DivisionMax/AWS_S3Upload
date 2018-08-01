import logging
import traceback
import boto3
import botocore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Client():

    def __init__(self, access_key, secret_key, host, region):
        self.AWS_ACCESS_KEY_ID = access_key
        self.AWS_SECRET_ACCESS_KEY = secret_key
        self.S3_HOST = host
        self.S3_REGION = region

        try:
            session = boto3.Session(
                aws_access_key_id=self.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
                region_name=self.S3_REGION,
            )

            self.s3 = session.resource('s3')
        except:
            raise Exception('Bad AWS S3 credentials')

    def download_file_from_bucket(self, bucket, filepath):
        """
        Download a file relative to a bucket
        File-path must include a leading forward slash
        :param bucket:
        :param filepath:
        :return:
        """
        try:
            self.s3.Bucket(bucket).download_file(filepath, 'bloop.csv')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def upload_file_to_bucket(self, bucket, local_file, destination):

        try:
            data = open(local_file, 'rb')
            self.s3.Bucket(bucket).put_object(Key=destination, Body=data)
        except botocore.exceptions.ClientError as e:
            traceback.print_exc()

