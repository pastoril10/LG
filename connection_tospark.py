import configparser
import boto3

config = configparser.ConfigParser()
config.read("config.ini")
acess_key = config["bucket-s3"]["access_key"]
secret_key = config["bucket-s3"]["secret_key"]

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id= acess_key,
    aws_secret_access_key=secret_key
)

for obj in s3.Bucket('lead-generation-data-raw').objects.all():
    print(obj)
