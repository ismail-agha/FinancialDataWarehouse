# --------------
# AWS Services
# --------------

import boto3

# Initialize the Boto3 EC2 client
ec2_client = boto3.client('ec2', region_name='ap-south-1')

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the bucket name and file key
bucket_name = 's3.financial.data.warehouse'

