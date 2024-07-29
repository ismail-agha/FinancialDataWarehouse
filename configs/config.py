# --------------
# AWS Services
# --------------

import boto3

# Initialize the Boto3 EC2 client
ec2_client = boto3.client('ec2', region_name='ap-south-1')
ec2_istance_id = 'i-0c63c775026f5c981'

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the bucket name and file key
bucket_name = 's3.financial.data.warehouse'


# --------------
# DB
# --------------
DATABASE_URL = ""

# -----------------------
# Timestamp for logging
# -----------------------

from datetime import datetime

# Global variable to store the timestamp
timestamp = None

# Function to initialize the timestamp at the beginning of execution
def initialize_timestamp():
    global timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Function to get the stored timestamp
def get_timestamp():
    return timestamp