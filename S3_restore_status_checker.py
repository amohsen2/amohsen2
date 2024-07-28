import csv
from datetime import datetime
import boto3

# For IAM USERS:

# session = boto3.Session(
#     aws_access_key_id='YOUR_ACCESS_KEY',
#     aws_secret_access_key='YOUR_SECRET_KEY',
#     region_name='us-west-1'  # Replace with your desired region
# )

# S3 client
# s3_client = session.client('s3')

# Create an S3 client
s3_client = boto3.client('s3')

# Specify the bucket name and prefix
bucket_name = 'bucketname'
prefix_name = 'folder/'

# Create a list to store the object details
object_details = []

# Function to retrieve object details
def get_object_details(response):
    if 'Contents' in response:
        for obj in response['Contents']:
            key_name = obj['Key']
            if 'RestoreStatus' in obj:
                restore_info = obj['RestoreStatus']
                if 'IsRestoreInProgress' in restore_info and restore_info['IsRestoreInProgress']:
                    restore_status = 'In progress'
                    restore_expiry_date = 'N/A'
                elif 'RestoreExpiryDate' in restore_info:
                    restore_status = 'Completed'
                    restore_expiry_date_str = str(restore_info['RestoreExpiryDate'])
                    restore_expiry_date = datetime.strptime(restore_expiry_date_str, "%Y-%m-%d %H:%M:%S%z")
                else:
                    restore_status = 'Not applicable'
                    restore_expiry_date = 'N/A'
            else:
                restore_status = 'Not applicable'
                restore_expiry_date = 'N/A'

            object_details.append([key_name, restore_status, restore_expiry_date])

# Paginate through the objects
paginator = s3_client.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix_name, OptionalObjectAttributes=['RestoreStatus'])

for page in page_iterator:
    get_object_details(page)

# Check if objects were found
if object_details:
    # Print the object details in CSV format
    with open('object_details.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Key Name', 'Restore Status', 'Restore Expiry Date'])
        writer.writerows(object_details)

    print('CSV file "object_details.csv" created successfully.')
else:
    print('No objects found in the bucket.')

