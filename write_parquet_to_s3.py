import boto3
import configparser

def read_aws_config(file_path='/home/almerco/datalakehouse/jupyter-notebooks/credentials/credentials'):
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read(file_path)

    # Access AWS configuration values
    aws_access_key_id = config.get('aws', 'aws_access_key_id')
    aws_secret_access_key = config.get('aws', 'aws_secret_access_key')
    region = config.get('aws', 'region')

    return aws_access_key_id, aws_secret_access_key, region

def create_boto3_session(access_key_id, secret_access_key, region):
    # Create a boto3 Session with the provided credentials
    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region
    )

    return session

access_key, secret_key, aws_region = read_aws_config()
aws_session = create_boto3_session(access_key, secret_key, aws_region)

print(f"AWS Access Key ID: {access_key}")
print(f"AWS Secret Access Key: {secret_key}")
print(f"AWS Region: {aws_region}")

import pandas as pd
import csv


# Specify your CSV file path
csv_file_path = './tableX.csv'

with open(csv_file_path, 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter='¦', quotechar='"')
    data = list(reader)

# Convert the data to a Pandas DataFrame
df = pd.DataFrame(data[1:], columns=data[0])

# Display the DataFrame
print(df)

# Convert DataFrame to Arrow Table
#table = pa.Table.from_pandas(df)

# Write Arrow Table to Parquet in S3
# pq.write_to_dataset(
#     table,
#     root_path=f's3://{s3_bucket}/{parquet_key}',
#     filesystem='s3',
#     compression='snappy'  # You can choose the compression type
# )

# print(f"Parquet file written to s3://{s3_bucket}/{parquet_key}")


import awswrangler as wr

wr.s3.to_parquet(
    df=df,
    path='s3://bucket_name/table1/my_file.parquet', boto3_session=aws_session
)
