def read_aws_config(file_path='/jupyter-notebooks/credentials/credentials'):
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

def list_buckets_with_tags(session, match_list, tags_match):
    # Create an S3 client using the session
    s3_client = session.client('s3')

    # List S3 buckets
    response = s3_client.list_buckets()
    buckets = response['Buckets']
    bucket_names_list = []
    print("S3 Buckets and their Tags:")
    for bucket in buckets:
        bucket_name = bucket['Name']

        # Get bucket tags
        tags_response = s3_client.get_bucket_tagging(Bucket=bucket_name)
        tags = tags_response.get('TagSet', [])
        if bucket_name in match_list:
            if tags:
                for tag in tags:
                    if tag['Key']=="project" and tag['Value']==tags_match["project"]:
                        print(f"Bucket: {bucket_name}")
                        bucket_names_list.append(bucket_name)
                        print("Tags:")
                        print(f" - {tag['Key']}: {tag['Value']}")
            else:
                print("No tags found for project: lakehouse.")

    return bucket_names_list


access_key, secret_key, aws_region = read_aws_config()
aws_session = create_boto3_session(access_key, secret_key, aws_region)

print(f"AWS Access Key ID: {access_key}")
print(f"AWS Secret Access Key: {secret_key}")
print(f"AWS Region: {aws_region}")

s3_client = aws_session.client('s3')

bucket_name_match_list=["bucket_name1","bucket_name2"]
tags_match={"project": "lakehouse"}
# List S3 buckets with their respective tags
bucket_names_list= list_buckets_with_tags(aws_session, bucket_name_match_list, tags_match)

bucket_names_list=["bucket_name1","bucket_name2"]



import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def put_policy(bucket, policy):
    """
    Apply a security policy to the bucket. Policies control users' ability
    to perform specific actions, such as listing the objects in the bucket.

    :param policy: The policy to apply to the bucket.
    """
    try:
        bucket.Policy().put(Policy=json.dumps(policy))
        logger.info("Put policy %s for bucket '%s'.", policy, bucket.name)
        
    except ClientError:
        logger.exception("Couldn't apply policy to bucket '%s'.", bucket.name)
        raise

def get_policy(bucket):
    """
    Get the security policy of the bucket.

    :return: The security policy of the specified bucket, in JSON format.
    """
    try:
        policy = bucket.Policy()
        logger.info(
            "Got policy %s for bucket '%s'.", policy.policy, bucket.name
        )
    except ClientError:
        logger.exception("Couldn't get policy for bucket '%s'.", bucket.name)
        raise
    else:
        return json.loads(policy.policy)

def delete_policy(bucket):
    """
    Delete the security policy from the bucket.
    """
    try:
        bucket.Policy().delete()
        logger.info("Deleted policy for bucket '%s'.", bucket.name)
    except ClientError:
        logger.exception(
            "Couldn't delete policy for bucket '%s'.", bucket.name
        )
        raise


import uuid
import json



s3_resource = aws_session.resource('s3')
file_path = 'your_file_path.txt'


# Open the file in write mode ('w' for write)
with open(file_path, 'w') as file:

    for bucket_name in bucket_names_list:
        print(bucket_name)
        bucket = s3_resource.Bucket(bucket_name)
        print(bucket)

        put_policy_desc = {
            "Version": "2012-10-17",
            "Id": str(uuid.uuid1()),
            "Statement": [
                {
                    "Sid": "AllowSSLRequestsOnly",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": [
                        f"arn:aws:s3:::{bucket.name}/*",
                        f"arn:aws:s3:::{bucket.name}",
                    ],
                    "Condition": {
                        "Bool": {
                            "aws:SecureTransport": "false"
                        }
                    }
                }
            ],
        }

        try:
            put_policy(bucket, put_policy_desc)
            policy = get_policy(bucket)
            print(f"Bucket {bucket.name} has policy {json.dumps(policy)}.")
            #delete_policy(bucket)


            # Replace 'your_file_path.txt' with the desired file path and name

            # Iterate through a for loop to generate and save strings
            generated_text = f"Bucket {bucket.name} has policy {json.dumps(policy)}.\n"
            # Write the generated text to the file
            file.write(generated_text)

        except ClientError as error:
            if error.response["Error"]["Code"] == "MalformedPolicy":
                print("*" * 88)
                print(
                    "This demo couldn't set the bucket policy because the principal user\n"
                    "specified in the demo policy does not exist. For this request to\n"
                    "succeed, you must replace the user ARN with an existing AWS user."
                )
                print("*" * 88)
            else:
                raise
