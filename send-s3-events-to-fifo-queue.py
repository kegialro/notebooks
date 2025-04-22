import json
import boto3
import os
import sys
from io import StringIO

# ============================
# Custom Logging Functions
# ============================
def printdebug(text):
    print(f"[DEBUG - line {str(sys._getframe().f_back.f_lineno)}] {text}")

def printinfo(text):
    print(f"[INFO - line {str(sys._getframe().f_back.f_lineno)}] {text}")

def printcritical(text):
    print(f"[CRITICAL ERROR - line {str(sys._getframe().f_back.f_lineno)}] {text}")
    raise Exception(f"[CRITICAL ERROR - line {str(sys._getframe().f_back.f_lineno)}] {text}")

# ============================
# Initialize AWS Clients
# ============================
sqs = boto3.client('sqs')
s3_client = boto3.client('s3')

# SQS FIFO Queue URL (from environment variables)
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

# S3 Bucket for Hudi Props
ARTIFACTORY_BUCKET_NAME = os.environ['ARTIFACTORY_BUCKET_NAME']
HUDI_PROPS_PREFIX = "props"
STAGE_BUCKET_NAME = os.environ['STAGE_BUCKET_NAME']
EMR_APPLICATION_ID = os.environ['EMR_APPLICATION_ID']
EMR_EXECUTION_ROLE_ARN = os.environ['EMR_EXECUTION_ROLE_ARN']


# ============================
# Function to Extract S3 Folder
# ============================
def extract_message_group_id(s3_key):
    """Extract the first folder from the S3 key."""
    return s3_key.split('/')[1] if '/' in s3_key else 'default_group'

def extract_s3_folder(s3_bucket, s3_key):
    """Construct the S3 folder path excluding the filename."""
    return f"s3://{s3_bucket}/" + '/'.join(s3_key.split('/')[:-1]) + '/'

# ============================
# Function to Update Hudi Props
# ============================
def load_and_flatten_hudi_props(target_table, s3_bucket, s3_key, s3_folder_uri):
    """Load and flatten props file content from S3 into a key-value dictionary."""
    try:
        template_props_key = f"{HUDI_PROPS_PREFIX}/templates/hudi_{target_table}.props"
        printinfo(f"📥 Downloading template Hudi props from s3://{ARTIFACTORY_BUCKET_NAME}/{template_props_key}")
        response = s3_client.get_object(Bucket=ARTIFACTORY_BUCKET_NAME, Key=template_props_key)
        props_content = response['Body'].read().decode('utf-8')

        # Replace placeholder and flatten to dict
        flattened_props = {}
        for line in props_content.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                if "${RAW_BUCKET_NAME_AND_KEY}" in value:
                    value = value.replace("${RAW_BUCKET_NAME_AND_KEY}", f"{s3_bucket}/{s3_key}")
                if "${s3_folder_uri}" in value:
                    value = value.replace("${s3_folder_uri}", s3_folder_uri)
                flattened_props[key.strip()] = value.strip()

        printdebug(f"🔧 Flattened hoodie-conf: {json.dumps(flattened_props)}")
        return flattened_props

    except Exception as e:
        printcritical(f"❌ Failed to load and flatten props file: {e}")

# ============================
# Main Lambda Handler
# ============================
def lambda_handler(event, context):
    """Lambda handler triggered by S3 PUT event to update Hudi props and send event to SQS."""
    try:
        printinfo("🚀 Received S3 Event")

        for record in event['Records']:
            s3_bucket = record['s3']['bucket']['name']
            s3_key = record['s3']['object']['key']
            date_string=s3_key.split(os.path.basename(s3_key))[0].split("/")[-2]
            printdebug(f"S3 Put Event from: {s3_bucket}/{s3_key}")

            # ✅ Step 1: Extract Group ID and Folder Path
            message_group_id = extract_message_group_id(s3_key)
            s3_folder_uri = extract_s3_folder(s3_bucket, s3_key)

            printdebug(f"Extracted MessageGroupId: {message_group_id}")
            printdebug(f"Extracted S3 Folder URI: {s3_folder_uri}")

            # ✅ Step 2: Load and flatten Hudi props
            flattened_props = load_and_flatten_hudi_props(message_group_id, s3_bucket, s3_key, s3_folder_uri)
            target_table = message_group_id
            event = {
                "jar": [
                    f"s3://{ARTIFACTORY_BUCKET_NAME}/jar/hudi-aws-bundle-0.14.1.jar",
                    f"s3://{ARTIFACTORY_BUCKET_NAME}/jar/hudi-utilities-slim-bundle_2.12-0.14.1.jar",
                    f"s3://{ARTIFACTORY_BUCKET_NAME}/jar/hudi-spark3.4-bundle_2.12-0.14.1.jar"
                ],
                "spark_submit_parameters": [
                    "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
                    "--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
                    "--conf spark.sql.hive.convertMetastoreParquet=false",
                    "--conf spark.driver.memory=2g",
                    "--conf spark.executor.memory=3g",
                    "--conf spark.executor.cores=2",
                    "--conf spark.executor.instances=1",
                    "--class org.apache.hudi.utilities.streamer.HoodieStreamer"
                ],
                "arguments": {
                    "table-type": "COPY_ON_WRITE",
                    "op": "UPSERT",
                    "enable-sync": True,#False,
                    "source-ordering-field": "source_timestamp",
                    "source-class": "org.apache.hudi.utilities.sources.ParquetDFSSource",
                    "target-table": f"{target_table}",
                    "target-base-path": f"s3a://{STAGE_BUCKET_NAME}/guay_jocker_db/{target_table}/",
                    "props": f"s3://{ARTIFACTORY_BUCKET_NAME}/props/templates/empty-streamer.props",
                    "sync-tool-classes": "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool",
                    "hoodie-conf": flattened_props
                },
                "job": {
                    "job_name": f"Hudi_0.14.0_{target_table}_{date_string}",
                    "created_by": "Kevin Almerco",
                    "created_at": "2024-03-20",
                    "ApplicationId": EMR_APPLICATION_ID,
                    "ExecutionTime": 600,
                    "JobActive": True,
                    "JobStatusPolling": True,
                    "JobDescription": "Ingest data from parquet source (MySQL data)",
                    "ExecutionArn": EMR_EXECUTION_ROLE_ARN
                }
            }

            # ✅ Step 3: Construct and send message with hoodie-conf
            sqs_message = {
                "s3_folder": s3_folder_uri,
                "target_table": message_group_id,
                "emr_event": event
            }

            # ✅ Step 3: Send Message to SQS FIFO
            response = sqs.send_message(
                QueueUrl=f"{SQS_QUEUE_URL}/sdlf-analytics-{message_group_id}_queue.fifo",
                MessageBody=json.dumps(sqs_message),
                MessageGroupId=message_group_id,
            )

            printinfo(
                f"📤 Sent event to SQS FIFO: "+f"{SQS_QUEUE_URL}/sdlf-analytics-{message_group_id}_queue.fifo\n"
                f"MessageBody: {json.dumps(sqs_message)}\n"
                f"Bucket: {s3_bucket}, Key: {s3_key}\n"
                f"GroupId: {message_group_id}, MessageId: {response['MessageId']}"
            )

        return {
            'statusCode': 200,
            'body': json.dumps('S3 event successfully sent to SQS FIFO and Hudi props updated.')
        }

    except Exception as e:
        printcritical(f"❌ Failed to process S3 event: {e}")
