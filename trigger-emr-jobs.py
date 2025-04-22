import os
import time
import uuid
import json
import boto3
import sys

# Logging Helpers
def printdebug(text):
    print(f"[DEBUG - line {str(sys._getframe().f_back.f_lineno)}] {text}")

def printinfo(text):
    print(f"[INFO - line {str(sys._getframe().f_back.f_lineno)}] {text}")

def printcritical(text):
    print(f"[CRITICAL ERROR - line {str(sys._getframe().f_back.f_lineno)}] {text}")
    raise Exception(f"[CRITICAL ERROR - line {str(sys._getframe().f_back.f_lineno)}] {text}")


# Environment Variables
ARTIFACTORY_BUCKET_NAME = os.environ['ARTIFACTORY_BUCKET_NAME']
STAGE_BUCKET_NAME = os.environ['STAGE_BUCKET_NAME']
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")  # Optional
EMR_APPLICATION_ID = os.environ['EMR_APPLICATION_ID']
EMR_EXECUTION_ROLE_ARN = os.environ['EMR_EXECUTION_ROLE_ARN']

# ===========================
# Extract `emr_event` from SQS message
# ===========================
def extract_emr_event(sqs_record):
    try:
        # Step 1: Deserialize outer SQS body
        raw_body = sqs_record['body']
        printdebug(f"Raw body: {raw_body[:100]}...")  # Trimmed for display
        parsed_body = json.loads(raw_body)

        # Step 2: Get `emr_event` section
        emr_event = parsed_body['emr_event']
        printinfo(f"✅ Extracted emr_event successfully")

        return emr_event

    except Exception as e:
        printcritical(f"❌ Failed to parse emr_event from SQS record: {e}")


def check_job_status(client, run_id, applicationId):
    printdebug(f"Checking job status for run ID: {run_id}")
    response = client.get_job_run(applicationId=applicationId, jobRunId=run_id)
    return response['jobRun']['state']


def lambda_handler(event, context):
    try:
        printinfo("Lambda handler invoked")
        printdebug(f"Received event without json.dumps: {event}")
        printdebug(f"Received event: {json.dumps(event)}")

        client = boto3.client("emr-serverless",
                              aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
                              aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
                              region_name=os.getenv("DEV_REGION"))

        for record in event.get("Records", []):
            emr_event = extract_emr_event(record)
            printdebug(f"Received emr_event: {json.dumps(emr_event)}")

            jar = emr_event.get("jar", [])
            spark_submit_parameters = ' '.join(emr_event.get("spark_submit_parameters", []))
            spark_submit_parameters = f'--conf spark.jars={",".join(jar)} {spark_submit_parameters}'

            arguments = emr_event.get("arguments", {})
            job = emr_event.get("job", {})

            JobName = job.get("job_name")
            ApplicationId = job.get("ApplicationId", EMR_APPLICATION_ID)
            ExecutionTime = job.get("ExecutionTime")
            ExecutionArn = job.get("ExecutionArn", EMR_EXECUTION_ROLE_ARN)

            printdebug(f"Preparing entry point arguments for job: {JobName}")
            entryPointArguments = []
            for key, value in arguments.items():
                if key == "hoodie-conf":
                    for hoodie_key, hoodie_value in value.items():
                        entryPointArguments.extend(["--hoodie-conf", f"{hoodie_key}={hoodie_value}"])
                elif isinstance(value, bool):
                    if value:
                        entryPointArguments.append(f"--{key}")
                else:
                    entryPointArguments.extend([f"--{key}", f"{value}"])

            printinfo(f"Submitting job: {JobName}")
            response = client.start_job_run(
                applicationId=ApplicationId,
                clientToken=str(uuid.uuid4()),
                executionRoleArn=ExecutionArn,
                jobDriver={
                    'sparkSubmit': {
                        'entryPoint': "local:///usr/lib/spark/examples/jars/spark-examples.jar",
                        'entryPointArguments': entryPointArguments,
                        'sparkSubmitParameters': spark_submit_parameters
                    },
                },
                executionTimeoutMinutes=600,
                name=JobName
            )

            run_id = response['jobRunId']
            printinfo(f"Job submitted with run ID: {run_id}")

            if job.get("JobStatusPolling") is True:
                polling_interval = 5
                printdebug("Polling job status...")
                while True:
                    status = check_job_status(client, run_id, ApplicationId)
                    printinfo(f"Job status: {status}")
                    if status in ["CANCELLED", "FAILED", "SUCCESS"]:
                        break
                    time.sleep(polling_interval)

            return {
                "statusCode": 200,
                "body": json.dumps(response)
            }

    except Exception as e:
        printcritical(f"An error occurred during job execution: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
