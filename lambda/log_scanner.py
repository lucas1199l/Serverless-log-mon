import json
import boto3
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")

# TODO: Replace these with your actual values
DYNAMODB_TABLE_NAME = "LogScanResults"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:651917101295:LogErrorAlerts"

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    # 1. Extract bucket and object key from S3 event
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    print(f"Processing file: bucket={bucket_name}, key={object_key}")

    # 2. Read file content from S3
    log_content = read_s3_object(bucket_name, object_key)

    # 3. Scan for errors
    error_count, error_lines = scan_for_errors(log_content)

    # 4. Write results to DynamoDB
    write_result_to_dynamodb(object_key, error_count, error_lines)

    # 5. Send SNS alert if errors found
    if error_count > 0:
        send_sns_alert(object_key, error_count, error_lines)

    print("Lambda execution complete.")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "file": object_key,
            "error_count": error_count
        })
    }


# -----------------------------
# Helper Functions
# -----------------------------

def read_s3_object(bucket_name, object_key):
    print(f"Reading S3 object: {bucket_name}/{object_key}")
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    content_bytes = response["Body"].read()
    content_str = content_bytes.decode("utf-8", errors="ignore")
    return content_str


def scan_for_errors(log_content):
    lines = log_content.splitlines()
    error_lines = []

    for line in lines:
        if ("ERROR" in line) or ("CRITICAL" in line) or ("FATAL" in line):
            error_lines.append(line)

    error_count = len(error_lines)
    print(f"Found {error_count} error lines.")
    return error_count, error_lines


def write_result_to_dynamodb(object_key, error_count, error_lines):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    timestamp = datetime.utcnow().isoformat()

    item = {
        "FileName": object_key,
        "ScanTimestamp": timestamp,
        "error_count": error_count,
        "has_errors": error_count > 0,
        "sample_errors": error_lines[:5]  # store up to 5 sample lines
    }

    print(f"Writing to DynamoDB: {item}")
    table.put_item(Item=item)


def send_sns_alert(object_key, error_count, error_lines):
    subject = f"Log Monitor Alert: {error_count} errors in {object_key}"

    preview = "\n".join(error_lines[:5])
    message = (
        f"The log file '{object_key}' contains {error_count} error lines.\n\n"
        f"Sample errors:\n{preview}"
    )

    print(f"Sending SNS alert to topic: {SNS_TOPIC_ARN}")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )
