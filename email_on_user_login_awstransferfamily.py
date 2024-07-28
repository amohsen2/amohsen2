import gzip
import json
import base64
from datetime import datetime
import boto3

def lambda_handler(event, context):
    cw_data = event['awslogs']['data']
    compressed_payload = base64.b64decode(cw_data)
    uncompressed_payload = gzip.decompress(compressed_payload)
    payload = json.loads(uncompressed_payload)

    log_events = payload['logEvents']
    log_messages = []
    for log_event in log_events:
        log_message = log_event['message']
        log_data = json.loads(log_message)
        if isinstance(log_data, str):
            log_data = json.loads(base64.b64decode(log_data).decode('utf-8'))
        username = log_data.get('user', 'Unknown User')
        timestamp = log_event['timestamp']
        timestamp_utc = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
        message = f"The user '{username}' has logged in at {timestamp_utc} UTC"
        log_messages.append(message)

    sns = boto3.client('sns')
    topic_arn = 'arn:aws:sns:us-east-1:1234567890:snstopic'  # Replace with your SNS topic ARN
    sns.publish(
        TopicArn=topic_arn,
        Message=''.join(log_messages),
        Subject='Login Events'
    )

    response = ''.join(log_messages) + ' , Logs processed and published to SNS'
    
    return response