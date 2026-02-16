import json
import os
import boto3
from datetime import datetime


# Initialize SNS client
sns_client = boto3.client('sns')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')


def lambda_handler(event, context):
    """
    DLQ Processor Lambda function triggered by SQS Event Source Mapping.
    Processes failed events from DLQ and sends SNS notifications.
    """
    print('=' * 80)
    print('📬 DLQ PROCESSOR LAMBDA - Started')
    print('=' * 80)
    
    record_count = len(event.get('Records', []))
    log_info('DLQ processor invoked via SQS Event Source Mapping', {'recordCount': record_count})
    
    batch_item_failures = []
    processed_messages = []
    failed_messages = []
    
    for record in event.get('Records', []):
        try:
            print(f'\n📨 Processing DLQ Message')
            print(f'   Message ID: {record["messageId"]}')
            print(f'   Receipt Handle: {record["receiptHandle"][:50]}...')
            
            # EventBridge Scheduler sends the entire failed event as the message body
            # The body structure from EventBridge Scheduler DLQ is:
            # {
            #   "version": "0",
            #   "id": "event-id",
            #   "detail-type": "Scheduled Event",
            #   "source": "aws.scheduler",
            #   "account": "account-id",
            #   "time": "timestamp",
            #   "region": "region",
            #   "resources": ["schedule-arn"],
            #   "detail": {}
            # }
            
            body = record['body']
            print(f'   Raw Body Type: {type(body).__name__}')
            print(f'   Raw Body: {str(body)[:500]}...')  # Log first 500 chars
            
            # Parse the message body
            try:
                if isinstance(body, str):
                    message_body = json.loads(body)
                elif isinstance(body, dict):
                    message_body = body
                else:
                    # Unexpected type - create a wrapper
                    print(f'   ⚠️  Unexpected body type: {type(body).__name__}')
                    message_body = {
                        'id': record['messageId'],
                        'time': datetime.fromtimestamp(int(record['attributes']['SentTimestamp']) / 1000).isoformat() + 'Z',
                        'source': 'EventBridge Scheduler',
                        'detail-type': 'Failed Execution',
                        'detail': {
                            'error': f'Unexpected message body type: {type(body).__name__}',
                            'rawBody': str(body)
                        }
                    }
            except (json.JSONDecodeError, TypeError) as e:
                print(f'   ⚠️  Failed to parse message body: {str(e)}')
                message_body = {
                    'id': record['messageId'],
                    'time': datetime.fromtimestamp(int(record['attributes']['SentTimestamp']) / 1000).isoformat() + 'Z',
                    'source': 'EventBridge Scheduler',
                    'detail-type': 'Failed Execution',
                    'detail': {
                        'error': f'Failed to parse message: {str(e)}',
                        'rawBody': str(body)[:1000]
                    }
                }
            
            print('\n🔍 Failed Event Details:')
            print(f'   Event ID: {message_body.get("id", "N/A")}')
            print(f'   Time: {message_body.get("time", "N/A")}')
            print(f'   Source: {message_body.get("source", "N/A")}')
            print(f'   Detail Type: {message_body.get("detail-type", "N/A")}')
            
            # Extract error information from the detail section
            detail = message_body.get('detail', {})
            if isinstance(detail, dict):
                error_info = (
                    detail.get('errorMessage') or 
                    detail.get('error') or 
                    detail.get('errorCode') or
                    'Lambda function execution failed after all retry attempts'
                )
            else:
                error_info = f'Lambda execution failed. Detail: {str(detail)[:200]}'
            
            print(f'   Error: {error_info}')
            
            # Extract additional details
            resources = message_body.get('resources', [])
            schedule_arn = resources[0] if resources else 'N/A'
            region = message_body.get('region', 'N/A')
            account = message_body.get('account', 'N/A')
            
            # Prepare SNS notification
            notification_subject = '🚨 EventBridge Scheduler Execution Failed'
            notification_message = f"""
EventBridge Scheduler Execution Failure Alert
=============================================

A scheduled Lambda execution has failed after all retry attempts.

Failure Details:
----------------
Event ID: {message_body.get('id', 'N/A')}
Time: {message_body.get('time', datetime.utcnow().isoformat() + 'Z')}
Source: {message_body.get('source', 'EventBridge Scheduler')}
Detail Type: {message_body.get('detail-type', 'N/A')}
Region: {region}
Account: {account}

Schedule Information:
---------------------
Schedule ARN: {schedule_arn}

Error Information:
------------------
{error_info}

Original Event:
---------------
{json.dumps(message_body, indent=2)}

SQS Message Details:
--------------------
Message ID: {record['messageId']}
Sent Timestamp: {datetime.fromtimestamp(int(record['attributes']['SentTimestamp']) / 1000).isoformat()}Z
Approximate Receive Count: {record['attributes']['ApproximateReceiveCount']}

Action Required:
----------------
1. Review the error details above
2. Check CloudWatch Logs for the scheduled Lambda function
3. Investigate the root cause of the failure
4. Fix the issue and monitor subsequent executions

This is an automated notification from your EventBridge Scheduler monitoring system.
"""
            
            print('\n📤 Sending SNS Notification...')
            
            # Publish to SNS
            response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=notification_subject,
                Message=notification_message,
                MessageAttributes={
                    'event-id': {
                        'DataType': 'String',
                        'StringValue': message_body.get('id', 'unknown')
                    },
                    'severity': {
                        'DataType': 'String',
                        'StringValue': 'ERROR'
                    },
                    'source': {
                        'DataType': 'String',
                        'StringValue': 'EventBridge-Scheduler-DLQ'
                    }
                }
            )
            
            sns_message_id = response['MessageId']
            print(f'   ✅ SNS Notification sent successfully')
            print(f'   Message ID: {sns_message_id}')
            
            processed_messages.append({
                'messageId': record['messageId'],
                'snsMessageId': sns_message_id,
                'status': 'success'
            })
            
            log_info('DLQ message processed and notification sent', {
                'messageId': record['messageId'],
                'snsMessageId': sns_message_id
            })
            
        except Exception as error:
            print(f'\n❌ Failed to process message: {record["messageId"]}')
            print(f'   Error: {str(error)}')
            
            log_error('Failed to process DLQ message', error, {'messageId': record['messageId']})
            
            # Add to batch item failures for SQS to retry
            batch_item_failures.append({
                'itemIdentifier': record['messageId']
            })
            
            failed_messages.append({
                'messageId': record['messageId'],
                'error': str(error),
                'status': 'failed'
            })
    
    print('\n' + '=' * 80)
    print('📊 DLQ PROCESSOR SUMMARY')
    print(f'   Total Messages: {record_count}')
    print(f'   Processed Successfully: {len(processed_messages)}')
    print(f'   Failed: {len(failed_messages)}')
    print('=' * 80 + '\n')
    
    # Return batch item failures for SQS partial batch response
    # Messages in this list will be retried by SQS
    return {
        'batchItemFailures': batch_item_failures
    }


def log_info(message, data=None):
    """Log informational message in JSON format"""
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'level': 'INFO',
        'message': message
    }
    if data:
        log_entry.update(data)
    print(json.dumps(log_entry))


def log_error(message, error, data=None):
    """Log error message in JSON format"""
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'level': 'ERROR',
        'message': message,
        'error': str(error),
        'errorType': type(error).__name__
    }
    if data:
        log_entry.update(data)
    print(json.dumps(log_entry))
