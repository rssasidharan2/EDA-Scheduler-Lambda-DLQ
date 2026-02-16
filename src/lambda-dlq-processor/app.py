import json
import os
import boto3
from datetime import datetime


# Initialize SNS client
sns_client = boto3.client('sns')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')


def lambda_handler(event, context):
    """
    Lambda DLQ Processor function triggered by SQS Event Source Mapping.
    Processes Lambda execution failures from Lambda DLQ and sends SNS notifications.
    """
    print('=' * 80)
    print('🔴 LAMBDA DLQ PROCESSOR - Started')
    print('=' * 80)
    
    record_count = len(event.get('Records', []))
    log_info('Lambda DLQ processor invoked via SQS Event Source Mapping', {'recordCount': record_count})
    
    batch_item_failures = []
    processed_messages = []
    failed_messages = []
    
    for record in event.get('Records', []):
        try:
            print(f'\n📨 Processing Lambda DLQ Message')
            print(f'   Message ID: {record["messageId"]}')
            print(f'   Receipt Handle: {record["receiptHandle"][:50]}...')
            
            # Lambda DLQ messages contain the original event that caused the failure
            # The message structure is:
            # {
            #   "version": "0",
            #   "id": "event-id",
            #   "detail-type": "Scheduled Event",
            #   "source": "aws.scheduler",
            #   ...
            # }
            
            body = record['body']
            print(f'   Raw Body Type: {type(body).__name__}')
            print(f'   Raw Body: {str(body)[:500]}...')
            
            # Parse the message body
            try:
                if isinstance(body, str):
                    message_body = json.loads(body)
                elif isinstance(body, dict):
                    message_body = body
                else:
                    print(f'   ⚠️  Unexpected body type: {type(body).__name__}')
                    message_body = {
                        'id': record['messageId'],
                        'time': datetime.fromtimestamp(int(record['attributes']['SentTimestamp']) / 1000).isoformat() + 'Z',
                        'source': 'Lambda Execution Failure',
                        'detail-type': 'Lambda Execution Failed',
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
                    'source': 'Lambda Execution Failure',
                    'detail-type': 'Lambda Execution Failed',
                    'detail': {
                        'error': f'Failed to parse message: {str(e)}',
                        'rawBody': str(body)[:1000]
                    }
                }
            
            print('\n🔍 Failed Lambda Execution Details:')
            print(f'   Event ID: {message_body.get("id", "N/A")}')
            print(f'   Time: {message_body.get("time", "N/A")}')
            print(f'   Source: {message_body.get("source", "N/A")}')
            print(f'   Detail Type: {message_body.get("detail-type", "N/A")}')
            
            # Extract error information
            detail = message_body.get('detail', {})
            if isinstance(detail, dict):
                error_info = (
                    detail.get('errorMessage') or 
                    detail.get('error') or 
                    detail.get('errorCode') or
                    'Lambda function threw an exception during execution'
                )
            elif isinstance(detail, str):
                # Sometimes detail is a JSON string
                try:
                    detail_obj = json.loads(detail)
                    error_info = detail_obj.get('errorMessage', 'Lambda execution failed')
                except:
                    error_info = f'Lambda execution failed. Detail: {str(detail)[:200]}'
            else:
                error_info = f'Lambda execution failed. Detail: {str(detail)[:200]}'
            
            print(f'   Error: {error_info}')
            
            # Extract additional details
            resources = message_body.get('resources', [])
            schedule_arn = resources[0] if resources else 'N/A'
            region = message_body.get('region', 'N/A')
            account = message_body.get('account', 'N/A')
            
            # Prepare SNS notification
            notification_subject = '🔴 Lambda Execution Failed - EventBridge Scheduler'
            notification_message = f"""
Lambda Execution Failure Alert
===============================

A Lambda function invoked by EventBridge Scheduler has failed during execution.

Failure Details:
----------------
Event ID: {message_body.get('id', 'N/A')}
Time: {message_body.get('time', datetime.utcnow().isoformat() + 'Z')}
Source: {message_body.get('source', 'Lambda Execution')}
Detail Type: {message_body.get('detail-type', 'Lambda Execution Failed')}
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
2. Check CloudWatch Logs for the Lambda function
3. Investigate the root cause of the execution failure
4. Fix the issue in the Lambda code
5. Monitor subsequent executions

This is an automated notification from your Lambda DLQ monitoring system.
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
                        'StringValue': 'Lambda-DLQ'
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
            
            log_info('Lambda DLQ message processed and notification sent', {
                'messageId': record['messageId'],
                'snsMessageId': sns_message_id
            })
            
        except Exception as error:
            print(f'\n❌ Failed to process message: {record["messageId"]}')
            print(f'   Error: {str(error)}')
            
            log_error('Failed to process Lambda DLQ message', error, {'messageId': record['messageId']})
            
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
    print('📊 LAMBDA DLQ PROCESSOR SUMMARY')
    print(f'   Total Messages: {record_count}')
    print(f'   Processed Successfully: {len(processed_messages)}')
    print(f'   Failed: {len(failed_messages)}')
    print('=' * 80 + '\n')
    
    # Return batch item failures for SQS partial batch response
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
