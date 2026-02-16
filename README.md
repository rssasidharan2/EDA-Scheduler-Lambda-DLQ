# EventBridge Scheduler with Lambda, SQS DLQ, and SNS Notifications

This pattern demonstrates how to use Amazon EventBridge Scheduler to invoke AWS Lambda functions on a schedule with automatic retry logic, dual dead letter queue (DLQ) handling for both invocation and execution failures, and SNS notifications.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      EventBridge Scheduler                          │
│                       (Every 5 minutes)                             │
└────────────┬────────────────────────────────────┬───────────────────┘
             │                                    │
             │ Invokes (Async)                    │ Invocation Failure
             │                                    │ (after retries)
             ▼                                    ▼
┌─────────────────────────────┐    ┌─────────────────────────────────┐
│   Lambda Function           │    │  EventBridge Scheduler DLQ      │
│   (Scheduled Task)          │    │  (SQS - KMS Encrypted)          │
└────────────┬────────────────┘    └────────────┬────────────────────┘
             │                                   │
             │ Execution Failure                 │ Event Source Mapping
             │ (after Lambda retries)            │
             ▼                                   ▼
┌─────────────────────────────┐    ┌─────────────────────────────────┐
│  Lambda Execution DLQ       │    │  EventBridge DLQ Processor      │
│  (SQS - KMS Encrypted)      │    │  (Lambda)                       │
└────────────┬────────────────┘    └────────────┬────────────────────┘
             │                                   │
             │ Event Source Mapping              │
             ▼                                   │
┌─────────────────────────────┐                 │
│  Lambda DLQ Processor       │                 │
│  (Lambda)                   │                 │
└────────────┬────────────────┘                 │
             │                                   │
             │ Publishes                         │ Publishes
             └───────────────┬───────────────────┘
                             ▼
                ┌─────────────────────────────┐
                │   SNS Topic                 │
                │   (Email Notifications)     │
                └─────────────────────────────┘
```

## How It Works

### Path 1: Lambda Execution Failures
1. **EventBridge Scheduler** invokes the main Lambda function asynchronously every 5 minutes
2. **Main Lambda Function** executes the scheduled task
3. **Lambda Retry**: If the function fails, Lambda's async retry mechanism retries automatically (default: 2 times)
4. **Lambda Execution DLQ**: After Lambda's retries are exhausted, the failed event is sent to Lambda's DLQ (KMS encrypted)
5. **Lambda DLQ Processor**: Triggered automatically via Event Source Mapping when messages arrive
6. **SNS Notification**: Sends detailed failure notification via email

### Path 2: EventBridge Scheduler Invocation Failures
1. **EventBridge Scheduler** attempts to invoke Lambda
2. **Invocation Failure**: Permission errors, throttling, or other invocation-level failures occur
3. **Scheduler Retry**: EventBridge Scheduler retries up to 3 times (configurable)
4. **EventBridge Scheduler DLQ**: After all retries fail, event is sent to EventBridge's DLQ (KMS encrypted)
5. **EventBridge DLQ Processor**: Triggered automatically via Event Source Mapping
6. **SNS Notification**: Sends detailed failure notification via email

## Key Features

- ⏰ **Scheduled Execution**: Runs Lambda function every 5 minutes using EventBridge Scheduler
- 🔄 **Dual Retry Mechanisms**: 
  - Lambda async retries (2 attempts) for execution failures
  - EventBridge Scheduler retries (3 attempts) for invocation failures
- 📬 **Dual Dead Letter Queues**: 
  - Lambda DLQ for execution failures
  - EventBridge Scheduler DLQ for invocation failures
- 🔐 **KMS Encryption**: Both DLQs encrypted with customer-managed KMS key
- 🔒 **Least Privilege IAM**: All permissions scoped to specific resources (no wildcards)
- 🔔 **SNS Notifications**: Automatic email alerts for both types of failures with detailed error information
- ⚡ **Event Source Mapping**: Automatic DLQ processing without polling
- 🧪 **Testable**: Built-in failure simulation for testing the complete flow

## Requirements

- AWS CLI configured with appropriate credentials
- AWS SAM CLI installed
- Email address for receiving failure notifications

## Deployment

```bash
# Deploy with guided prompts
sam deploy --guided
```

During deployment, you'll be prompted for:
- **Stack name** (e.g., `scheduler-lambda-dlq-sns-demo`)
- **AWS Region** (e.g., `us-east-1`)
- **NotificationEmail**: Email address to receive failure notifications
- Confirmation for IAM role creation
- Confirmation for deploying the changeset

## Post-Deployment Setup

### Confirm SNS Subscription

**CRITICAL STEP**: After deployment, you MUST confirm your SNS subscription to receive failure notifications:

1. Check the email address you provided during deployment
2. Look for an email from AWS Notifications with subject "AWS Notification - Subscription Confirmation"
3. Click the "Confirm subscription" link in the email
4. You should see a confirmation page in your browser

**Without confirming the subscription, you will NOT receive any failure notifications!**

You can verify your subscription status:

```bash
# Get SNS Topic ARN from stack outputs
SNS_TOPIC_ARN=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`SNSTopicArn`].OutputValue' \
  --output text)

# List subscriptions
aws sns list-subscriptions-by-topic --topic-arn ${SNS_TOPIC_ARN}
```

Look for `"SubscriptionArn"` - if it shows `"PendingConfirmation"`, you need to confirm the email. If it shows an actual ARN, you're all set!

## Testing

### Test 1: Normal Execution

The function runs automatically every 5 minutes. View the logs:

```bash
# Get function name from stack outputs
FUNCTION_NAME=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`ScheduledFunctionName`].OutputValue' \
  --output text)

# Tail logs
aws logs tail /aws/lambda/${FUNCTION_NAME} --follow
```

### Test 2: Simulate Lambda Execution Failure

Enable failure simulation to test the Lambda DLQ → Lambda DLQ Processor → SNS flow:

```bash
# Get function name
FUNCTION_NAME=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`ScheduledFunctionName`].OutputValue' \
  --output text)

# Update function to simulate failures
aws lambda update-function-configuration \
  --function-name ${FUNCTION_NAME} \
  --environment 'Variables={LOG_LEVEL=INFO,SIMULATE_FAILURE=true}'
```

Wait for the next scheduled execution (up to 5 minutes). The following will happen:

1. ⏰ EventBridge Scheduler invokes the Lambda function asynchronously
2. ❌ Lambda function fails (simulated exception)
3. 🔄 Lambda's async retry mechanism retries 2 times (each attempt fails)
4. 📬 After Lambda retries exhausted, event is sent to Lambda Execution DLQ (encrypted)
5. ⚡ SQS triggers the Lambda DLQ Processor via Event Source Mapping
6. 📧 Lambda DLQ Processor decrypts message and sends SNS notification
7. ✉️ You receive an email with subject "🔴 Lambda Execution Failed - EventBridge Scheduler"

**Check your email** for the failure notification!

### Test 3: Simulate EventBridge Scheduler Invocation Failure

To test the EventBridge Scheduler DLQ flow, you would need to create an invocation failure (e.g., remove Lambda invoke permission temporarily). This is not recommended for production testing but demonstrates the dual DLQ architecture.

If you do test this scenario, the following will happen:

1. ⏰ EventBridge Scheduler attempts to invoke Lambda
2. ❌ Invocation fails (e.g., permission denied)
3. 🔄 EventBridge Scheduler retries 3 times (each attempt fails)
4. 📬 After 3 failed retries, event is sent to EventBridge Scheduler DLQ (encrypted)
5. ⚡ SQS triggers the EventBridge DLQ Processor via Event Source Mapping
6. 📧 EventBridge DLQ Processor decrypts message and sends SNS notification
7. ✉️ You receive an email with subject "🚨 EventBridge Scheduler Execution Failure Alert"

**Check your email** for the invocation failure notification!

### Test 4: View DLQ Processor Logs

```bash
# Lambda DLQ Processor logs
LAMBDA_DLQ_PROCESSOR=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaDLQProcessorFunctionName`].OutputValue' \
  --output text)

aws logs tail /aws/lambda/${LAMBDA_DLQ_PROCESSOR} --follow

# EventBridge DLQ Processor logs
EB_DLQ_PROCESSOR=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`DLQProcessorFunctionName`].OutputValue' \
  --output text)

aws logs tail /aws/lambda/${EB_DLQ_PROCESSOR} --follow
```

### Test 5: Check DLQ Messages (Optional)

```bash
# Lambda Execution DLQ
LAMBDA_DLQ_URL=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaExecutionDLQUrl`].OutputValue' \
  --output text)

aws sqs get-queue-attributes \
  --queue-url ${LAMBDA_DLQ_URL} \
  --attribute-names ApproximateNumberOfMessages

# EventBridge Scheduler DLQ
EB_DLQ_URL=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`DeadLetterQueueUrl`].OutputValue' \
  --output text)

aws sqs get-queue-attributes \
  --queue-url ${EB_DLQ_URL} \
  --attribute-names ApproximateNumberOfMessages
```

### Test 6: Disable Failure Simulation

Return to normal operation:

```bash
aws lambda update-function-configuration \
  --function-name ${FUNCTION_NAME} \
  --environment 'Variables={LOG_LEVEL=INFO,SIMULATE_FAILURE=false}'
```

## Configuration Options

### Schedule Expression

Modify the schedule in `template.yaml`:

```yaml
ScheduleExpression: rate(5 minutes)  # Every 5 minutes
# OR
ScheduleExpression: cron(0 9 * * ? *)  # Every day at 9 AM UTC
# OR
ScheduleExpression: rate(1 hour)  # Every hour
```

### EventBridge Scheduler Retry Policy

Adjust EventBridge Scheduler retry behavior in `template.yaml`:

```yaml
RetryPolicy:
  MaximumRetryAttempts: 3  # Number of retries (0-185)
  MaximumEventAgeInSeconds: 3600  # Max age of event (60-86400)
```

### Lambda Async Retry Configuration

Lambda's default async retry is 2 attempts. To customize, add `EventInvokeConfig` in `template.yaml`:

```yaml
EventInvokeConfig:
  MaximumRetryAttempts: 2  # 0-2 retries
  MaximumEventAgeInSeconds: 21600  # 60-21600 seconds (6 hours max)
```

### DLQ Retention

Change message retention in `template.yaml`:

```yaml
MessageRetentionPeriod: 1209600  # 14 days (60-1209600 seconds)
```

## Security Features

### KMS Encryption
- Both DLQs (Lambda Execution DLQ and EventBridge Scheduler DLQ) are encrypted using a customer-managed KMS key
- KMS key policy grants least privilege access to EventBridge Scheduler and Lambda services
- All Lambda functions have appropriate KMS decrypt/encrypt permissions

### IAM Least Privilege
- EventBridge Scheduler role: Scoped to specific Lambda function and DLQ
- Lambda execution roles: Scoped to specific SQS queues and KMS key
- DLQ processor roles: Scoped to specific queues, SNS topic, and KMS key
- No wildcard (`*`) resources in production policies

### Resource Policies
- SQS queue policies restrict access to specific EventBridge Scheduler ARN
- SNS topic policy restricts publishing to specific Lambda functions

## Monitoring

### CloudWatch Metrics

Monitor the pattern using CloudWatch:

```bash
# Lambda invocations
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=<function-name> \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum

# Lambda errors
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=<function-name> \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum

# Lambda DLQ errors (messages that failed to send to DLQ)
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name DeadLetterErrors \
  --dimensions Name=FunctionName,Value=<function-name> \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum

# DLQ message count
aws cloudwatch get-metric-statistics \
  --namespace AWS/SQS \
  --metric-name ApproximateNumberOfMessagesVisible \
  --dimensions Name=QueueName,Value=<queue-name> \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average
```

### CloudWatch Alarms

Create alarms for DLQ messages:

```bash
# Alarm for Lambda Execution DLQ
aws cloudwatch put-metric-alarm \
  --alarm-name lambda-execution-dlq-messages \
  --alarm-description "Alert when messages appear in Lambda Execution DLQ" \
  --metric-name ApproximateNumberOfMessagesVisible \
  --namespace AWS/SQS \
  --statistic Average \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=QueueName,Value=<lambda-dlq-name>

# Alarm for EventBridge Scheduler DLQ
aws cloudwatch put-metric-alarm \
  --alarm-name scheduler-dlq-messages \
  --alarm-description "Alert when messages appear in EventBridge Scheduler DLQ" \
  --metric-name ApproximateNumberOfMessagesVisible \
  --namespace AWS/SQS \
  --statistic Average \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=QueueName,Value=<scheduler-dlq-name>
```

## Use Cases

This pattern is ideal for:

- **Scheduled Data Processing**: ETL jobs, data aggregation, report generation
- **Periodic Health Checks**: API monitoring, system health validation
- **Scheduled Notifications**: Daily/weekly email reports, alerts
- **Cleanup Tasks**: Log rotation, temporary file cleanup, cache invalidation
- **Batch Operations**: Bulk updates, scheduled backups, data synchronization
- **Mission-Critical Schedules**: Where both invocation and execution failures must be captured and alerted

## Cost Optimization

- **EventBridge Scheduler**: $1.00 per million invocations
- **Lambda**: Pay per execution (free tier: 1M requests/month)
- **SQS**: First 1M requests/month free, then $0.40 per million
- **KMS**: $1/month per key + $0.03 per 10,000 requests
- **CloudWatch Logs**: $0.50 per GB ingested
- **SNS**: $0.50 per million notifications (first 1,000 free)

For a schedule running every 5 minutes:
- ~8,640 invocations/month
- Estimated cost: ~$2-3/month (including KMS key)

## Troubleshooting

### No Email Notifications Received

1. **Check SNS subscription status**: Ensure you confirmed the subscription email
2. **Check spam folder**: AWS notification emails might be filtered
3. **Verify email in stack outputs**: Ensure correct email was provided during deployment
4. **Check DLQ processor logs**: Verify the processor is being invoked

### Messages Stuck in DLQ

1. **Check DLQ processor logs**: Look for errors in processing
2. **Verify KMS permissions**: Ensure DLQ processor can decrypt messages
3. **Check SNS permissions**: Ensure DLQ processor can publish to SNS topic

### DeadLetterErrors Metric Shows Errors

This indicates Lambda cannot send messages to its DLQ:
1. **Check KMS permissions**: Lambda needs `kms:GenerateDataKey` and `kms:Decrypt`
2. **Check SQS permissions**: Lambda needs `sqs:SendMessage` to the DLQ
3. **Verify DLQ configuration**: Ensure DLQ ARN is correct in Lambda configuration

## Cleanup

Delete the stack and all resources:

```bash
sam delete --stack-name <your-stack-name>
```

Note: The KMS key will be scheduled for deletion (default 30-day waiting period). To delete immediately:

```bash
# Get KMS key ID from stack
KEY_ID=$(aws cloudformation describe-stack-resources \
  --stack-name <your-stack-name> \
  --logical-resource-id DLQEncryptionKey \
  --query 'StackResources[0].PhysicalResourceId' \
  --output text)

# Schedule deletion with minimum waiting period (7 days)
aws kms schedule-key-deletion --key-id ${KEY_ID} --pending-window-in-days 7
```

## Learn More

- [EventBridge Scheduler Documentation](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html)
- [Lambda Error Handling](https://docs.aws.amazon.com/lambda/latest/dg/invocation-retries.html)
- [Lambda Asynchronous Invocation](https://docs.aws.amazon.com/lambda/latest/dg/invocation-async.html)
- [SQS Dead Letter Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [EventBridge Scheduler Retry Policies](https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-schedule-retry.html)
- [KMS Encryption for SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-server-side-encryption.html)

---

Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.

SPDX-License-Identifier: MIT-0
