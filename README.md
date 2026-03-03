# EventBridge Scheduler with Lambda and Dual SQS DLQs

This pattern demonstrates the powerful failure handling capabilities of Amazon EventBridge Scheduler by showcasing how it manages both Lambda execution failures and scheduler invocation failures through separate Dead Letter Queues (DLQs).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      EventBridge Scheduler                          │
│                       (Every 5 minutes)                             │
│                                                                     │
│  Features Demonstrated:                                             │
│  • Automatic retry with configurable attempts                      │
│  • Maximum event age configuration                                 │
│  • Separate DLQ for invocation failures                            │
└────────────┬────────────────────────────────────┬───────────────────┘
             │                                    │
             │ Invokes (Async)                    │ Invocation Failure
             │                                    │ (after 3 retries)
             ▼                                    ▼
┌─────────────────────────────┐    ┌─────────────────────────────────┐
│   Lambda Function           │    │  EventBridge Scheduler DLQ      │
│   (Scheduled Task)          │    │  (SQS - No Encryption)          │
└────────────┬────────────────┘    └─────────────────────────────────┘
             │
             │ Execution Failure
             │ (after Lambda retries)
             ▼
┌─────────────────────────────┐
│  Lambda Execution DLQ       │
│  (SQS - No Encryption)      │
└─────────────────────────────┘
```

## EventBridge Scheduler Features Showcased

This pattern highlights the following EventBridge Scheduler capabilities:

### 1. Automatic Retry Mechanism
- Configurable retry attempts (0-185)
- Configurable maximum event age (60-86400 seconds)
- Exponential backoff between retries
- Ensures transient failures don't result in lost executions

### 2. Dead Letter Queue for Invocation Failures

**This is a critical EventBridge Scheduler feature that captures failures at the invocation level.**


#### When Does the EventBridge Scheduler DLQ Receive Messages?

The EventBridge Scheduler DLQ receives messages in the following scenarios:

**🔴 CRITICAL: These are INVOCATION failures, not execution failures**

1. **IAM Permission Errors**
   - Scheduler role lacks `lambda:InvokeFunction` permission
   - Lambda resource policy denies the scheduler
   - Cross-account invocation permission issues
   - **Why it matters**: Configuration errors are caught immediately

2. **Lambda Service Throttling**
   - Lambda concurrent execution limit reached
   - Account-level throttling (TooManyRequestsException)
   - Reserved concurrency exhausted
   - **Why it matters**: Prevents silent data loss during traffic spikes

3. **Lambda Function State Issues**
   - Function is being deleted or updated
   - Function doesn't exist (deleted after schedule creation)
   - Function ARN is invalid
   - **Why it matters**: Captures lifecycle management issues

4. **Resource Not Found**
   - Lambda function deleted but schedule still active
   - Function moved to different region
   - **Why it matters**: Detects configuration drift

5. **Maximum Event Age Exceeded**
   - Event couldn't be delivered within MaximumEventAgeInSeconds (1 hour in this pattern)
   - Retries exhausted before event expiration
   - **Why it matters**: Prevents indefinite retry loops

6. **Maximum Retry Attempts Exhausted**
   - All 3 retry attempts failed (configurable 0-185)
   - Each retry encountered invocation-level errors
   - **Why it matters**: Final safety net for persistent issues


#### ⚠️ IMPORTANT: What the Scheduler DLQ Does NOT Capture

The EventBridge Scheduler DLQ does **NOT** receive messages for:

- Lambda function execution errors (exceptions in your code)
- Lambda function timeouts
- Lambda function out-of-memory errors
- Business logic failures

**These execution failures go to the Lambda Execution DLQ instead.**

#### Why This Dual DLQ Architecture Matters

This pattern demonstrates EventBridge Scheduler's sophisticated failure handling:

- **Separation of Concerns**: Invocation failures vs execution failures are handled separately
- **Operational Visibility**: Different failure types require different remediation strategies
- **No Silent Failures**: Every failure is captured in the appropriate DLQ
- **Debugging Clarity**: Know immediately whether the issue is configuration or code

### 3. Flexible Time Windows
- Configured with `FlexibleTimeWindow: OFF` for precise scheduling
- Can be set to distribute invocations over a time window (up to 15 minutes)
- Helps reduce thundering herd problems at scale

### 4. Schedule Expression Flexibility
- Supports rate expressions: `rate(5 minutes)`, `rate(1 hour)`
- Supports cron expressions: `cron(0 9 * * ? *)` for complex schedules
- Timezone support for business-hour scheduling

## How It Works

### Path 1: Lambda Execution Failures (Code Errors)
1. EventBridge Scheduler invokes Lambda function asynchronously every 5 minutes
2. Lambda function executes but throws an exception (code error, timeout, OOM)
3. Lambda's async retry mechanism retries automatically (default: 2 times)
4. After Lambda retries exhausted → Event sent to Lambda Execution DLQ
5. Messages remain in DLQ for manual inspection or automated processing

### Path 2: EventBridge Scheduler Invocation Failures (Configuration/Service Errors)
1. EventBridge Scheduler attempts to invoke Lambda
2. Invocation fails (permission denied, throttling, function not found, etc.)
3. EventBridge Scheduler retries up to 3 times with exponential backoff
4. After all retries fail → Event sent to EventBridge Scheduler DLQ
5. Messages remain in DLQ for manual inspection or automated processing


## Key Features

- ⏰ **Scheduled Execution**: Runs Lambda every 5 minutes using EventBridge Scheduler
- 🔄 **Dual Retry Mechanisms**: 
  - Lambda async retries (2 attempts) for execution failures
  - EventBridge Scheduler retries (3 attempts) for invocation failures
- 📬 **Dual Dead Letter Queues**: 
  - Lambda DLQ for execution failures (code errors)
  - Scheduler DLQ for invocation failures (configuration/service errors)
- 🔒 **Least Privilege IAM**: All permissions scoped to specific resources
- 🎯 **Simple Architecture**: No encryption, no extra processors - focus on core scheduler features
- 🧪 **Testable**: Built-in failure simulation

## Requirements

- AWS CLI configured with appropriate credentials
- AWS SAM CLI installed

## Deployment

```bash
# Deploy with guided prompts
sam deploy --guided
```

During deployment, you'll be prompted for:
- Stack name (e.g., `eventbridge-scheduler-demo`)
- AWS Region (e.g., `us-east-1`)
- Confirmation for IAM role creation
- Confirmation for deploying the changeset

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

Test the Lambda Execution DLQ:

```bash
# Enable failure simulation
aws lambda update-function-configuration \
  --function-name ${FUNCTION_NAME} \
  --environment 'Variables={LOG_LEVEL=INFO,SIMULATE_FAILURE=true}'
```

Wait up to 5 minutes for the next scheduled execution. The flow:

1. ⏰ EventBridge Scheduler invokes Lambda
2. ❌ Lambda throws exception (simulated)
3. 🔄 Lambda retries 2 times (each fails)
4. 📬 Event sent to Lambda Execution DLQ

Check the DLQ:

```bash
LAMBDA_DLQ_URL=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaExecutionDLQUrl`].OutputValue' \
  --output text)

aws sqs receive-message --queue-url ${LAMBDA_DLQ_URL}
```

Disable failure simulation:

```bash
aws lambda update-function-configuration \
  --function-name ${FUNCTION_NAME} \
  --environment 'Variables={LOG_LEVEL=INFO,SIMULATE_FAILURE=false}'
```


### Test 3: Simulate EventBridge Scheduler Invocation Failure

Test the EventBridge Scheduler DLQ by removing Lambda invoke permission:

```bash
# Get schedule name
SCHEDULE_NAME=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`ScheduleName`].OutputValue' \
  --output text)

# Get current scheduler role ARN
SCHEDULER_ROLE=$(aws scheduler get-schedule \
  --name ${SCHEDULE_NAME} \
  --query 'Target.RoleArn' \
  --output text)

# Remove Lambda invoke permission from scheduler role
POLICY_NAME="InvokeLambda"
aws iam delete-role-policy \
  --role-name $(echo ${SCHEDULER_ROLE} | cut -d'/' -f2) \
  --policy-name ${POLICY_NAME}
```

Wait up to 5 minutes for the next scheduled execution. The flow:

1. ⏰ EventBridge Scheduler attempts to invoke Lambda
2. ❌ Invocation fails (permission denied)
3. 🔄 Scheduler retries 3 times (each fails)
4. 📬 Event sent to EventBridge Scheduler DLQ

Check the Scheduler DLQ:

```bash
SCHEDULER_DLQ_URL=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query 'Stacks[0].Outputs[?OutputKey==`SchedulerDLQUrl`].OutputValue' \
  --output text)

aws sqs receive-message --queue-url ${SCHEDULER_DLQ_URL}
```

**Restore permissions** by redeploying the stack:

```bash
sam deploy
```

### Test 4: Monitor DLQ Message Counts

```bash
# Lambda Execution DLQ
aws sqs get-queue-attributes \
  --queue-url ${LAMBDA_DLQ_URL} \
  --attribute-names ApproximateNumberOfMessages

# Scheduler DLQ
aws sqs get-queue-attributes \
  --queue-url ${SCHEDULER_DLQ_URL} \
  --attribute-names ApproximateNumberOfMessages
```

## Configuration Options

### Schedule Expression

Modify in `template.yaml`:

```yaml
ScheduleExpression: rate(5 minutes)  # Every 5 minutes
# OR
ScheduleExpression: cron(0 9 * * ? *)  # Every day at 9 AM UTC
# OR
ScheduleExpression: rate(1 hour)  # Every hour
```

### EventBridge Scheduler Retry Policy

Adjust retry behavior in `template.yaml`:

```yaml
RetryPolicy:
  MaximumRetryAttempts: 3  # 0-185 retries
  MaximumEventAgeInSeconds: 3600  # 60-86400 seconds (1-24 hours)
```

### DLQ Message Retention

Change retention in `template.yaml`:

```yaml
MessageRetentionPeriod: 1209600  # 14 days (60-1209600 seconds)
```


## Monitoring

### CloudWatch Metrics

```bash
# Lambda invocations
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=${FUNCTION_NAME} \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum

# Lambda errors
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=${FUNCTION_NAME} \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum

# DLQ message counts
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
  --alarm-name lambda-execution-dlq-alarm \
  --alarm-description "Alert when Lambda execution failures occur" \
  --metric-name ApproximateNumberOfMessagesVisible \
  --namespace AWS/SQS \
  --statistic Average \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=QueueName,Value=<your-stack-name>-lambda-dlq

# Alarm for Scheduler DLQ
aws cloudwatch put-metric-alarm \
  --alarm-name scheduler-dlq-alarm \
  --alarm-description "Alert when EventBridge Scheduler invocation failures occur" \
  --metric-name ApproximateNumberOfMessagesVisible \
  --namespace AWS/SQS \
  --statistic Average \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=QueueName,Value=<your-stack-name>-scheduler-dlq
```

## Use Cases

This pattern is ideal for:

- **Scheduled Data Processing**: ETL jobs, data aggregation, report generation
- **Periodic Health Checks**: API monitoring, system health validation
- **Scheduled Notifications**: Daily/weekly reports, reminders
- **Cleanup Tasks**: Log rotation, temporary file cleanup, cache invalidation
- **Batch Operations**: Bulk updates, scheduled backups, data synchronization
- **Learning EventBridge Scheduler**: Understanding retry behavior and DLQ mechanisms


## Troubleshooting

### Messages in Lambda Execution DLQ

This indicates Lambda function execution failures:
- Check Lambda function logs for exceptions
- Review function timeout settings
- Check memory configuration
- Verify environment variables and dependencies

### Messages in Scheduler DLQ

This indicates EventBridge Scheduler invocation failures:
- Verify IAM permissions (scheduler role has `lambda:InvokeFunction`)
- Check if Lambda function exists and is in the correct region
- Review Lambda concurrent execution limits
- Check for Lambda throttling errors

### No Scheduled Executions

- Verify schedule state is `ENABLED`
- Check scheduler role permissions
- Review CloudWatch Logs for the Lambda function
- Verify schedule expression syntax

## Cleanup

Delete the stack and all resources:

```bash
sam delete --stack-name <your-stack-name>
```

## Learn More

- [EventBridge Scheduler Documentation](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html)
- [EventBridge Scheduler Retry Policies](https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-schedule-retry.html)
- [EventBridge Scheduler Dead Letter Queues](https://docs.aws.amazon.com/scheduler/latest/UserGuide/configuring-schedule-dlq.html)
- [Lambda Asynchronous Invocation](https://docs.aws.amazon.com/lambda/latest/dg/invocation-async.html)
- [Lambda Error Handling](https://docs.aws.amazon.com/lambda/latest/dg/invocation-retries.html)
- [SQS Dead Letter Queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)

---

Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.

SPDX-License-Identifier: MIT-0
