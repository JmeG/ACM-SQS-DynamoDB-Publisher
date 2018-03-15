'''Write billing items to DynamoDB'''

import json
import logging
import os
from time import sleep

import boto3
from botocore.exceptions import ClientError
import iso8601
from retry import retry

# Logging
log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))  # type: ignore
_logger = logging.getLogger(__name__)

# DynamoDB
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
DDT_HASH_KEY = os.environ.get('DDT_HASH_KEY')
DDT_RANGE_KEY = os.environ.get('DDT_RANGE_KEY')
dynamodb = boto3.resource('dynamodb')
ddt = dynamodb.Table(DYNAMODB_TABLE)

# SQS
SQS_RCV_MAX_MSGS = 10
# NOTE: This should not be shorter than the function invocation limit.
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
sqs = boto3.client('sqs')

# Lambda
lambda_client = boto3.client('lambda')

MSG_PUBLISH_DELAY = int(os.environ.get('MSG_PUBLISH_DELAY'))


@retry(ClientError, delay=5, backoff=2)
def _delete_sqs_messages(receipt_handle: str) -> None:
    '''Delete messages from SQS queue'''

    # NOTE: We set message visibility timeout on the queue in the stack.
    resp = sqs.delete_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=SQS_RCV_MAX_MSGS,
    )
    _logger.debug('_delete_sqs_messages:sqs.delete_message() -> {}'.format(json.dumps(resp)))

    return


def _get_line_item_from_message_body(message: str) -> dict:
    '''Return a line item'''
    line_item = json.loads(message)

    line_item_id = line_item.get('identity').get('LineItemId')
    line_item[DDT_HASH_KEY] = line_item_id

    time_interval_start = line_item.get('identity').get('TimeInterval').split('/')[0]
    time_interval_start_dt = iso8601.parse_date(time_interval_start)
    line_item[DDT_RANGE_KEY] = str(time_interval_start_dt)

    return line_item


@retry(ClientError, delay=5, backoff=2)
def _get_sqs_messages(visibility_timeout=None) -> list:
    '''Get messages from SQS queue'''
    kwargs = {'MaxNumberOfMessages': SQS_RCV_MAX_MSGS}
    if visibility_timeout is not None:
        kwargs['VisibilityTimeout'] = visibility_timeout

    resp = sqs.receive_message(**kwargs)
    _logger.debug('_get_sqs_messages:sqs.receive_message() -> {}'.format(json.dumps(resp)))

    return resp.get('Messages')


def _process_additional_items(event, context):
    '''Spawn function to continue handling messages.'''
    lambda_client.invoke(
        QueueUrl=SQS_QUEUE_URL,
        FunctionName=context.invoked_function_arn,
        Payload=json.dumps(event),
        InvocationType='Event'
    )

    return


@retry(ClientError, delay=30, backoff=2, jitter=(0, 10))
def _publish_to_dynamodb(item: dict) -> None:
    '''Publish a line item to DynamoDB'''
    resp = ddt.put_item(
        TableName=DYNAMODB_TABLE,
        Item=item
    )
    _logger.debug('_publish_to_dynamodb:ddt.put_item() -> {}'.format(json.dumps(resp)))

    return


def handler(event, context):
    '''Function handler'''
    _logger.info('handler: event={}'.format(json.dumps(event)))

    while True:
        sqs_messages = _get_sqs_messages()

        # Nothing to do here!
        if not sqs_messages:
            break

        # Loop and publish messages/
        for message in sqs_messages:
            receipt_handle = message.get('ReceiptHandle')
            message_body = message.get('Body')
            line_item = _get_line_item_from_message_body(message_body)
            _publish_to_dynamodb(line_item)
            _delete_sqs_messages(receipt_handle)

            # Optional sleep to control publishing date.
            sleep(MSG_PUBLISH_DELAY)

        # Make that Mario sound because we're running out of time. See if
        # there's more messages in queue and invoke another Lambda if there
        # are.
        if context.get_remaining_time_in_millis() <= 2000:
            #
            if _get_sqs_messages(visibility_timeout=0):
                _logger.info('handler: Additional items in queue, invoking...')
                _process_additional_items(event, context)
            break

    rep = {'STATUS': 'OK'}
    return json.dumps(rep)

