# Write billing items to S3

import boto3
from botocore.exceptions import ClientError
import iso8601
import json
import logging
import os
from retry import retry

log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))  # type: ignore
_logger = logging.getLogger(__name__)

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
DDT_HASH_KEY = os.environ.get('DDT_HASH_KEY')
DDT_RANGE_KEY = os.environ.get('DDT_RANGE_KEY')

dynamodb = boto3.resource('dynamodb')
ddt = dynamodb.Table(DYNAMODB_TABLE)


@retry(ClientError, delay=5, backoff=2, jitter=(0, 10))
def _publish_to_dynamodb(item: dict) -> None:
    '''Publish a line item to DynamoDB'''
    resp = ddt.put_item(
        TableName=DYNAMODB_TABLE,
        Item=item
    )
    _logger.debug('DynamoDB Response: {}'.format(json.dumps(resp)))

    return


def handler(event, context):
    _logger.info('Event received: {}'.format(json.dumps(event)))
    line_item_string = event.get('Records')[0].get('Sns').get('Message')
    line_item = json.loads(line_item_string)

    line_item_id = line_item.get('identity').get('LineItemId')
    line_item[DDT_HASH_KEY] = line_item_id

    # Ensure a consistent time format by always converting to a datetime in
    # case something changes upstream.
    time_interval_start = line_item.get('identity').get('TimeInterval').split('/')[0]
    time_interval_start_dt = iso8601.parse_date(time_interval_start)
    line_item[DDT_RANGE_KEY] = str(time_interval_start_dt)

    # XXX: We should look into stopping after a number of tries and reinvoking
    # ourself in order to keep errors down.
    _publish_to_dynamodb(line_item)

    rep = {'STATUS': 'OK'}
    return json.dumps(rep)

