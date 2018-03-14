# Write billing items to S3

import boto3
import iso8601
import json
import logging
import os

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
log_level = os.environ.get('LOG_LEVEL', 'INFO')
logging.root.setLevel(logging.getLevelName(log_level))  # type: ignore
_logger = logging.getLogger(__name__)

ddb_client = boto3.client('ssb')

def handler(event, context):
    _logger.info('Event received: {}'.format(json.dumps(event)))

    rep = {'STATUS': 'OK'}
    return json.dumps(rep)

