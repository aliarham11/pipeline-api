from pipeline_service.services.rediz import rediz
from pipeline_service.services.storage import Storage
from threading import Timer
from uuid import uuid4
from googleapiclient.discovery import build

import re

BUCKET_REGEX = r'([^*?#]*)/([^*?#]*)/([^*?#]*)(#([0-9]+))?'
REDIS_KEY_TABLE_PREFIX = 'temp'
REDIS_KEY_TRIGGER_PREFIX = 'trigger'

class InputManager(object):

  def register_input(self, bucket_id, table_name, partition_folder, file_name):
    table_key = '{}~{}~{}'.format(REDIS_KEY_TABLE_PREFIX, table_name, partition_folder)
    trigger_key = '{}~{}~{}'.format(REDIS_KEY_TRIGGER_PREFIX, table_name, partition_folder)
    rediz._sadd(table_key, file_name)

    if rediz._get(trigger_key) is None:
      rediz._set(trigger_key, 1)
      t = Timer(5.0, self.compose_input, [bucket_id, trigger_key, partition_folder, table_key])
      t.start()

  def compose_input(self, bucket_id, trigger_key, partition_folder, table_key):
    service = build('storage', 'v1')
    storage = Storage(bucket_id)
    input_sources_arr = []
    table_key_arr = table_key.split('~')
    source = rediz._spop(table_key)
    print('Adding newline to eachfile..')
    while not source is None:
      print(source)
      file_regexed = re.match(BUCKET_REGEX, source)
      content = storage.read(source)
      content = content + '\n'
      dest = '{}/{}/SKIP~{}'.format(file_regexed.group(1), file_regexed.group(2), file_regexed.group(3))
      input_sources_arr.append(dest)
      storage.write(dest, content)
      source = rediz._spop(table_key)
    
    print('Composing input source: {}'.format(','.join(input_sources_arr)))
    compose_req_body = {
        'sourceObjects': [{'name': filename} for filename in input_sources_arr],
        'destination': {
            'contentType': 'application/octet-stream',
        }
    }
    destination = '{}/dataflow_inputs/{}/READY~{}.ndjson'.format(table_key_arr[1], partition_folder, uuid4())
    req = service.objects().compose(
        destinationBucket=bucket_id,
        destinationObject=destination,
        body=compose_req_body)

    req.execute()
    print('Composed input source: {}'.format(','.join(input_sources_arr)))
    rediz._delete(trigger_key)
    rediz._delete(table_key)

input_manager = InputManager()