import subprocess
import logging
import json
import re
import time
from uuid import uuid4

from datetime import datetime
from flask import request
from flask.json import jsonify, loads
from pipeline_service.helpers.helper import get_py_file_path, get_python_bin
from .managers.input_manager import input_manager
from google.cloud import datastore
from os import environ as env

from threading import Timer

logger = logging.getLogger(__name__)
BUCKET_REGEX = r'([^*?#]*)/([^*?#]*)/([^*?#]*)(#([0-9]+))?'
REDIS_KEY_TABLE_PREFIX = 'temp'
REDIS_KEY_TRIGGER_PREFIX = 'trigger'

class DataLakeSinkHandler:

  def __init__(self, options=None):
    self.options = options
    self.destination_table = None
    self.uploaded_at = None
    self.notified_at = None
    self.partitioned_at = None
    self.partition_folder = None
    self.input_source = None
    self.schema = None
    self.invalid_data_gcs_dir = None
    self.job_id = None

  def handle(self):
    body = loads(request.data)
    print('Raw Messages: {}'.format(json.dumps(body)))
    
    attributes = body['message']['attributes']
    attributes['publishTime'] = body['message']['publishTime']
    
    if (attributes['eventType'] == 'OBJECT_FINALIZE'):
      if 'INVALID' in attributes['objectId'] or 'SKIP' in attributes['objectId']:
        print('INVALID / SKIP File Received, not processed with pipeline')
        return

      self.parse_attributes(attributes)

      if 'READY' in attributes['objectId']:
        self.options.update({'setup_file': get_py_file_path(pipeline_dir='data_lake_sink', file='setup')})
        self.schema = self.get_jsonschema(
          project_id=self.options['project_id'], 
          dataset=self.options['destination_dataset'], 
          table=self.destination_table)
        
        splitted_obj_id = attributes['objectId'].split('READY~')
        if (len(splitted_obj_id) > 1):
          self.job_id = splitted_obj_id[1].split('.')[0]
        else:
          self.job_id = uuid4()

        self.invalid_data_gcs_dir = '{}/INVALID~{}'.format(self.invalid_data_gcs_dir, self.job_id)
        command = [
          get_python_bin(),
          get_py_file_path(pipeline_dir='data_lake_sink', file='pipeline'),
          '--params',
          self.serialize()
        ]
        
        subprocess.Popen(command)
        return

      input_manager.register_input(attributes['bucketId'], self.destination_table, self.partition_folder, attributes['objectId'])

  def parse_attributes(self, raw_attributes):
    file_regexed = re.match(BUCKET_REGEX, raw_attributes['objectId'])
    uploaded_at_unix_ts = int(raw_attributes['objectGeneration']) / 1000000
    self.destination_table = file_regexed.group(1).split('/')[0]
    self.partition_folder = file_regexed.group(2)
    self.partitioned_at = datetime.strptime(self.partition_folder, '%Y%m%d').strftime('%Y-%m-%d %H:%M:%S')
    self.uploaded_at = datetime.fromtimestamp(uploaded_at_unix_ts).strftime('%Y-%m-%d %H:%M:%S')
    self.notified_at = datetime.strptime(raw_attributes['publishTime'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
    self.input_source = 'gs://{}/{}'.format(raw_attributes['bucketId'], raw_attributes['objectId'])
    self.invalid_data_gcs_dir = 'gs://{}/{}/{}'.format(raw_attributes['bucketId'], 
      file_regexed.group(1), 
      file_regexed.group(2))

  def get_jsonschema(self, project_id, dataset, table):
    key = '{}-{}-{}'.format(project_id, dataset, table)
    client = datastore.Client()
    product_key = client.key(env.get('DATASTORE_SCHEMA_KEY', 'BQPumpImportConfiguration'), key)
    data = client.get(product_key)
  
    if data is None:
      return None
    
    jsonschema = {
      'type': 'object',
      'properties': {},
      'required': []
    }

    for schema_str in data['sourceSchema']:
      schema_json = json.loads(schema_str)
      jsonschema['properties'].update(self.construct_jsonschema_type(schema_json))

      if schema_json['mode'] == 'REQUIRED':
        jsonschema['required'].append(schema_json['name'])
    
    return jsonschema

  def construct_jsonschema_type(self, schema):
    if schema['type'] == 'STRUCT':
      if ('mode' in schema and schema['mode'] == 'REPEATED'):
        props_obj = {}
        
        for field in schema['fields']:
          props_obj.update(self.construct_jsonschema_type(field))
        
        type_obj = {}
        type_obj[schema['name']] = {
          'type': 'array',
          'items': {
            'type': 'object',
            'properties': props_obj
          }
        }

        return type_obj
      else:
        type_obj = {}
        type_obj[schema['name']] = {
          'type': 'object',
          'properties': self.construct_jsonschema_type(schema['fields'])
        }
        
        return type_obj
    else:
      switcher = {
        'DATE': {
          'type': 'string',
          'format': 'date'
        },
        'TIMESTAMP': {
          'type': 'string',
          'format': 'date-time'
        },
        'INT64': {
          'type': 'integer'
        },
        'FLOAT64': {
          'type': 'number'
        },
        'BOOL': {
          'type': 'boolean'
        }
      }

    stype = switcher.get(schema['type'], {'type': schema['type'].lower()})
    if 'mode' in schema and schema['mode'] == 'REPEATED':
      stype = {
        'type': 'array',
        'items': stype
      }
    type_obj = {}
    type_obj[schema['name']] = stype

    return type_obj


  def serialize(self):
    return json.dumps(self.__dict__)