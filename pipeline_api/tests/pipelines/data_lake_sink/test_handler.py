import subprocess
import pytest
import json

from flask import request
from pipeline_service.pipelines.data_lake_sink.handler import DataLakeSinkHandler
from pipeline_service.core.server import Server
from pytest_mock import mocker
from google.cloud import datastore

def mock_return():
    return 'called'

server = Server('test_server')

@pytest.fixture
def subprocess_mocker(mocker):
    
  mock_subprocess = mocker.patch.object(subprocess, 'Popen')
  subprocess.Popen.return_value = True
  mock_subprocess.start()

  return mock_subprocess

@pytest.fixture
def test_client(mocker):

  config = {
    'pipelines': [
      {
        'url': '/data_lake_sink',
        'name': 'data_lake_sink',
        'handler_class': 'data_lake_sink.handler.DataLakeSinkHandler',
        'options': {
          'runner': 'DataFlowRunner',
          'project_id': 'test-project',
          'destination_dataset': 'example'
        }
      }
    ]
  }
  server.generate_endpoint(config)
  flask_app = server.app
  flask_app.testing = True

  return flask_app.test_client()

@pytest.fixture
def msg_object():
    return {
                'message': {
                    'attributes': {
                        'bucketId':'test-bucket',
                        'objectId': 'test/20190222/111415854.ndjson',
                        'eventTime':'2019-02-22T05:44:29.924391Z',
                        'objectGeneration':'1550727869924706',
                        'publishTime':'2019-02-22T02:39:12.642Z'
                    }
                }
            }

@pytest.fixture
def get_json_schema_mock(mocker):
  mock_getjsonschema = mocker.patch.object(DataLakeSinkHandler, 'get_jsonschema')
  DataLakeSinkHandler.get_jsonschema.return_value = None
  mock_getjsonschema.start()

  return mock_getjsonschema

@pytest.fixture
def datastore_client_mock(mocker):
  class Client():
    def __init__(self):
      self.keys = None
    
    def get(self, key):
      return {
        'sourceSchema': [
          "{\"name\":\"id\",\"type\":\"STRING\",\"mode\":\"REQUIRED\"}",
          "{\"name\":\"placement\",\"type\":\"INT64\",\"mode\":\"NULLABLE\"}",
          "{\"name\":\"date\",\"type\":\"DATE\",\"mode\":\"NULLABLE\"}",
          "{\"name\":\"choices\",\"type\":\"STRUCT\",\"mode\":\"REPEATED\",\"fields\":[{\"name\":\"order\",\"type\":\"INT64\"},{\"name\":\"option\",\"type\":\"STRING\"},{\"name\":\"title\",\"type\":\"STRING\"}]}"
        ]
      }
    
    def key(self, keys, val):
      self.keys = keys
    
  class DatastoreClient():
    def __call__(self):
      return Client()

  mocker.stopall()
  mock_datastore = DatastoreClient()
  mock_patcher = mocker.patch.object(datastore, 'Client', mock_datastore)

  assert datastore.Client is mock_patcher

  return mock_patcher

def test_handle(subprocess_mocker, msg_object, get_json_schema_mock, test_client):
  response = test_client.post('/data_lake_sink', \
      data=json.dumps(msg_object)
  )
  dl_sink_obj = server.obj_pool.get('data_lake_sink')

  assert dl_sink_obj.destination_table == 'test'
  assert dl_sink_obj.input_source == 'gs://test-bucket/test/20190222/111415854.ndjson'
  assert response.status_code == 200
  assert subprocess_mocker.call_count == 1

def test_get_jsonschema(datastore_client_mock):
  options = {
    'runner': 'DataFlowRunner',
    'project_id': 'test-project',
    'destination_dataset': 'example'
  }
  dl_sink_obj = DataLakeSinkHandler(options=options)
  jsonschema = dl_sink_obj.get_jsonschema('test-project', 'example', 'test')

  assert jsonschema == {  
    'required':[  
        'id'
    ],
    'type':'object',
    'properties':{  
        'date':{  
          'type':'string',
          'format':'date'
        },
        'placement':{  
          'type':'integer'
        },
        'id':{  
          'type':'string'
        },
        'choices':{  
          'items':{  
              'type':'object',
              'properties':{  
                'title':{  
                    'type':'string'
                },
                'order':{  
                    'type':'integer'
                },
                'option':{  
                    'type':'string'
                }
              }
          },
          'type':'array'
        }
    }
  }