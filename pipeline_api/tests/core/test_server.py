import pytest
from pipeline_service.core.server import Server
from pytest_mock import mocker
import json

class MockObj(object):
  
  def handle(self):
    return 'handled'

server = Server('test_server')

@pytest.fixture
def test_client(mocker):
  config = {
    'pipelines': [
      {
        'url': '/data_lake_sink',
        'name': 'data_lake_sink',
        'handler_class': 'data_lake_sink.pipeline.DataLakeSinkPipeline'
      },
      {
        'url': '/data_mart_sink',
        'name': 'data_mart_sink',
        'handler_class': 'data_mart_sink.pipeline.DataMartSinkPipeline'
      }
    ]
  }
  
  mocker.patch.object(server, '_import')
  server._import.return_value = MockObj
  server.generate_endpoint(config)
  flask_app = server.app
  flask_app.testing = True

  return flask_app.test_client()


def test_generate_endpoint(test_client):
  assert len(server.obj_pool) == 2

