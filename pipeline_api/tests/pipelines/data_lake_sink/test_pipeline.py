import pytest
import uuid

from pytest_mock import mocker
from datetime import datetime
from pipeline_service.pipelines.data_lake_sink.pardo import parse_row_ndjson, validate_row

SCHEMA = {
    'type': 'object',
    'properties': {
      'name': {
        'type': 'string'
      },
      'age': {
        'type': 'integer'
      },
      'class': {
        'type': 'string'
      }
    }
  }

ELEMENT = "{\"name\":\"john\",\"age\":22,\"class\":\"mca\"}"

@pytest.fixture
def uuid_mocker(mocker):
  mock_uuid = mocker.patch.object(uuid, 'uuid4')
  uuid.uuid4.return_value = 'mock-uuid'
  mock_uuid.start()

  return mock_uuid

def test_parserow_ndjson_to_validation_step(uuid_mocker):
  now = '2019-01-01 01:01'
  datenow = datetime.now()
  imported_at = datenow.strftime("%Y-%m-%d %H:%M")
  dofn = parse_row_ndjson.ParseRowNdjsonFn()
  result_gen = dofn.process(ELEMENT, 'gs://dummy-input-source', now, now, now, True)
  result = result_gen.next()
  expected = '{"name": "john", "bq_imported_by": "Pipeline-service_experiment_1", "age": 22, "bq_uuid": "mock-uuid", "bq_notified_at": "2019-01-01 01:01", "bq_source": "gs://dummy-input-source", "bq_uploaded_at": "2019-01-01 01:01", "bq_partitioned_at": "2019-01-01 01:01", "bq_imported_at": "' + imported_at + '", "class": "mca"}'
  assert result == expected

def test_parserow_ndjson_direct_bq_write(uuid_mocker):
  now = '2019-01-01 01:01'
  datenow = datetime.now()
  imported_at = datenow.strftime("%Y-%m-%d %H:%M")
  dofn = parse_row_ndjson.ParseRowNdjsonFn()
  result_gen = dofn.process(ELEMENT, 'gs://dummy-input-source', now, now, now, False)
  result = result_gen.next()
  
  assert result == {  
    "name":"john",
    "bq_imported_by":"Pipeline-service_experiment_1",
    "age":22,
    "bq_uuid":"mock-uuid",
    "bq_notified_at":now,
    "bq_source":"gs://dummy-input-source",
    "bq_uploaded_at":now,
    "bq_partitioned_at":now,
    "bq_imported_at":imported_at,
    "class":"mca"
  }

def test_validate_row_valid_coll():
  dofn = validate_row.ValidateRowFn(SCHEMA)
  result_gen = dofn.process(ELEMENT)
  result = result_gen.next()
  print(result)

  assert result == {'age': 22, 'class': 'mca', 'name': 'john'}

def test_validate_row_invalid_coll():
  dofn = validate_row.ValidateRowFn(SCHEMA)
  invalid_element = "{\"name\":\"john\",\"age\":\"22\",\"class\":\"mca\"}"
  result_gen = dofn.process(invalid_element)
  result = result_gen.next()
  
  assert result.tag == 'invalid_coll'


