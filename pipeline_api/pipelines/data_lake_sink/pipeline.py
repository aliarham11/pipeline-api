import json
import datetime
import argparse
import time

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery, WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from multiprocessing.dummy import Pool

from pardo.parse_row_ndjson import ParseRowNdjsonFn
from pardo.validate_row import ValidateRowFn

def get_current_time_millis():
  return int(round(time.time() * 1000))

def run(attributes, options):

  runner = str(options['runner'])
  input_source = attributes['input_source']
  notification_time = attributes['notified_at']
  upload_time = attributes['uploaded_at']
  partition_time = attributes['partitioned_at']
  destination = '{}:{}.{}'.format(options['project_id'], options['destination_dataset'], attributes['destination_table'])
  pipeline_params = []
  invalid_data_gcs_dir = attributes['invalid_data_gcs_dir']

  if runner == 'DataFlowRunner':
    pipeline_params = [
      '--project', options['project_id'],
      '--temp_location', options['temp_location'],
      '--job_name', '{}-{}'.format(attributes['destination_table'].replace('_','-'), attributes['job_id']),
      '--setup_file', options['setup_file']
    ]
  
  p = beam.Pipeline(runner=runner, argv=pipeline_params)
  input_coll = p | 'read_file_{}'.format(input_source) >> ReadFromText(input_source) 
  
  if '.ndjson' in input_source:
    parsed_coll = input_coll | 'parse_elements_(ndjson)' >> beam.ParDo(
      ParseRowNdjsonFn(), input_source, notification_time, upload_time, partition_time, 
      to_validation_step=False if attributes['schema'] is None else True
    )
  
  # else
    # parse csv data 

  if attributes['schema'] is None:
    parsed_coll | 'write_to_BQ_without_validation' >> beam.io.WriteToBigQuery(
          destination,
          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
  else:
    valid_coll, invalid_coll = parsed_coll | 'validate_rows' >> beam.ParDo(
      ValidateRowFn(attributes['schema'])).with_outputs('invalid_coll', main='valid_coll'
    )
        
    valid_coll | 'write_valid_coll_to_BQ' >> beam.io.WriteToBigQuery(
          destination,
          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    invalid_coll | 'write_invalid_coll_to_GCS' >> beam.io.WriteToText(invalid_data_gcs_dir, file_name_suffix='.ndjson')

  p.run()

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--params',
                      dest='params',
                      required=True,
                      help='Parameters for pipelines')
  args = parser.parse_args()
  params = json.loads(args.params)
  run(attributes=params, options=params['options'])
  