import apache_beam as beam
import json

class ParseRowNdjsonFn(beam.DoFn):
  def process(self, element, input_source, notification_time, upload_time, partition_time, to_validation_step):
    import datetime
    import uuid

    try:
      datenow = datetime.datetime.now()
      raw_data = json.loads(element)
      data = {k: v for k, v in raw_data.items() if v is not None}
      data['bq_uuid'] = str(uuid.uuid4())
      data['bq_imported_at'] = datenow.strftime("%Y-%m-%d %H:%M")
      data['bq_imported_by'] = 'Pipeline-service_experiment_1'
      data['bq_source'] = input_source
      data['bq_notified_at'] = notification_time
      data['bq_uploaded_at'] = upload_time
      data['bq_partitioned_at'] = partition_time
      
      if to_validation_step == True:
        yield json.dumps(data)
      else:
        yield data
    except:
      pass
    