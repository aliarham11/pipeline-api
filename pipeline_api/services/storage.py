# -*- coding: utf-8 -*-

from itertools import groupby
from google.cloud import storage

import re
import time
import json

DATE_REGEX = r'(\d{4})[/.-](\d{2})[/.-](\d{2})'
BUCKET_REGEX = r'([^*?#]*)/([^*?#]*)/([^*?#]*)(#([0-9]+))?'


class Storage(object):

  def __init__(self, bucket_name):
    storage_client = storage.Client()
    self.bucket = storage_client.get_bucket(bucket_name)

  def write(self, filename, content, file_type='application/json'):
      """
      Write content to GCS

      Arguments:
          filename    (string)
          content     (string)
          file_type   (optional)(string)
      Returns:
          void
      """
      blob = self.bucket.blob(filename)
      blob.upload_from_string(content, content_type=file_type)

  def read(self, filename):
      blob = self.bucket.get_blob(filename)
      content = blob.download_as_string()
      return content

  def convert_to_ndjson(self, filename):
      blob_content = self.read(filename)
      self.write(filename.replace('.json', '.ndjson'), self._map_to_ndjson(blob_content))

  def _map_to_ndjson(self, blob_content):
      return '\n'.join(self._create_json_generator(json.loads(blob_content)))

  # def filter_by_selected_table(self, filename, selected_table):
  #     mode = 0
  #     file_regexed = re.match(BUCKET_REGEX, filename)
  #     if (len(selected_table) > 0 and type(file_regexed) is not None):
  #         table = file_regexed.group(1).split('/')[0]
  #         mode = 1 if (table in selected_table) == True else 0

  #     if (mode == 1):
  #         self.convert_to_ndjson_partitioned(filename)
  #     else:
  #         self.convert_to_ndjson(filename)

  # def convert_to_ndjson_partitioned(self, filename):
  #     blob_content = self.read(filename)
  #     file_regexed = re.match(BUCKET_REGEX, filename) # group_1 => table_name/country_id, group_2 => folder_date, group_3 => file_name
  #     ndjson_filename = str(file_regexed.group(3)).replace('.json', '.ndjson')

  #     for res in self._map_to_ndjson_partitioned(blob_content):
  #       self.write(f'{file_regexed.group(1)}/{res[0]}/{ndjson_filename}', '\n'.join(self._create_json_generator(res[1])))

  def _map_to_ndjson_partitioned(self, blob_content):
      for key, group in groupby(json.loads(blob_content), lambda x: ''.join(re.match(DATE_REGEX, x['updated_at']).groups())):
          yield (key, group)

  def _create_json_generator(self, json_content):
      for content in json_content:
          yield json.dumps(content)
