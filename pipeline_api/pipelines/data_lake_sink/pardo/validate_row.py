import apache_beam as beam
from apache_beam import pvalue
import json

class ValidateRowFn(beam.DoFn):
  
  def __init__(self, schema):
    self.schema = schema

  def process(self, element):
    from jsonschema import validate    
    
    element_json = json.loads(element)
    try:
      validate(instance=element_json, schema=self.schema)
      yield element_json
    except:
      yield pvalue.TaggedOutput('invalid_coll', json.dumps(element_json))
  