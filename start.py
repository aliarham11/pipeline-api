
import yaml
from flask import Flask
from pipeline_service.core.server import Server

def api():
  config = yaml.load(open('config.yml', 'r'))
  server = Server('pipeline-service')
  server.generate_endpoint(config)
  return server.app

app = api()