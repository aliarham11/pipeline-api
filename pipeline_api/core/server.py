from flask import Flask, Response
from dependency_injector import containers, providers
import json
import os
import logging
logger = logging.getLogger(__name__)

class ApiHandler(object):

    def __init__(self, action):
        self.action = action

    def __call__(self, *args):
        try:
          	self.action()
          	return str({ 'status': 'OK' })
        except Exception:
            logging.exception('error')
            return Response(status=400, headers={}, content_type='application/json')
		
class Server(object):
	
	def __init__(self, name):
		self.app = Flask(name)
		self.obj_pool = {}

	def run(self):
		self.app.run()

	def generate_endpoint(self, config):
		for endpoint in config['pipelines']:
			klass = providers.Factory(self._import('pipeline_service.pipelines.{}'.format(endpoint['handler_class'])))
			
			if ('options' in endpoint):
				self.obj_pool.update({ endpoint['name']: klass(endpoint['options']) })
			else:
				self.obj_pool.update({ endpoint['name']: klass() })

			self.app.add_url_rule(
				endpoint['url'],
				endpoint['name'], 
				ApiHandler(self.obj_pool.get(endpoint['name']).handle),
				methods=['POST']
			)
				
		
	def _import(self, name):
		comps = name.split('.')
		cls_name = comps[-1]
		mod = __import__('.'.join(comps[0:-1]), fromlist=[cls_name])
		klass = getattr(mod, cls_name)
		return klass