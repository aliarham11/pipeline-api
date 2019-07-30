import time
import os
from os import environ as env

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_py_file_path(pipeline_dir, file):
  pipeline_base_dir = env.get('PIPELINE_BASE_DIR_NAME', 'pipelines')
  py_file = '{}.py'.format(file)
  return os.path.join(BASE_DIR, pipeline_base_dir, pipeline_dir, py_file)

def get_python_bin():
  return env.get('PYTHON_BIN', 'python')
