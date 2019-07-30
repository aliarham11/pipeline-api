import redis

POOL = redis.ConnectionPool(host='10.140.0.4', port=6379)
TIMEOUT = 60
class Redis(object):

  def __init__(self):
    self.rediz = redis.Redis(connection_pool=POOL)

  def _get(self, variable_name):
    response = self.rediz.get(variable_name)
    return response

  def _set(self, variable_name, value):
    self.rediz.set(variable_name, value, ex=TIMEOUT)

  def _keys(self, key):
    self.rediz.keys(pattern=key)

  def _sadd(self, variable_name, value):
    self.rediz.sadd(variable_name, value)

  def _spop(self, variable_name):
    return self.rediz.spop(variable_name)
  
  def _delete(self, variable_name):
    self.rediz.delete(variable_name)

rediz = Redis()
