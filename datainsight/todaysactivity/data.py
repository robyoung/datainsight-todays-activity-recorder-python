from datainsight import settings
import pymongo

class Measurements(object):
  @classmethod
  def create_test(cls):
    return cls(pymongo.Connection(settings.MONGO["host"]), "test")

  def __init__(self, conn, env):
    self._conn = conn
    self._env = env

  def conn(self):
    return self._conn

  def coll(self):
    """
    Return an instance of the PyMongo collection object
    """
    if not hasattr(self, "_coll"):
      self._coll = self._conn[settings.MONGO["database"] + "_" + self._env]["measurements"]
    return self._coll

  def get_live_at(self):
    latest = list(self.coll().find().sort("collected_at", pymongo.DESCENDING).limit(1))
    if latest:
      return latest[0]['collected_at']