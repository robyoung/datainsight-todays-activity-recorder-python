import datetime
from datainsight import settings
import pymongo

class Measurements(object):
  DEFAULT_SITE = "govuk"
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

  def save_measurement(self, collected_at, start_at, end_at, value, site):
    query = {"start_at": start_at, "end_at": end_at, "site": site}
    update = {
      "collected_at": collected_at,
      "start_at": start_at,
      "end_at": end_at,
      "value": value,
      "site": site,
      "updated_at": datetime.datetime.now()
    }
    self.coll().update(query, update, upsert=True)

  def get_live_at(self):
    latest = list(self.coll().find().sort("collected_at", pymongo.DESCENDING).limit(1))
    if latest:
      return latest[0]['collected_at']

  def get_visitors_today_by_hour(self, live_at):
    query = {
      "start_at": {"$gte": datetime.datetime.combine(live_at.date(), datetime.time())},
      "end_at": {"$lte": live_at},
      "site": self.DEFAULT_SITE
    }
    d = dict((m['start_at'].hour, m['value']) for m in self.coll().find(query))

    return [d.get(k) for k in range(max(d.keys())+1)]
