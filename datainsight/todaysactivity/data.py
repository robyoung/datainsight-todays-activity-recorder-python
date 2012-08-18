import datetime
import pandas
import pymongo

from datainsight import settings

def mongo_to_data_frame(cursor, index, fields):
  df = pandas.DataFrame(list(cursor), columns=[index] + fields)
  df.index = df[index]

  return df

class Measurements(object):
  DEFAULT_SITE = "govuk"
  @classmethod
  def create_test(cls):
    return cls.create("test")

  @classmethod
  def create(cls, env):
    return cls(pymongo.Connection(settings.MONGO["host"]), env)

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

  def get_visitors_yesterday_by_hour(self, live_at):
    midnight = datetime.datetime.combine(live_at.date(), datetime.time())
    query = {
      "start_at": {"$gte": midnight - datetime.timedelta(days=1)},
      "end_at": {"$lte": midnight},
      "site": self.DEFAULT_SITE
    }
    d = dict((m['start_at'].hour, m['value']) for m in self.coll().find(query))

    return [d.get(k) for k in range(24)]

  def get_last_month_average_by_hour(self, live_at):
    midnight = datetime.datetime.combine(live_at.date(), datetime.time())
    query = {
      "start_at": {"$gte": midnight - datetime.timedelta(days=30)},
      "end_at": {"$lte": midnight},
      "site": self.DEFAULT_SITE
    }
    cursor = self.coll().find(query)

    result = [None] * 24

    df = mongo_to_data_frame(cursor, "start_at", ["value"])
    for hour, average in df.groupby(lambda x: x.hour).mean()['value'].iteritems():
      result[hour] = average

    return result

#    for key, values in itertools.groupby(sorted((v['start_at'].hour, v['value']) for v in self.coll().find(query)), lambda v: v[0]):
#      values = list(v[1] for v in values)
#      result[key] = sum(values) / float(len(values))
#
#    return result

  def get_activity_today_by_hour(self):
    live_at = self.get_live_at()

    visitors_today = self.get_visitors_today_by_hour(live_at)
    visitors_yesterday = self.get_visitors_yesterday_by_hour(live_at)
    last_month_average = self.get_last_month_average_by_hour(live_at)

    def build_result(hour):
      result = {"hour_of_day": hour, "visitors": {}}
      if hour < len(visitors_today):
        result['visitors']['today'] = visitors_today[hour]
      result['visitors']['yesterday'] = visitors_yesterday[hour]
      result['visitors']['monthly_average'] = last_month_average[hour]

      return result

    return {
      "values": [build_result(hour) for hour in range(24)],
      "live_at": live_at
    }