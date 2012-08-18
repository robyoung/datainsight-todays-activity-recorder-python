import unittest
import datetime
import time

from datainsight.todaysactivity.data import Measurements

class MeasurementsTestCase(unittest.TestCase):
  def setUp(self):
    self.measurements = Measurements.create_test()

  def tearDown(self):
    self.measurements.coll().drop()
    del self.measurements

  def add_measurements(self, start_at, end_at, collected_at=None, value=None, site=None):
    current_time = start_at
    while current_time < end_at:
      params = {
        "collected_at": collected_at or datetime.datetime(2012, 2, 2, 2),
        "start_at": current_time,
        "end_at": current_time + datetime.timedelta(hours=1),
        "site": site or "govuk"
      }
      if value:
        params["value"] = value(params) if callable(value) else value
      else:
        params["value"] = 100
      self.measurements.coll().save(params)
      current_time = params["end_at"]

class LiveAtTestCase(MeasurementsTestCase):
  def test_get_live_at(self):
    conn = self.measurements.coll()
    conn.save({"collected_at": datetime.datetime(2011, 1, 1, 1, 1, 0)})
    conn.save({"collected_at": datetime.datetime(2011, 1, 1, 1, 0, 0)})
    self.assertEqual(
      datetime.datetime(2011, 1, 1, 1, 1, 0),
      self.measurements.get_live_at()
    )

  def test_get_live_at_returns_null(self):
    self.assertIsNone(self.measurements.get_live_at())

class SaveMeasurementTestCase(MeasurementsTestCase):
  def save_measurement(self, value):
    self.measurements.save_measurement(
      collected_at = datetime.datetime(2012, 2, 2, 2),
      start_at = datetime.datetime(2012, 2, 2, 1),
      end_at = datetime.datetime(2012, 2, 2, 1, 1),
      value = value,
      site = "govuk"
    )

  def test_save_measurement(self):
    self.save_measurement(100)
    measurements = list(self.measurements.coll().find())
    self.assertEqual(len(measurements), 1)

    measurement = measurements[0]
    self.assertEqual(measurement['collected_at'], datetime.datetime(2012, 2, 2, 2))
    self.assertEqual(measurement['start_at'], datetime.datetime(2012, 2, 2, 1))
    self.assertEqual(measurement['end_at'], datetime.datetime(2012, 2, 2, 1, 1))
    self.assertEqual(measurement['value'], 100)
    self.assertEqual(measurement['site'], "govuk")
    self.assertIn("_id", measurement)
    self.assertAlmostEqual(measurement['updated_at'], datetime.datetime.now(), delta=datetime.timedelta(seconds=1))

  def test_save_measurement_update_an_existing_one(self):
    self.save_measurement(100)
    measurements1 = list(self.measurements.coll().find())
    time.sleep(0.1)
    self.save_measurement(200)
    measurements2 = list(self.measurements.coll().find())
    self.assertEqual(len(measurements2), 1)

    measurement1 = measurements1[0]
    measurement2 = measurements2[0]

    self.assertEqual(measurement1['_id'], measurement2['_id'])
    self.assertEqual(measurement1['value'], 100)
    self.assertEqual(measurement2['value'], 200)

    self.assertNotEqual(measurement1['updated_at'], measurement2['updated_at'])


class GetDataTestCase(MeasurementsTestCase):
  def setUp(self):
    super(GetDataTestCase, self).setUp()

    self.two_hours_ago = datetime.datetime(2012, 8, 16, 9, 50, 0)
    now = datetime.datetime(2012, 8, 16, 11, 50, 0)
    fourty_days_ago = datetime.datetime.combine(now - datetime.timedelta(days=40), datetime.time())
    midnight = datetime.datetime.combine(now + datetime.timedelta(days=1), datetime.time())

    self.add_measurements(fourty_days_ago, midnight,
      collected_at=self.two_hours_ago,
      value=lambda params: 500 if params["end_at"] < now else 0
    )


class GetTodayByHourTestCase(GetDataTestCase):
  def test_get_visitors_today_by_hour(self):
    activity = self.measurements.get_visitors_today_by_hour(self.two_hours_ago)

    self.assertEqual(len(activity), 9)
    self.assertEqual(activity, [500]*9)

  def test_get_visitors_today_by_hour_with_blanks(self):
    self.measurements.coll().remove({
      "start_at": {"$gte": self.two_hours_ago - datetime.timedelta(hours=7)},
      "end_at": {"$lte": self.two_hours_ago - datetime.timedelta(hours=3)}
    })

    activity = self.measurements.get_visitors_today_by_hour(self.two_hours_ago)

    self.assertEqual(len(activity), 9)
    self.assertEqual(activity, [500, 500, 500, None, None, None, 500, 500, 500])
