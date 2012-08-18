import unittest
import datetime

from datainsight.todaysactivity.data import Measurements

class MeasurementsTestCase(unittest.TestCase):
  def setUp(self):
    self.measurements = Measurements.create_test()

  def tearDown(self):
    self.measurements.coll().drop()
    del self.measurements

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

  def test_save_measurement(self):
    self.measurements.save_measurement(
      collected_at = datetime.datetime(2012, 2, 2, 2),
      start_at = datetime.datetime(2012, 2, 2, 1),
      end_at = datetime.datetime(2012, 2, 2, 1, 1),
      value = 100,
      site = "govuk"
    )
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
    self.measurements.save_measurement(
      collected_at = datetime.datetime(2012, 2, 2, 2),
      start_at = datetime.datetime(2012, 2, 2, 1),
      end_at = datetime.datetime(2012, 2, 2, 1, 1),
      value = 100,
      site = "govuk"
    )
    measurements1 = list(self.measurements.coll().find())
    self.measurements.save_measurement(
      collected_at = datetime.datetime(2012, 2, 2, 3),
      start_at = datetime.datetime(2012, 2, 2, 1),
      end_at = datetime.datetime(2012, 2, 2, 1, 1),
      value = 200,
      site = "govuk"
    )
    measurements2 = list(self.measurements.coll().find())
    self.assertEqual(len(measurements2), 1)

    measurement1 = measurements1[0]
    measurement2 = measurements2[0]

    self.assertEqual(measurement1['_id'], measurement2['_id'])
    self.assertEqual(measurement1['value'], 100)
    self.assertEqual(measurement2['value'], 200)

    self.assertNotEqual(measurement1['updated_at'], measurement2['updated_at'])