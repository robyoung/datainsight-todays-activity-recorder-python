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