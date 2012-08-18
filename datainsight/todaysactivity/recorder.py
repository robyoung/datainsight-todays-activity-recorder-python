import dateutil.parser
import re
import json
from amqplib import client_0_8 as amqp
from datainsight.todaysactivity import data

class Recorder(object):
  EXCHANGE = "datainsight"
  QUEUE_NAME = "ga-weekly-python"
  ROUTING_KEYS = ["google_analytics.visitors.hourly"]
  CALLBACK_NAME = "my-callback"
  METRIC_PATTERN = re.compile(r'\.(?P<metric>visits|visitors)\.weekly$')

  def __init__(self, logger, env):
    self.logger = logger
    self.measurements = data.Measurements.create(env)

  @property
  def chan(self):
    if not hasattr(self, "_chan"):
      self._chan = self.create_channel()
    return self._chan

  @property
  def conn(self):
    if not hasattr(self, "_conn"):
      self._conn = amqp.Connection()
    return self._conn

  def create_channel(self):
    chan = self.conn.channel()
    chan.queue_declare(queue=self.QUEUE_NAME, exclusive=True)
    chan.exchange_declare(exchange=self.EXCHANGE, type="topic", auto_delete=False)
    for routing_key in self.ROUTING_KEYS:
      chan.queue_bind(queue=self.QUEUE_NAME, exchange=self.EXCHANGE, routing_key=routing_key)
    chan.basic_consume(queue=self.QUEUE_NAME, callback=self.handle_message, consumer_tag=self.CALLBACK_NAME)

    return chan

  def handle_message(self, message):
    print "Received message: %s" % message.body
    message = self.parse_amqp_message(message)
    self.measurements.save_measurement(
      collected_at = dateutil.parser.parse(message["envelope"]["collected_at"]),
      start_at = self.parse_start_at(message["payload"]["start_at"]),
      end_at = self.parse_end_at(message["payload"]["end_at"]),
      value = message["payload"]["value"],
      site = message["payload"]["site"]
    )

  def parse_amqp_message(self, raw_message):
    message = json.loads(raw_message.body)
    message['envelope']['_routing_key'] = raw_message.delivery_info['routing_key']

    return message

  def parse_metric(self, routing_key):
    return self.METRIC_PATTERN.search(routing_key).group("metric")

  def parse_start_at(self, start_at):
    return dateutil.parser.parse(start_at)

  def parse_end_at(self, end_at):
    return dateutil.parser.parse(end_at)

  def run(self):
    try:
      while True:
        self.chan.wait()
    except KeyboardInterrupt:
      self.chan.basic_cancel(self.CALLBACK_NAME)
      self.chan.close()
      self.conn.close()

