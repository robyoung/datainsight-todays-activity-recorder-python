import datetime
import flask
from datainsight.todaysactivity import data

app = flask.Flask(__name__)

@app.route('/todays-activity')
def todays_activity():
  def json_default(obj):
    if isinstance(obj, datetime.datetime):
      return obj.isoformat()
    else:
      raise TypeError("Invalid object")
  return flask.Response(
    flask.json.dumps(data.Measurements.create('dev').get_activity_today_by_hour(), default=json_default),
    content_type="application/json"
  )
