from mqtt_daemon import buildInfluxDbJsonBody

NAME1 = "NAME1"
TAGS1 = {"tag1": "val1", "tag2": "val2"}
TIME1 = "2009-11-10T23:00:00Z"
FIELDS1 = {"field3": "val3", "field4": "val4"}


def test_buildInfluxDbJsonBody_ok():
     result = buildInfluxDbJsonBody(NAME1, TAGS1, TIME1, FIELDS1)
     assert result[0].get('measurement') == NAME1