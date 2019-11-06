#! /usr/bin/python

# Need debian package python-setuptools

import sys
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import datetime

MSG_TOPIC = "amipo/#"

MQTT_HOSTNAME = "127.0.0.1"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "foo"

INFLUX_HOSTNAME = "127.0.0.1"
INFLUX_PORT = 8086
#INFLUX_USER = "root"
#INFLUX_PASSWORD = "root"
INFLUX_USER = ""
INFLUX_PASSWORD = ""
INFLUX_DB = "amipo_capteurs"

def buildInfluxDbJsonBody(measurement, tags, time, fields):
    jsonBody = [{
        "measurement": measurement,
        "tags": tags,
        "time": time,
        "fields": fields
    }]
    #print("jsonBody: ", jsonBody)
    return jsonBody

# measurement: "nom", tags: map, time: "2009-11-10T23:00:00Z", fields: map
def publishData(measurement, tags, time, fields):
    influxClient = InfluxDBClient(INFLUX_HOSTNAME, INFLUX_PORT, INFLUX_USER, INFLUX_PASSWORD, INFLUX_DB)
    jsonBody = buildInfluxDbJsonBody(measurement, tags, time, fields)
    influxClient.write_points(jsonBody)
    influxClient.close()

def processMessage(topic, payload):
    # First case : payload contain raw data
    time = datetime.datetime.utcnow()
    formattedTime = time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    print("formattedTime: ", formattedTime)

    #measurement = topic.replace("/", ".")
    measurement = INFLUX_DB
    tags = { "tenant": "mqtt_daemon", "topic": topic }
    payloadValue = float(payload)
    fields = { "value": payloadValue }

    publishData(measurement, tags, formattedTime, fields)

def on_log(client, userdata, level, buf):
    print("log: ", buf)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code ", str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #client.subscribe("$SYS/#")
    client.subscribe(MSG_TOPIC, qos=2)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("Received MQTT message on topic: [", msg.topic, "] with payload: [", str(msg.payload), "], raw message: [", msg, "]")
    processMessage(msg.topic, msg.payload)

def main():
    mqttClient = mqtt.Client("amipo_daemon", clean_session=False, userdata=None)
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.on_log = on_log

    #mqttClient.connect(MQTT_HOSTNAME, MQTT_PORT, 60)
    print("infos:", MQTT_HOSTNAME, MQTT_PORT)
    mqttClient.connect(MQTT_HOSTNAME, MQTT_PORT)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    try:
        mqttClient.loop_forever()
    except KeyboardInterrupt:
        sys.exit(0)

if __name__ == '__main__':
    main()