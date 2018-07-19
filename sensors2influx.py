#!/usr/bin/env python3

import io
import subprocess
import json
from influxdb import InfluxDBClient

rtl_433 = subprocess.Popen(
    ['rtl_433', '-R', '12', '-F', 'json'],
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL
)

influx = InfluxDBClient(
    host='bugenhagen',
    port=8086,
    database='house'
)

for line in io.TextIOWrapper(rtl_433.stdout, encoding='utf-8'):
    parsed = json.loads(line)
    # {"time" : "2018-07-19 10:23:41", "brand" : "OS", "model" : "THGR122N", "id" : 230, "channel" : 1, "battery" : "LOW", "temperature_C" : 27.600, "humidity" : 36}
    # {"time" : "2018-07-19 10:23:27", "brand" : "OS", "model" : "THGR122N", "id" : 251, "channel" : 2, "battery" : "LOW", "temperature_C" : 24.300, "humidity" : 46}
    # {"time" : "2018-07-19 10:23:53", "brand" : "OS", "model" : "THGR122N", "id" : 251, "channel" : 3, "battery" : "OK", "temperature_C" : 30.600, "humidity" : 36}
    print('Sensor: {}, temperature: {}, humidity: {}'.format(parsed['channel'], parsed['temperature_C'], parsed['humidity']))
    points = []
    points.append({'measurement': 'temperature', 'tags': { 'channel': parsed['channel'] }, 'fields': { 'value': parsed['temperature_C'] }})
    points.append({'measurement': 'humidity', 'tags': { 'channel': parsed['channel'] }, 'fields': { 'value': parsed['humidity'] }})
    influx.write_points(points)
