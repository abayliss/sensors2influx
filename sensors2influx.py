#!/usr/bin/env python3

import io
import json
import configparser
from influxdb import InfluxDBClient
import logging
import asyncio

def make_stdout_handler():

    conf = configparser.ConfigParser()
    conf.read('sensors2influx.ini')

    influx = InfluxDBClient(
        host=conf['sensors2influx']['influxdb_host'],
        port=conf['sensors2influx']['influxdb_port'],
        database=conf['sensors2influx']['influxdb_database']
    )

    def real_stdout_handler(line):

        logger = logging.getLogger(__name__)
        parsed = json.loads(line)
        # {"time" : "2018-07-19 10:23:41", "brand" : "OS", "model" : "THGR122N", "id" : 230, "channel" : 1, "battery" : "LOW", "temperature_C" : 27.600, "humidity" : 36}
        # {"time" : "2018-07-19 10:23:27", "brand" : "OS", "model" : "THGR122N", "id" : 251, "channel" : 2, "battery" : "LOW", "temperature_C" : 24.300, "humidity" : 46}
        # {"time" : "2018-07-19 10:23:53", "brand" : "OS", "model" : "THGR122N", "id" : 251, "channel" : 3, "battery" : "OK", "temperature_C" : 30.600, "humidity" : 36}
        logger.debug('Sensor: {}, temperature: {}, humidity: {}'.format(parsed['channel'], parsed['temperature_C'], parsed['humidity']))
        points = []
        points.append({'measurement': 'temperature', 'tags': { 'channel': parsed['channel'] }, 'fields': { 'value': parsed['temperature_C'] }})
        points.append({'measurement': 'humidity', 'tags': { 'channel': parsed['channel'] }, 'fields': { 'value': parsed['humidity'] }})

        influx.write_points(points)

    async def stdout_handler(line):
        logger = logging.getLogger(__name__)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, real_stdout_handler, line)

    return stdout_handler


async def stderr_handler(line):
    logger = logging.getLogger(__name__)
    line = line.rstrip()
    logger.info('rtl_433: {}'.format(line))


async def _read_stream(stream, cb):
    logger = logging.getLogger(__name__)
    while True:
        line = await stream.readline()
        if line:
            await cb(line.decode())
        else:
            break


async def main():

    logger = logging.getLogger(__name__)
    logger.info('Starting rtl_433')

    rtl_433 = await asyncio.create_subprocess_exec(
        *['rtl_433', '-R', '12', '-F', 'json'],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    logger.info('Started rtl_433')

    await asyncio.wait([
        _read_stream(rtl_433.stdout, make_stdout_handler()),
        _read_stream(rtl_433.stderr, stderr_handler),
    ])

    return await rtl_433.wait()
    

if __name__ == '__main__':

    # https://kevinmccarthy.org/2016/07/25/streaming-subprocess-stdin-and-stdout-with-asyncio-in-python/

    logging.basicConfig(
        filename='sensors2influx.log',
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(threadName)10s %(name)s %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info('Starting main loop')

    loop = asyncio.get_event_loop()
    rc = loop.run_until_complete(main())
    loop.close()
    
