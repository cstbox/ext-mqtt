#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of CSTBox.
#
# CSTBox is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# CSTBox is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with CSTBox.  If not, see <http://www.gnu.org/licenses/>.

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import unittest
import time
import os

from pycstbox.mqtt.core import MQTTConnector, mqtt_client, MQTTNotConnectedError
import pycstbox.log


logger = None

def setUpModule():
    global logger
    pycstbox.log.setup_logging(level=pycstbox.log.INFO)
    logger = pycstbox.log.getLogger()

    logger.info('checking Apollo test server availability...')
    if os.system('ping -c 1 -w 1 %s > /dev/null 2>&1' % MQTTConnectorTestCase.BROKER_HOST):
        logger.error('Apollo test server not reachable')
        raise unittest.SkipTest()
    logger.info('... server OK')

class MQTTConnectorTestCase(unittest.TestCase):
    BROKER_HOST = 'wisdom-cloud.local'
    TOPIC = 'test'
    rcv_message = None

    def setUp(self):
        logger.info('-------------- %s starting...', self._testMethodName)
        self.rcv_message = None
        try:
            self.connector = MQTTConnector(broker=self.BROKER_HOST, port=61613, logger=logger.getChild('connector'))
            self.connector.run()
        except Exception:
            logger.error('Ignoring test : Apollo is not running')
            self.skipTest('%s skipped' % self._testMethodName)

    def tearDown(self):
        self.connector.shutdown()

    def test_01_publish(self):
        logger.debug('first publish')
        result, mid = self.connector.publish(self.TOPIC, 'Hello')
        self.assertEqual(result, mqtt_client.MQTT_ERR_SUCCESS)

        logger.debug('shutdown')
        self.connector.shutdown()

        logger.debug('second publish (should fail)')
        with self.assertRaises(MQTTNotConnectedError):
            self.connector.publish(self.TOPIC, 'Hello')

    def test_02_subscribe(self):
        result, mid = self.connector.subscribe(self.TOPIC)
        self.assertEqual(result, mqtt_client.MQTT_ERR_SUCCESS)

        self.connector.on_message = self._msg_handler

        data = 'Hello'
        result, mid = self.connector.publish(self.TOPIC, data)
        self.assertEqual(result, mqtt_client.MQTT_ERR_SUCCESS)

        time.sleep(0.1)
        self.assertEqual(self.rcv_message, data)

        result, mid = self.connector.unsubscribe(self.TOPIC)
        self.assertEqual(result, mqtt_client.MQTT_ERR_SUCCESS)

    def _msg_handler(self, client, userdata, message):
        self.rcv_message = message.payload


if __name__ == '__main__':
    unittest.main()
