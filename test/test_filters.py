#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import unittest
import time

from paho.mqtt.client import MQTTMessage

from pycstbox.mqtt.core import REOutboundFilter, REInboundFilter
from pycstbox.log import getLogger, DEBUG
from pycstbox.evtmgr import CONTROL_EVENT_CHANNEL


_DEBUG_LOG = True

class _BaseTestCase(unittest.TestCase):
    def setUp(self):
        self._logger = getLogger(self.__class__.__name__ + '.' + self._testMethodName)
        if _DEBUG_LOG:
            self._logger.setLevel(DEBUG)
        self._logger.debug('started')

    def tearDown(self):
        self._logger.debug('ended')


class REOutboundFilterTestCase(_BaseTestCase):
    """ REOutboundFilter class unit tests.
    """
    def test_01_valid_rules(self):
        """ Checks that valid rules are all accepted.
        """
        f = REOutboundFilter([
            (r'temperature:bedroom', '/static/topic'),
            (r'flow:.*', '/static/water/flow'),
            (r'open:door_(?P<which>[a-z0-9]+)', '/dynamic/door/%(which)s'),
            (r'open:(?P<what>[a-z0-9]+)_(?P<which>[a-z0-9]+)', '/dynamic/door/%(which)s')
        ], self._logger)
        self.assertEqual(len(f._rules), 4)

    def test_02_invalid_rules(self):
        """ Checks that all invalid forms are caught.
        """
        with self.assertRaisesRegexp(ValueError, r'^invalid rule .*'):
            REOutboundFilter([
                ('', '/static/topic')
            ], self._logger)
        with self.assertRaisesRegexp(ValueError, r'^invalid rule .*'):
            REOutboundFilter([
                (r'foo', ' ')
            ], self._logger)
        with self.assertRaisesRegexp(ValueError, r'^invalid regex .*'):
            REOutboundFilter([
                (r'(<foo', '/bar')
            ], self._logger)
        with self.assertRaisesRegexp(ValueError, r'^topic format parameters.*(foo)'):
            REOutboundFilter([
                (r'(?P<vtype>[a-z0-9]+):(?P<vname>[a-z0-9]+)', '/dynamic/door/%(foo)s')
            ], self._logger)

    def test_03_filtering(self):
        """ Checks outgoing events transformation.
        """
        f = REOutboundFilter([
            (r'temperature:bedroom', '/static/topic'),
            (r'flow:.*', '/static/water/flow'),
            (r'open:door_(?P<which>[a-z0-9]+)', '/dynamic/door/%(which)s'),
            (r'open:(?P<what>[a-z0-9]+)_(?P<which>[a-z0-9]+)', '/dynamic/%(what)s/%(which)s')
        ], self._logger)

        self.assertEqual(
            f.get_mqtt_topic(time.time(), 'temperature', 'bedroom', {'value': 25.0}),
            '/static/topic'
        )
        self.assertEqual(
            f.get_mqtt_topic(time.time(), 'flow', 'kitchen', {'value': 12.0}),
            '/static/water/flow'
        )
        self.assertEqual(
            f.get_mqtt_topic(time.time(), 'open', 'door_entrance', {'value': True}),
            '/dynamic/door/entrance'
        )
        self.assertEqual(
            f.get_mqtt_topic(time.time(), 'open', 'window_bedroom', {'value': True}),
            '/dynamic/window/bedroom'
        )
        self.assertEqual(len(f._cache), 4)

    def test_04_blocking(self):
        """ Checks events blocking mechanism.
        """
        f = REOutboundFilter([
            (r'temperature:bedroom', '/static/topic'),
            (r'flow:.*', '/static/water/flow'),
            (r'open:door_(?P<which>[a-z0-9]+)', '/dynamic/door/%(which)s'),
            (r'open:(?P<what>[a-z0-9]+)_(?P<which>[a-z0-9]+)', '/dynamic/%(what)s/%(which)s')
        ], self._logger)
        self.assertIsNone(
            f.get_mqtt_topic(time.time(), 'temperature', 'kitchen', {'value': 25.0})
        )
        self.assertEqual(len(f._cache), 1)
        self.assertIsNone(
            f.get_mqtt_topic(time.time(), 'temperature', 'kitchen', {'value': 25.0})
        )
        self.assertEqual(len(f._cache), 1)


class REInboundFilterTestCase(_BaseTestCase):
    """ REInboundFilter class unit tests.
    """
    def test_01a_valid_rules(self):
        """ Checks that valid rules are all accepted.
        """
        f = REInboundFilter([
            (r'/dim/bedroom', [('dim', 'bedroom', CONTROL_EVENT_CHANNEL)]),
            (r'/dim/(?P<which>[a-z0-9]+)', [('dim', '%(which)s')]),
        ], self._logger)
        self.assertEqual(len(f._rules), 2)

        # check that the channel has been defaulted as expected
        for rule in f._rules:
            _, dispatch = rule
            self.assertEqual(len(dispatch), 1)
            var_type, var_name, channel = dispatch[0]
            self.assertEqual(channel, CONTROL_EVENT_CHANNEL)

    def test_01b_valid_multi_events(self):
        """ Checks valid rules with multiple events dispatch.
        """
        f = REInboundFilter([
            (r'/dim/all', [('dim', 'bedroom'), ('dim', 'living')]),
        ], self._logger)
        self.assertEqual(len(f._rules), 1)
        _, dispatch = f._rules[0]
        self.assertEqual(len(dispatch), 2)
        for specs in dispatch:
            var_type, var_name, channel = specs
            self.assertEqual(channel, CONTROL_EVENT_CHANNEL)

    def test_01c_empty_dispatch(self):
        """ Checks acceptance of rules with an empty events dispatch.
        """
        f = REInboundFilter([
            (r'/dim/all', []),
        ], self._logger)
        self.assertEqual(len(f._rules), 0)

    def test_02_invalid_rules(self):
        """ Checks that all invalid forms are caught.
        """
        with self.assertRaisesRegexp(ValueError, r'^missing regex .*'):
            REInboundFilter([
                ('', [('dim', 'bedroom', CONTROL_EVENT_CHANNEL)])
            ], self._logger)
        with self.assertRaisesRegexp(ValueError, r'^invalid regex .*'):
            REInboundFilter([
                (r'(<foo', [('dim', 'bedroom', CONTROL_EVENT_CHANNEL)])
            ], self._logger)
        with self.assertRaisesRegexp(ValueError, r'^parameters not found .*(foo)'):
            REInboundFilter([
                (r'(?P<bar>[a-z0-9]+)', [('dim', '%(foo)s')])
            ], self._logger)

    def test_03a_filtering_not_impl(self):
        f = REInboundFilter([
            (r'/dim/all', [('dim', 'bedroom'), ('switch', 'living')]),
        ], self._logger)
        mqtt_message = MQTTMessage()
        mqtt_message.topic = '/dim/all'

        with self.assertRaises(NotImplementedError):
            # if we get the exception, it means we have passed all the steps
            f.accept_event(None, None, mqtt_message)

    def test_03b_filtering_impl(self):
        class RealFilter(REInboundFilter):
            def make_event_payload(self, client, user_data, message, var_type, var_name):
                if var_type == 'dim':
                    status = message.payload
                else:
                    status = bool(message.payload)
                return status, None

        f = RealFilter([
            (r'/dim/all', [('dim', 'bedroom'), ('switch', 'living')]),
            (r'/dim/(?P<what>[a-z0-9]+)', [('dim', '%(what)s')]),
        ], self._logger)

        mqtt_message = MQTTMessage()
        mqtt_message.topic = '/dim/all'
        mqtt_message.payload = 50

        events = f.accept_event(None, None, mqtt_message)
        self.assertEqual(len(events), 2)
        for evt, channel in events:
            self.assertEqual(channel, CONTROL_EVENT_CHANNEL)
            if evt.var_type == 'dim':
                self.assertEqual(evt.value, 50)
            else:
                self.assertEqual(evt.value, True)

        mqtt_message.payload = 0
        events = f.accept_event(None, None, mqtt_message)
        for evt, channel in events:
            if evt.var_type == 'dim':
                self.assertEqual(evt.value, 0)
            else:
                self.assertEqual(evt.value, False)

        mqtt_message = MQTTMessage()
        mqtt_message.topic = '/dim/bedroom'
        mqtt_message.payload = 50

        events = f.accept_event(None, None, mqtt_message)
        self.assertEqual(len(events), 1)
        event, channel = events[0]
        self.assertEqual(event.value, 50)

        mqtt_message = MQTTMessage()
        mqtt_message.topic = '/open/door'
        mqtt_message.payload = True

        events = f.accept_event(None, None, mqtt_message)
        self.assertEqual(len(events), 0)


if __name__ == '__main__':
    # disable detailed trace when running as a script
    _DEBUG_LOG = False
    unittest.main()
