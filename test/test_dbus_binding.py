# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import time

from paho.mqtt.client import MQTT_ERR_SUCCESS

from pycstbox.mqtt.dbus_binding import MQTTGatewayServiceObject


class MockEventManagerService(object):
    handler = None

    def connect_to_signal(self, sig_name, handler, dbus_interface=None):
        self.handler = handler
        print("connecting handler %s to signal %s" % (handler, sig_name))


class TestDBusBinding(object):
    connector_started = False
    mqtt_out = []
    last_out_len = len(mqtt_out)
    evtmgr_svc = {}

    def _reset_publish(self):
        self.mqtt_out = []

    def _published(self):
        cur_len = len(self.mqtt_out)
        changed = cur_len != self.last_out_len
        self.last_out_len = cur_len
        return changed

    def test_events_handling(self, monkeypatch, cfg):
        def mock_get_object(channel):
            self.evtmgr_svc[channel] = svc = MockEventManagerService()
            return svc

        def mock_config_read(*args, **kwargs):
            pass

        def mock_run(connector):
            print('starting MQTT connector')
            self._reset_publish()
            self.connector_started = True

        def mock_publish(connector, topic, payload):
            print('publishing MQTT message (%s, %s)' % (topic, payload))
            self.mqtt_out.append((topic, payload))
            self.connector_started = True
            return MQTT_ERR_SUCCESS, 42

        self.connector_started = False

        monkeypatch.setattr('pycstbox.evtmgr.get_object', mock_get_object)
        monkeypatch.setattr('pycstbox.wisdom.mqtt.gs', {'system_id': 'cbx-unittests'})
        monkeypatch.setattr('pycstbox.mqtt.core.MQTTConnector.run', mock_run)
        monkeypatch.setattr('pycstbox.mqtt.core.MQTTConnector.publish', mock_publish)
        so = MQTTGatewayServiceObject(cfg)
        assert so

        so.start()
        assert self.connector_started

        assert len(self.evtmgr_svc) != 0

        sensor_bus = self.evtmgr_svc['sensor']

        sensor_bus.handler(so, int(time.time() * 1000), 'usage', 'water_kitchen', {'value': True})
        assert self._published()
        topic, payload = self.mqtt_out[-1]
        assert topic == '/data/observation/usage_kitchen'

        sensor_bus.handler(so, int(time.time() * 1000), 'movement', 'bedroom', {'value': True})
        assert not self._published()

        value = 35
        sensor_bus.handler(so, int(time.time() * 1000), 'flow', 'main', {'value': value})
        assert self._published()
        topic, payload = self.mqtt_out[-1]
        assert topic == '/data/observation/main_flow'
        assert payload['data'][0]['value'] == value

        self.evtmgr_svc['control'].handler(so, int(time.time() * 1000), 'switch', 'bedroom', {'value': True})
        assert not self._published()
