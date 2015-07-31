#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import time

from pycstbox.mqtt.dbus_binding import MQTTGatewayServiceObject


class MockEventManagerService(object):
    handler = None

    def connect_to_signal(self, sig_name, handler, dbus_interface=None):
        self.handler = handler
        print("connecting handler %s to signal %s" % (handler, sig_name))


class TestRealPublication(object):
    mqtt_out = []
    last_out_len = len(mqtt_out)
    evtmgr_svc = {}

    def _published(self):
        cur_len = len(self.mqtt_out)
        changed = cur_len != self.last_out_len
        self.last_out_len = cur_len
        return changed

    def test_events_publication(self, monkeypatch, cfg):
        def mock_get_object(channel):
            self.evtmgr_svc[channel] = svc = MockEventManagerService()
            return svc

        def mock_config_read(*args, **kwargs):
            pass

        monkeypatch.setattr('pycstbox.evtmgr.get_object', mock_get_object)
        monkeypatch.setattr('pycstbox.wisdom.mqtt.gs', {'system_id': 'cbx-unittests'})
        # monkeypatch.setattr('pycstbox.mqtt.core.MQTTConnector.run', mock_run)
        # monkeypatch.setattr('pycstbox.mqtt.core.MQTTConnector.publish', mock_publish)
        so = MQTTGatewayServiceObject(cfg)
        assert so

        so.start()
        assert len(self.evtmgr_svc) != 0

        sensor_bus = self.evtmgr_svc['sensor']

        sensor_bus.handler(so, int(time.time() * 1000), 'usage', 'water_kitchen', {'value': True})

        so.stop()

