# -*- coding: utf-8 -*-

""" pytest unit tests for ConfigurableMixin
"""

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import os

import pytest

from pycstbox.mqtt.core import ConfigurableGatewayMixin
from pycstbox.mqtt.re_adapters import REOutboundAdapter

@pytest.fixture
def cfg():
    import json
    with file(os.path.join(os.path.dirname(__file__), 'fixtures/gateway.cfg')) as fp:
        cfg_dict = json.load(fp)
    assert cfg_dict
    return cfg_dict


@pytest.fixture
def cgm(request, show_log):
    instance = ConfigurableGatewayMixin()
    return instance


class TestCGM(object):
    def test_config_ok(self, cgm, cfg):
        """
        :param ConfigurableGatewayMixin cgm:
        :param dict cfg:
        """
        mqttc, inbound_adapter, outbound_adapters = cgm.configure(cfg)

        assert mqttc
        assert mqttc._broker == "messageserver.doc.ic.ac.uk"
        assert mqttc._port == 61613

        # TODO assert inbound_adapter

        assert outbound_adapters
        assert set(outbound_adapters.keys()) == {'sensor', 'control'}

        adapter = outbound_adapters['sensor']
        assert isinstance(adapter, REOutboundAdapter)
        assert adapter.rules
        assert len(adapter.rules) == 2
        for rule in adapter.rules:
            assert type(rule.regex).__name__ == 'SRE_Pattern'
            mpr = rule.mpr
            assert mpr.topic
            assert callable(mpr.payload_builder)
