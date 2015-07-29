# -*- coding: utf-8 -*-

""" pytest unit tests for REOutboundAdapter
"""

import os
import logging
import inspect

import pytest

from pycstbox.mqtt.re_adapters import REOutboundAdapter
from pycstbox.mqtt.errors import ConfigurationError


@pytest.fixture
def cfg():
    import json
    with file(os.path.join(os.path.dirname(__file__), 'fixtures/outbound_tests.cfg')) as fp:
        cfg_dict = json.load(fp)
    assert cfg_dict
    return cfg_dict


@pytest.fixture
def adapter(request, show_log):
    adapt = REOutboundAdapter()
    if show_log:
        adapt.logger.critical('=========== ' + request.node.name)
        adapt.log_setLevel(logging.DEBUG)
    else:
        adapt.log_setLevel(logging.CRITICAL)
    return adapt


class TestConfiguration(object):
    def test_config_ok(self, adapter, cfg):
        adapter.configure(cfg['outbound.ok'])
        assert len(adapter.rules) == 2
        assert adapter.rules[1].regex.match('sensor:usage:water_kitchen')

    def test_default_builder(self, adapter, cfg):
        adapter.configure(cfg['outbound.default_builder'])
        assert len(adapter.rules) == 2
        builder = adapter.rules[1].mpr.payload_builder
        assert callable(builder)
        assert len(inspect.getargspec(builder)) == 4

    def test_adapter_class_mismatch(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.class_mismatch'])
        assert 'class type mismatch' in str(exc_info.value)

    def test_adapter_rule_incomplete_1(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.rule_incomplete.1'])
        assert 'no builder' in str(exc_info.value)

    def test_adapter_rule_incomplete_2(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.rule_incomplete.2'])
        assert 'no builder' in str(exc_info.value)

    def test_adapter_rule_incomplete_3(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.rule_incomplete.3'])
        assert 'empty member' in str(exc_info.value)

    def test_adapter_rule_incomplete_4(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.rule_incomplete.4'])
        assert 'incomplete rule' in str(exc_info.value)

    def test_adapter_bad_regex(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.bad_regex'])
        assert 'invalid regex' in str(exc_info.value)

    def test_adapter_bad_builder_1(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.bad_builder.1'])
        assert 'builder not found' in str(exc_info.value)

    def test_adapter_bad_builder_2(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.bad_builder.2'])
        assert 'builder not found' in str(exc_info.value)

    def test_adapter_bad_builder_3(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.bad_builder.3'])
        assert 'not callable' in str(exc_info.value)

    def test_adapter_bad_builder_4(self, adapter, cfg):
        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.bad_builder.4'])
        assert 'builder signature mismatch' in str(exc_info.value)

    def test_adapter_topic_parms(self, adapter, cfg):
        adapter.configure(cfg['outbound.topic_parms.1'])

        with pytest.raises(ConfigurationError) as exc_info:
            adapter.configure(cfg['outbound.topic_parms.2'])
        assert 'topic format' in str(exc_info.value)


@pytest.fixture
def cfg_outbound(adapter, cfg):
    return adapter.configure(cfg['outbound.ok'])


class TestHandling(object):
    def test_match_1(self, adapter, cfg, cfg_outbound):
        result = adapter.handle_event(0, "sensor", "flow", "main", {"value": True})
        assert result is not None, "no match for flow:main"
        topic, payload = result
        assert topic == 'main_flow'
        assert payload['value']

    def test_match_2(self, adapter, cfg, cfg_outbound):
        result = adapter.handle_event(0, "sensor", "usage", "water_kitchen", {"value": True})
        assert result is not None, "no match for usage:water_kitchen"
        topic, payload = result
        assert topic == 'usage_kitchen'
        assert payload['value']

    def test_match_3(self, adapter, cfg, cfg_outbound):
        result = adapter.handle_event(0, "sensor", "usage", "cupboard_kitchen", {"value": True})
        assert result is None, "unexpected match for usage:cupboard_kitchen"

    def test_match_4(self, adapter, cfg, cfg_outbound):
        result = adapter.handle_event(0, "control", "switch", "living", {"value": True})
        assert result is None, "unexpected match for control:switch:living"


@pytest.fixture
def cfg_builder(adapter, cfg):
    return adapter.configure(cfg['outbound.builder'])


def wisdom_builder(mqtt_topic, event, group_dict, adapter):
    msg = {
        'origin_type': 'device',
        'origin': 'cstbox.gerhome',
        'observation': mqtt_topic,
        "timestamp": event.timestamp,
        'variable_value': [
            {'timestamp': event.timestamp, 'value': event.payload['value']}
        ]
    }
    return msg


class TestBuilder(object):
    def test_builder(self, adapter, cfg, cfg_builder):
        import time
        now = int(time.time())
        value = 35

        result = adapter.handle_event(now, "sensor", "meter", "main", {"value": value})
        assert result
        topic, payload = result
        assert topic == '/data/observation/main_meter'

        values = payload["variable_value"]
        assert len(values) == 1
        assert values[0]['timestamp'] == now
        assert values[0]['value'] == value

        # import json
        # json.dump(payload, sys.stdout, indent=4)
