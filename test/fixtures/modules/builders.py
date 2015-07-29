#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


def mqtt_payload_builder(mqtt_topic, event, group_dict, adapter):
    event_payload = event[-1]
    return {'value': event_payload['value']}


not_callable = 42


def wrong_sig():
    pass
