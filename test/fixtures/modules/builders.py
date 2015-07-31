#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


def mqtt_payload_builder(mqtt_topic, event, group_dict, extra, adapter):
    return {
        'value': event.data['value']
    }


not_callable = 42


def wrong_sig():
    pass
