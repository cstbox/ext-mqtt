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

""" D-Bus binding layer of the MQTT gateway.

This module implements the building blocks for interfacing the :py:class:`MQTTConnector` class
with the CSTBox D-Bus layer.

The feature is provided by the :py:class:`MQTTGatewayServiceObject` which is the worker instance
of the service.

The gateway service object works in association with the filters provided by the
application. The role of these filters is to filter messages allowing to cross the CSTBox/MQTT
boundary, and to translate accepted ones. Their prototypes is specified by classes :py:class:`InboundFilter`
and :py:class:`OutboundFilter` provided by the package module :py:mod:`pycstbox.mqtt.core`.
"""

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import dbus.service
import json
import time
import importlib
import threading

from pycstbox import dbuslib
from pycstbox import evtmgr
from pycstbox.log import Loggable
from .core import MQTTConnector, MQTTGatewayError, InboundFilter, OutboundFilter

SERVICE_NAME = "MQTTGateway"

ROOT_BUS_NAME = dbuslib.make_bus_name(SERVICE_NAME)
OBJECT_PATH = "/service"
SERVICE_INTERFACE = dbuslib.make_interface_name(SERVICE_NAME)

CFG_SERVICE_OBJECT_CLASS = 'svcobj_class'


def load_configuration(cfgfile_path):
    """ Loads the configuration from a file and returns it as a dictionnary.

    :param str cfgfile_path: configuration file path
    :returns: the configuration data
    :rtype: dict
    :raises ValueError: if path is not provided of configuration data are invalid
    :raises IOError: if not found or not a file
    """
    if not cfgfile_path:
        raise ValueError('missing parameter : cfgfile_path')
    with file(cfgfile_path, 'rt') as fp:
        cfg = json.load(fp)

    wrk = cfg['svcobj_class_name'].split('.')
    module_name, class_name = '.'.join(wrk[:-1]), wrk[-1]
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        raise ValueError('module not found : %s' % module_name)

    try:
        clazz = getattr(module, class_name)
    except AttributeError:
        raise ValueError('class not found : %s' % wrk)

    cfg[CFG_SERVICE_OBJECT_CLASS] = clazz

    return cfg


class MQTTGatewayServiceObject(Loggable, dbus.service.Object):
    """ The service object wrapping the gateway process.

    It takes care of all the generic tasks. It :

        - listens to both sensor and control CSTBox buses for processing circulating messages for
          forwarding them in the MQTT world
        - creates a MQTT client for the broker, and listens to the relevant MQTT topîcs,
          for injecting the corresponding messages in the CSTBox internal communication bus

    In both cases, messages are first passed to an application dependant filtering layer, which decides
    which ones are allowed to cross the boundaries. This layer takes also care of converting the messages
    in the proper form before publishing.

    It is the responsibility of the application to provide these filters when creating an instance of this
    service object (refer to the `__init__` method signature). It is valid to omit one of them, the absence of
    filter meaning that nothing will go through in the corresponding direction.
    """
    DEFAULT_BROKER_PORT = 61613     #: default MQTT broker connection port

    _mqttc = None
    _cbx_publishers = {}
    _lock = None
    _inbound_filter = None
    _outbound_filter = None

    def __init__(self, cfg, inbound_filter=None, outbound_filter=None):
        """
        The service object is configured based on the content of the configuration data, provided as
        a dictionary by the process which instantiates it.

        See :py:meth:`configure` method documentation for specifications of the configuration data.

        Note that providing no filter at all is invalid, since it would make the gateway useless.

        :param dict cfg: the configuration data
        :param InboundFilter inbound_filter: the filter for MQTT to CSTBox messages (default: None)
        :param OutboundFilter outbound_filter: the filter for CSTBox to MQTT messages (default: None)
        :raises ValueError: if both filters are unset
        """
        super(MQTTGatewayServiceObject, self).__init__()
        Loggable.__init__(self, logname='SO')

        if not (inbound_filter or outbound_filter):
            raise ValueError('no message filter were provided')

        self._inbound_filter = inbound_filter
        self._outbound_filter = outbound_filter

        self._lock = threading.Lock()

        self.configure(cfg)

    def configure(self, cfg):
        """ Configures the service object.

        The configuration data are provided as a dictionary, which keys and structure are :

            - `broker` : sub-dictionary providing the broker connection information

                - `host` : public host name or IP of the server
                - `port` : connection port, defaulted to :py:attr:`DEFAULT_BROKER_PORT` if not
                  provided
                - `client_id` : optional identifier attached to the MQTT client

            - `auth` : optional sub-dictionary containing the authentication information

                - `username` : login
                - `password` : guess what...

            - `listened_topics` : optional list of MQTT topics or topic patterns the gateway must subscribe

        :param dict cfg: configuration data as a dictionary
        """
        try:
            cfg_broker = cfg['broker']
            broker_host = cfg_broker['host']
            broker_port = cfg_broker.get('port', self.DEFAULT_BROKER_PORT)
            client_id = cfg_broker.get('client_id', self.DEFAULT_BROKER_PORT)

            cfg_auth = cfg.get('auth', None)
            if cfg_auth:
                login = cfg_auth['username']
                password = cfg_auth['password']
            else:
                login = password = None

            topics = cfg.get('listened_topics', None)

            # TODO handle TLS parms et al.

        except KeyError:
            raise ValueError('invalid configuration data')

        else:
            self._mqttc = MQTTConnector(
                broker=broker_host,
                port=broker_port,
                client_id=client_id,
                login=login,
                password=password,
                topics=topics,
                logger=self
            )

            self._mqttc.on_message = self._on_message

    def _on_message(self, client, user_data, message):
        if self._inbound_filter:
            with self._lock:
                pub_list = self._inbound_filter.accept_event(client, user_data, message)
                if pub_list:
                    timestamp = int(time.time() * 1000)
                    for event, channel in pub_list:
                        try:
                            publisher = self._cbx_publishers[channel]
                        except KeyError:
                            self._cbx_publishers[channel] = publisher = evtmgr.get_object(channel)
                        publisher.emitFullEvent(timestamp, event.var_type, event.var_name, event.data)

    def _event_signal_handler(self, timestamp, var_type, var_name, data):
        if self._outbound_filter:
            with self._lock:
                mqtt_topic = self._outbound_filter.get_mqtt_topic(timestamp, var_type, var_name, data)
                if mqtt_topic:
                    self.log_debug(
                        "forwarded to MQTT: timestamp=%s var_type=%s var_name=%s data=%s",
                        timestamp, var_type, var_name, data)
                    payload = {
                        'var_type': var_type,
                        'data': data
                    }
                    self._mqttc.publish(mqtt_topic, payload)

    def start(self):
        """ Starts the service object.
        """
        self.log_info('starting...')
        # collects first all the event manager service objects we need before
        # connecting to anything
        svcs = []
        for channel in (evtmgr.SENSOR_EVENT_CHANNEL, evtmgr.CONTROL_EVENT_CHANNEL):
            try:
                svc = evtmgr.get_object(channel)
            except dbus.exceptions.DBusException as e:
                self.log_error("cannot connect to event channel : %s (%s)", channel, e)
                raise MQTTGatewayError()
            else:
                svcs.append((channel, svc))

        # everybody is here, so we can start to work
        for channel, svc in svcs:
            svc.connect_to_signal("onCSTBoxEvent",
                                  self._event_signal_handler,
                                  dbus_interface=evtmgr.SERVICE_INTERFACE)
            self.log_info('connected to event channel : %s', channel)

        self._mqttc.run()
        self.log_info('started.')

    def stop(self):
        """ Stops the service object
        """
        self._mqttc.shutdown()
        self.log_info('stopped.')
