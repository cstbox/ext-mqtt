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

import time
import threading

import dbus.service

from pycstbox import dbuslib
from pycstbox import evtmgr
from pycstbox.log import Loggable
from .core import MQTTConnector, InboundAdapter, OutboundAdapter
from .config import *

SERVICE_NAME = "MQTTGateway"

ROOT_BUS_NAME = dbuslib.make_bus_name(SERVICE_NAME)
OBJECT_PATH = "/service"
SERVICE_INTERFACE = dbuslib.make_interface_name(SERVICE_NAME)


class MQTTGatewayServiceObject(Loggable, dbus.service.Object):
    """ The service object wrapping the gateway process.

    It takes care of all the generic tasks. It :

        - listens to both sensor and control CSTBox buses for processing circulating messages for
          forwarding them in the MQTT world
        - creates a MQTT client for the broker, and listens to the relevant MQTT topics,
          for injecting the corresponding messages in the CSTBox internal communication bus

    In both cases, messages are first passed to an application dependant adaptation layer, which decides
    which ones are allowed to cross the boundaries, and how to translate message payloads from one
    domain to the other. The adaptation layer is basically made of classes based on :py:class:`InboundAdapter`
    and :py:class:`OutboundAdapter`.

    It is the responsibility of the application to provide these adapters when creating an instance of this
    service object (:py:meth:`__init__` method `input_adapter` and `output_adapter` parameters).
    It is valid to omit one of them, the absence of an adapter meaning that nothing will go through in the
    corresponding direction.
    """
    DEFAULT_BROKER_PORT = 61613     #: default MQTT broker connection port

    _mqttc = None
    _cbx_publishers = {}
    _lock = None
    _inbound_adapter = None
    _outbound_adapter = None

    def __init__(self, cfg):
        """
        The service object is configured based on the content of the configuration data, provided as
        a dictionary by the process which instantiates it.

        See :py:meth:`configure` method documentation for specifications of the configuration data.

        Note that providing no filter at all is invalid, since it would make the gateway useless.

        :param dict cfg: the configuration data
        :raises ValueError: if both filters are unset
        """
        super(MQTTGatewayServiceObject, self).__init__()
        Loggable.__init__(self, logname='SO')

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
            # configure the adapters, passing them their specific sub-dictionary
            cfg_adapters = cfg[CFG_ADAPTERS]
            if not cfg_adapters:
                raise ConfigurationError('at least one of input or output adapter must be configured')

            self._inbound_adapter = self._configure_adapter(cfg_adapters.get(CFG_INBOUND, None))
            self._outbound_adapter = self._configure_adapter(cfg_adapters.get(CFG_OUTBOUND, None))

        except ValueError as e:
            raise ConfigurationError(e)

        except ConfigurationError:
            raise

        else:
            self._mqttc = self._configure_mqtt_connector(cfg)

    def _configure_mqtt_connector(self, cfg):
        try:
            cfg_broker = cfg[CFG_BROKER]
            broker_host = cfg_broker[CFG_HOST]
            broker_port = cfg_broker.get(CFG_PORT, self.DEFAULT_BROKER_PORT)
            client_id = cfg_broker.get(CFG_CLIENT_ID, self.DEFAULT_BROKER_PORT)

            cfg_auth = cfg.get(CFG_AUTH, None)
            if cfg_auth:
                login = cfg_auth[CFG_USER]
                password = cfg_auth[CFG_PASSWORD]
            else:
                login = password = None

            # TODO handle TLS parms et al.

        except KeyError:
            raise ValueError('invalid configuration data')

        else:
            topics = self._inbound_adapter.get_listened_topics() if self._inbound_adapter else None
            mqttc = MQTTConnector(
                broker=broker_host,
                port=broker_port,
                client_id=client_id,
                login=login,
                password=password,
                topics=topics,
                logger=self
            )

            mqttc.on_message = self._on_message
            return mqttc

    def _configure_adapter(self, cfg):
        """ Creates and configures an adapter based on the configuration data.

        Does nothing and returns None if the configuration contains no data or is None.

        :param dict cfg: the inbound adapter configuration data
        :return: the created adapter (or None)
        :rtype: :py:class:`InboundAdapter` or :py:class:`OutboundAdapter`
        """
        if not cfg:
            return None

        klass = symbol_for_name(cfg[CFG_ADAPTER_CLASS])
        adapter = klass()
        adapter.configure(cfg)
        return adapter

    def _on_message(self, client, user_data, message):
        if self._inbound_adapter:
            with self._lock:
                now = int(time.time() * 1000)
                for event, channel in self._inbound_adapter.handle_message(client, user_data, message):
                    try:
                        publisher = self._cbx_publishers[channel]
                    except KeyError:
                        self._cbx_publishers[channel] = publisher = evtmgr.get_object(channel)
                    publisher.emitFullEvent(now, event.var_type, event.var_name, event.data)

    @staticmethod
    def _channel_event_handler(channel):
        def _event_handler(self, timestamp, var_type, var_name, data):
            if self._outbound_adapter:
                with self._lock:
                    mqtt_message = self._outbound_adapter.handle_event(timestamp, channel, var_type, var_name, data)
                    if mqtt_message:
                        self.log_debug(
                            "forwarded to MQTT: timestamp=%s channel=%s var_type=%s var_name=%s data=%s",
                            timestamp, channel, var_type, var_name, data)
                        self._mqttc.publish(*mqtt_message)

        return _event_handler

    def start(self):
        """ Starts the service object.
        """
        self.log_info('starting...')

        # collects first all the event manager service objects we need before
        # connecting to anything
        # Note: current implementation handles sensor events only, but provisions are made for extending this
        # in the future
        svcs = []
        for channel in [evtmgr.SENSOR_EVENT_CHANNEL]:
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
                                  self._channel_event_handler(channel),
                                  dbus_interface=evtmgr.SERVICE_INTERFACE)
            self.log_info('connected to event channel : %s', channel)

        self._mqttc.run()
        self.log_info('started.')

    def stop(self):
        """ Stops the service object
        """
        self._mqttc.shutdown()
        self.log_info('stopped.')
