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
from paho.mqtt import client as mqtt_client

from pycstbox import dbuslib
from pycstbox import evtmgr
from pycstbox.log import Loggable

from .core import ConfigurableGatewayMixin
from .errors import MQTTGatewayError

SERVICE_NAME = "MQTTGateway"

ROOT_BUS_NAME = dbuslib.make_bus_name(SERVICE_NAME)
OBJECT_PATH = "/service"
SERVICE_INTERFACE = dbuslib.make_interface_name(SERVICE_NAME)


class MQTTGatewayServiceObject(dbus.service.Object, ConfigurableGatewayMixin, Loggable):
    """ The service object wrapping the gateway process.

    It takes care of all the generic tasks. It :

        - listens to CSTBox buses for processing circulating messages for forwarding them in the MQTT world
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
    _mqttc = None
    _cbx_publishers = {}
    _lock = None
    _inbound_adapter = None
    _outbound_adapters = {}

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

        Consists in configuring the gateway adapters and the MQTT connector, which is then
        connected to its event handler.

        :param dict cfg: configuration data as a dictionary
        """
        self._mqttc, self._inbound_adapter, self._outbound_adapters = \
            ConfigurableGatewayMixin.configure(self, cfg)

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
            try:
                adapter = self._outbound_adapters[channel]
            except KeyError:
                pass
            else:
                with self._lock:
                    mqtt_message = adapter.handle_event(timestamp, var_type, var_name, data)
                    if mqtt_message:
                        self.log_debug(
                            "forwarded to MQTT: timestamp=%s channel=%s var_type=%s var_name=%s data=%s",
                            timestamp, channel, var_type, var_name, data)
                        result, mid = self._mqttc.publish(*mqtt_message)
                        if result != mqtt_client.MQTT_ERR_SUCCESS:
                            raise MQTTGatewayError("publish failed (rc=%d)" % result)

        return _event_handler

    def start(self):
        """ Starts the service object.
        """
        self.log_info('starting...')

        # connect to CSTBox event channels
        for channel, adapter in self._outbound_adapters.iteritems():
            try:
                svc = evtmgr.get_object(channel)
            except dbus.exceptions.DBusException as e:
                msg = "cannot connect to event channel : %s (%s)" % (channel, e)
                self.log_error(msg)
                raise MQTTGatewayError(msg)
            else:
                svc.connect_to_signal("onCSTBoxEvent",
                                      self._channel_event_handler(channel),
                                      dbus_interface=evtmgr.SERVICE_INTERFACE)
                self.log_info('connected to event channel : %s', channel)

        # connect MQTT message handler and start the client
        self._mqttc.on_message = self._on_message
        self._mqttc.run()

        self.log_info('started.')

    def stop(self):
        """ Stops the service object
        """
        self._mqttc.shutdown()
        self.log_info('stopped.')
