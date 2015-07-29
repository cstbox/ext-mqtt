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

""" Base classes and definitions used for the MQTT gateway.

This gateway is basically based on the :py:class:`MQTTConnector` class, which
wraps the connection with the MQTT broken in a convenient way, and offers the proper
extension points for assembling the complete gateway.
"""
from pycstbox.mqtt.errors import MQTTConnectionError, MQTTNotConnectedError, ConfigurationError

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

__all__ = [
    'MQTTConnector',
    'InboundAdapter', 'OutboundAdapter'
]

import time
import inspect

from paho.mqtt import client as mqtt_client

from pycstbox.log import DummyLogger
from pycstbox.sysutils import symbol_for_name
from .config import Configurable, CFG_ADAPTER_CLASS, CFG_DEFAULT_BUILDER


class MQTTConnector(object):
    """ The base class taking care of bridging the CSTBox internal message bus and the
    MQTT one.

    This implementation uses `paho MQTT client <https://pypi.python.org/pypi/paho-mqtt>`_.
    """
    STATUS_DISCONNECTED, STATUS_CONNECTED, STATUS_ERROR = range(3)

    def __init__(
            self, broker, port, keep_alive=60, client_id=None, login=None, password=None, tls=None, topics=None,
            logger=None
    ):
        """
        :param str broker: the broker server public name or IP
        :param int port: the server connection port
        :param int keep_alive: delay (in seconds) during which the connection is maintained in the absence of communication
        :param str client_id: optional client identification, for further exchanges tracking
        :param str login: optional authentication login
        :param str password: optional authentication password
        :param tls: configuration for TLS if used (not used in current implementation)
        :param topics: an optional list of MQTT topics to subscribe to when connected
        :param logger: optional logger
        """
        self._broker = broker
        self._port = port
        self._keep_alive = keep_alive
        self._auth = {'username': login, 'password': password} if login else None
        self._tls = tls
        self._logger = logger or DummyLogger()
        self._logger_mqtt = logger.getChild('paho') if logger else self._logger
        self._topics = topics or []

        self._mqttc = mqtt_client.Client(client_id=client_id)
        if tls:
            self._logger.info('configuration: TLS')
            self._mqttc.tls_set(**tls)
        if login:
            self._logger.info('configuration: authentication')
            self._mqttc.username_pw_set(login, password)

        # connects callbacks
        self._mqttc.on_connect = self._on_connect
        self._mqttc.on_disconnect = self._on_disconnect
        self._mqttc.on_subscribe = self._on_subscribe
        # self._mqttc.on_unsubscribe = self._on_unsubscribe
        self._mqttc.on_publish = self._on_publish
        self._mqttc.on_log = self._on_log
        self._logger.info('callbacks wired')

        self._log_dispatch = {
            mqtt_client.MQTT_LOG_INFO: self._logger_mqtt.info,
            mqtt_client.MQTT_LOG_NOTICE: self._logger_mqtt.info,
            mqtt_client.MQTT_LOG_WARNING: self._logger_mqtt.warning,
            mqtt_client.MQTT_LOG_ERR: self._logger_mqtt.error,
            mqtt_client.MQTT_LOG_DEBUG: self._logger_mqtt.debug,
        }

        # exposes methods of the embedded MQTT client as our own ones when we had nothing special to do
        # self.subscribe = self._mqttc.subscribe
        # self.unsubscribe = self._mqttc.unsubscribe
        self.user_data_set = self._mqttc.user_data_set

        self._status = self.STATUS_DISCONNECTED

    def _on_connect(self, client, userdata, flags, rc):
        self._logger_mqtt.info('connected (rc=%s)', rc)
        self._status = self.STATUS_CONNECTED if rc == mqtt_client.MQTT_ERR_SUCCESS else self.STATUS_ERROR

        if self._topics:
            self._logger_mqtt.info("subscribing to '%s'...", self._topics)
            result, mid = self._mqttc.subscribe([(topic, 0) for topic in self._topics])
            if result == mqtt_client.MQTT_ERR_SUCCESS:
                self._logger_mqtt.info('... success')

            else:
                self._logger_mqtt.error('... failed')

    def _on_disconnect(self, client, userdata, rc):
        self._logger_mqtt.info('disconnected')
        self._status = self.STATUS_DISCONNECTED

    def publish(self, *args, **kwargs):
        """ Publish a message on MQTT.

        This method is a facade of the MQTT client homonym one, checking that the communication is established
        before passing the call.

        It uses the same signature as the homonym method of the paho Client. Refer to its
        `documentation <https://pypi.python.org/pypi/paho-mqtt>`_ for details.
        """
        if not self.connected:
            raise MQTTNotConnectedError('not connected')
        return self._mqttc.publish(*args, **kwargs)

    @property
    def connected(self):
        """ Tells if we are currently connected to the broker.
        """
        return self._status == self.STATUS_CONNECTED

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        self._logger_mqtt.info('subscribe done')

    def _on_unsubscribe(self, client, userdata, mid):
        self._logger_mqtt.info('unsubscribe done')

    @property
    def on_message(self):
        """ Handler of MQTT messages.

        See `paho MQTT Client documentation <https://pypi.python.org/pypi/paho-mqtt>`_ for details
        about the signature of the callback.
        """
        return self._mqttc.on_message

    @on_message.setter
    def on_message(self, func):
        self._mqttc.on_message = func

    def _on_publish(self, client, userdata, mid):
        self._logger_mqtt.info('message published')

    def _on_log(self, client, userdata, level, buf):
        self._log_dispatch[level]("(client: %s) %s" % (client, buf))

    def run(self):
        """ Starts the connector.

        Establish the connection with the MQTT broker, and starts the client loop in threaded mode. Subscription
        to the MQTT topics of interest (if any) is done in the :py:meth:`_on_connect` private callback,
        to avoid race conditions.
        """
        if not self.connected:
            self._logger.info('connecting to %s:%d...', self._broker, self._port)
            rc = self._mqttc.connect(self._broker, self._port, self._keep_alive)
            if rc != mqtt_client.MQTT_ERR_SUCCESS:
                raise MQTTConnectionError(rc)

            self._mqttc.loop_start()
            self._logger.info('threaded loop started')
            while self._status == self.STATUS_DISCONNECTED:
                time.sleep(0.1)

        else:
            self._logger.warning('already active')

    def shutdown(self):
        """ Shutdowns the connector.
        """
        if self.connected:
            self._mqttc.disconnect()
            self._mqttc.loop_stop()
            self._logger.info('shutdown complete')

        else:
            self._logger.warning('not currently active')


class ConfigurableAdapter(Configurable):
    """ Base class for any type of configurable adapter, handling shared configuration processing

    Derived classes must implement the :py:meth:`_configure` method to provide their specific
    configuration handling, which is invoked at the end of the shared processing.
    """
    def configure(self, cfg):
        """ Processes and applies the adapter specific configuration.

        This method handles shared configuration process, and then delegates to :py:meth:`_configure`
        for the rest of the process.

        The configuration data is expected to contain at least the following key:

            class_name:
                the fully qualified name of the class implementing the adapter

        This key is used here to check that the instance matches the configuration. The rest of
        the content depends on the concrete class.

        If specified, the default payload builder class is resolved, and the dictionary entry containing
        its name is replace by the class itself.

        :param dict cfg: the configuration data
        :raises ConfigurationError: if the configuration is not valid
        """
        # sanity check : does the class specified in the configuration match with ours ?

        # use a generalised comparison, able to deal with classes FQN as well as with classes directly
        cfg_class = cfg[CFG_ADAPTER_CLASS]
        my_class = self.__class__ if inspect.isclass(cfg_class) else self.__module__ + '.' + self.__class__.__name__

        if my_class != cfg_class:
            msg = 'class type mismatch (%s != %s)' % (my_class, cfg_class)
            self.logger.error(msg)
            raise ConfigurationError(msg)

        # resolve the default builder if here
        try:
            fqn = cfg[CFG_DEFAULT_BUILDER]
            cfg[CFG_DEFAULT_BUILDER] = self.resolve_builder(fqn)
        except KeyError:
            pass

        self._configure(cfg)

    def resolve_builder(self, fqn):
        # short-circuit the case where the resolution has already been done
        if inspect.isclass(fqn):
            return fqn

        try:
            builder = symbol_for_name(fqn)
        except (ImportError, NameError) as e:
            raise ConfigurationError('builder not found: %s' % fqn)
        except Exception as e:
            raise ConfigurationError("error resolving %s: %s" % (fqn, e))
        else:
            if not callable(builder):
                raise ConfigurationError("builder is not callable (%s)" % fqn)

            args, _, _, _ = inspect.getargspec(builder)
            if len(args) != 4:
                raise ConfigurationError('builder signature mismatch (%s)' % fqn)

            return builder

    def _configure(self, cfg):
        """ Sub-classes specific configuration process.

        To be overridden by sub-classes, since this implementation does nothing.

        :param dict cfg: the configuration data
        """


class InboundAdapter(ConfigurableAdapter):
    """ Base class for implementing adapters for incoming MQTT messages.

    Its role is to translate CSTBox events into corresponding MQTT messages.

    .. IMPORTANT::

        To be sub-classed, since the default implementation discards incoming MQTT messages.

    """
    _mqtt_subscr_list = []

    def handle_message(self, client, user_data, message):
        """ A generator yielding CSTBox events to be emitted corresponding to the passed MQTT message.

        The generator yields the corresponding CSTBox event(s) which must be published on CSTBox
        message bus in case the MQTT event is accepted, and the target channel for each one.

        The returned events are either instances of :py:class:`BasicEvent` or the equivalent tuples
        (see class definition for their attributes). To ensure the consistency of the time line, the CSTBox events
        will be dated by the event manager at publication time. Consequently, the time stamp included in
        the MQTT message will be discarded.

        The generator must return nothing for discarding the incoming MQTT event.

        :param client: the MQTT client
        :param user_data: application dependant user data transmitted by the caller
        :param message: the MQTT message
        :return: CSTBox events and associated channel
        :rtype: tuple
        """
        return

    def get_listened_topics(self):
        """ Returns the list of MQTT topics to be subscribed to.

        The default implementation returns an empty list, equivalent to accept no MQTT message.

        :return: the list of topics
        :rtype: list
        """
        return self._mqtt_subscr_list


class OutboundAdapter(ConfigurableAdapter):
    """ Base class for implementing adapters for outgoing CSTBox events.

    Its role is to define if a given CSTBox message can be published on the MQTT domain,
    and in this case which is the relevant MQTT topic to be used.

    .. IMPORTANT::

        To be sub-classed, since the default implementation forbid any event
        to go outside of the CSTBox domain.
    """
    def handle_event(self, timestamp, channel, var_type, var_name, data):
        """ Returns the MQTT topic and payload, conforming the broker conventions and corresponding to the CSTBox
        candidate message.

        Return None if the message is not to be passed to the MQTT world (which is the behaviour of the
        default implementation).

        :param int timestamp: the event time stamp
        :param str channel: the event channel
        :param str var_type: the type of the variable the event is related to
        :param str var_name: the name of the variable
        :param dict data: the event specific payload
        :return: the MQTT topic and payload to be used, or None if the message is not allowed to go outside
        :rtype: tuple
        """
        return None
