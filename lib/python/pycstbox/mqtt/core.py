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

import time
import inspect
import json

from paho.mqtt import client as mqtt_client

from pycstbox.log import DummyLogger, Loggable
from pycstbox.evtmgr import ALL_CHANNELS

from .config import *
from .errors import MQTTConnectionError, MQTTNotConnectedError, ConfigurationError

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

__all__ = [
    'MQTTConnector',
    'InboundAdapter', 'OutboundAdapter'
]


class MQTTConnector(Loggable):
    """ The base class taking care of bridging the CSTBox internal message bus and the
    MQTT one.

    This implementation uses `paho MQTT client <https://pypi.python.org/pypi/paho-mqtt>`_.
    """
    STATUS_DISCONNECTED, STATUS_CONNECTED, STATUS_ERROR = range(3)

    def __init__(
            self, broker, port, keep_alive=60, client_id=None, login=None, password=None, tls=None, topics=None,
            simulate=False,
            **kwargs
    ):
        """
        :param str broker: the broker server public name or IP
        :param int port: the server connection port
        :param int keep_alive: delay (in seconds) during which the connection is maintained in the absence of communication
        :param str client_id: optional client identification, for further exchanges tracking
        :param str login: optional authentication login
        :param str password: optional authentication password
        :param tls: optional configuration for TLS if used
        :param topics: an optional list of MQTT topics to subscribe to when connected
        :param bool simulate: if True, do no perform any network operation, but trace them instead
        :param kwargs: parameters for base class
        """
        super(MQTTConnector, self).__init__(**kwargs)

        self._broker = broker
        self._port = port
        self._keep_alive = keep_alive
        self._auth = {'username': login, 'password': password} if login else None
        self._tls = tls
        self._logger_mqtt = self.logger.getChild('paho')
        self._topics = topics or []
        self._simulate = simulate
        self._status = self.STATUS_DISCONNECTED
        self._mqtt_loop_active = False

        if not simulate:
            self._mqttc = mqtt_client.Client(client_id=client_id)
            if tls:
                self.log_info('configuring TLS encryption')
                self._mqttc.tls_set(**tls)
            else:
                self.log_warning('no communication encryption')

            if login:
                self.log_info('configuring authentication')
                self._mqttc.username_pw_set(login, password)
            else:
                self.log_warning('no authentication provided')

        else:
            self._mqttc = MQTTSimulatedClient(logger=self.logger)
            self._status = self.STATUS_CONNECTED

        # connects callbacks
        self._mqttc.on_connect = self._on_connect
        self._mqttc.on_disconnect = self._on_disconnect
        self._mqttc.on_subscribe = self._on_subscribe
        # self._mqttc.on_unsubscribe = self._on_unsubscribe
        self._mqttc.on_publish = self._on_publish
        self._mqttc.on_log = self._on_log
        self.log_info('callbacks wired')

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

    def _on_connect(self, client, userdata, flags, rc):
        self._logger_mqtt.info('connected (rc=%s)', rc)
        self._status = self.STATUS_CONNECTED if rc == mqtt_client.MQTT_ERR_SUCCESS else self.STATUS_ERROR

        if self.connected and self._topics:
            self._logger_mqtt.info("subscribing to '%s'...", self._topics)
            result, mid = self._mqttc.subscribe([(topic, 0) for topic in self._topics])
            if result == mqtt_client.MQTT_ERR_SUCCESS:
                self._logger_mqtt.info('... success')

            else:
                self._logger_mqtt.error('... failed')

    def _on_disconnect(self, client, userdata, rc):
        self._logger_mqtt.info('disconnected')
        self._status = self.STATUS_DISCONNECTED

    def publish(self, topic, payload, *args, **kwargs):
        """ Publish a message on MQTT.

        This method is a facade of the MQTT client homonym one, checking that the communication is established
        before passing the call.

        In addition, it automatically serialise complex payloads (i.e. dict, list, tuples) as JSON before
        sending them.

        It uses the same signature as the homonym method of the paho Client. Refer to its
        `documentation <https://pypi.python.org/pypi/paho-mqtt>`_ for details.
        """
        if not self.connected:
            self.log_warning('connecting before publishing the message...')
            self._connect()
            if not self.connected:
                raise MQTTNotConnectedError('not connected')

        if isinstance(payload, (dict, list, tuple)):
            payload = json.dumps(payload)
        self.log_info('publishing message (topic=%s payload=%s)', topic, payload)
        return self._mqttc.publish(topic, payload, *args, **kwargs)

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

    def _connect(self, timeout=10, loop_delay=2):
        if self.connected:
            self.log_warning('connection request ignored: already connected')
            return

        self.log_info('connecting to broker at "%s:%d"...', self._broker, self._port)
        rc = self._mqttc.connect(self._broker, self._port, self._keep_alive)
        if rc != mqtt_client.MQTT_ERR_SUCCESS:
            self.log_error('connection request failed (rc=%d)', rc)
            return

        self.log_info('waiting for broker connection to be established...')
        time_left = timeout
        while time_left:
            if self._status == self.STATUS_CONNECTED:
                self.log_info('... connected')
                return

            self.log_info('... time left: %d secs' % time_left)
            time.sleep(loop_delay)
            time_left -= loop_delay

        self.log_error('connection failed')

    def run(self):
        """ Starts the connector.

        Establish the connection with the MQTT broker, and starts the client loop in threaded mode. Subscription
        to the MQTT topics of interest (if any) is done in the :py:meth:`_on_connect` private callback,
        to avoid race conditions.
        """
        if self._mqtt_loop_active:
            self.log_warning('already active')
            return

        self._mqttc.loop_start()
        self._mqtt_loop_active = True
        self.log_info('connector loop started')

        if not self.connected:
            self.log_info('attempting first connection to MQTT broker...')
            self._connect()

        if self.connected:
            self.log_info('connector started and online with broker')
        else:
            self.log_warning('connector started without broker connection')

    def shutdown(self):
        """ Shutdowns the connector.
        """
        if not self._mqtt_loop_active:
            self.log_warning('not currently active')
            return

        if self.connected:
            self.log_info('disconnecting')
            self._mqttc.disconnect()

        self._mqttc.loop_stop()
        self._mqtt_loop_active = False

        self.log_info('shutdown complete')


class ConfigurableAdapter(Configurable):
    """ Base class for any type of configurable adapter, handling shared configuration processing

    Derived classes must implement the :py:meth:`_configure` method to provide their specific
    configuration handling, which is invoked at the end of the shared processing.
    """
    builder_args_count = None

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
        if inspect.isfunction(fqn):
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

            if self.builder_args_count is not None:
                args, _, _, _ = inspect.getargspec(builder)
                if len(args) != self.builder_args_count:
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

    An adapter is devoted to a given event channel. Its role is to listen to events on it, to filter events that
    will be published and and to build the MQTT message to be sent.

    .. IMPORTANT::

        To be sub-classed, since the default implementation of the event handler returns None, and thus forbids
        any event to go outside of the CSTBox domain.
    """
    def handle_event(self, timestamp, var_type, var_name, data):
        """ Returns the MQTT topic and payload, conforming the broker conventions and corresponding to the CSTBox
        candidate message.

        Return None if the message is not to be passed to the MQTT world (which is the behaviour of the
        default implementation).

        :param int timestamp: the event time stamp
        :param str var_type: the type of the variable the event is related to
        :param str var_name: the name of the variable
        :param dict data: the event specific payload
        :return: the MQTT topic and payload to be used, or None if the message is not allowed to go outside
        :rtype: tuple
        """
        return None


class ConfigurableGatewayMixin(Configurable):
    """ This mixin takes care of initializing and configuring the gateway core components,
    i.e. the MQTT connector, the inbound adapter and the outbound adapters.
    """

    def configure(self, cfg, logger=None):
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

            - `tls` : optional sub-dictionary containing the TLS related parameters

                - `ca_certs` : path to the Certificate Authority certificate files that are
                  to be treated as trusted by this client
                - `certfile` : path to the PEM encoded client certificate
                - `keyfile` : path to the private key

            - 'simulate' : if ``true``, do not really send the data but display them instead

        :param dict cfg: configuration data as a dictionary
        :param logger: an optional logger which will be passed to participating Loggable instances
        :return: the MQTT connector instance, the inbound adapter, the outbound adapters
        :rtype: tuple
        """
        try:
            cfg_adapters = cfg[CFG_ADAPTERS]
            if not cfg_adapters:
                raise ConfigurationError('at least one of input or output adapter must be configured')

            # configure the input adapter
            # TODO inbound_adapter = self._configure_adapter(cfg_adapters.get(CFG_INBOUND, None))
            inbound_adapter = None

            # configure the output adapters (one per channel)
            outbound_adapters = {}
            try:
                cfg_outbound = cfg_adapters[CFG_OUTBOUND]
            except KeyError:
                pass
            else:
                for channel, cfg_channel in cfg_outbound[CFG_CHANNELS].iteritems():
                    if channel in ALL_CHANNELS:
                        outbound_adapters[channel] = self._configure_adapter(cfg_channel)
                    else:
                        raise ConfigurationError('invalid channel: %s' % channel)

        except ValueError as e:
            raise ConfigurationError(e)

        except ConfigurationError:
            raise

        else:
            mqttc = self._configure_mqtt_connector(
                cfg,
                inbound_adapter.get_listened_topics() if inbound_adapter else None,
            )

        return mqttc, inbound_adapter, outbound_adapters

    def _configure_mqtt_connector(self, cfg, listened_topics=None):
        try:
            cfg_broker = cfg[CFG_BROKER]
            broker_host = cfg_broker[CFG_HOST]
            broker_port = cfg_broker.get(CFG_PORT, DEFAULT_BROKER_PORT)
            client_id = cfg_broker.get(CFG_CLIENT_ID, DEFAULT_CLIENT_ID)
            simulate = cfg.get(CFG_SIMULATE, False)

            cfg_auth = cfg.get(CFG_AUTH, None)
            if cfg_auth:
                login = cfg_auth[CFG_USER]
                password = cfg_auth[CFG_PASSWORD]
            else:
                login = password = None

            tls = cfg.get(CFG_TLS, None)

        except KeyError:
            raise ValueError('invalid configuration data')

        else:
            mqttc = MQTTConnector(
                broker=broker_host,
                port=broker_port,
                client_id=client_id,
                login=login,
                password=password,
                tls=tls,
                topics=listened_topics,
                logger=None,
                simulate=simulate
            )
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


class MQTTSimulatedClient(mqtt_client.Client, Loggable):
    def __init__(self, logger, *args, **kwargs):
        Loggable.__init__(self, logger)
        self.log_warning('using a simulated connector')
        mqtt_client.Client.__init__(self, *args, **kwargs)

    def publish(self, topic, payload=None, **kwargs):
        self.log_info('published: topic=%s payload=%s', topic, payload)

    def connect(self, host, port=1883, keepalive=60, bind_address=""):
        self.on_connect(None, None, None, mqtt_client.MQTT_ERR_SUCCESS)
        return mqtt_client.MQTT_ERR_SUCCESS