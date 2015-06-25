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

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

__all__ = [
    'MQTTConnector',
    'MQTTConnectionError', 'MQTTNotConnectedError',
    'InboundFilter', 'OutboundFilter',
    'REInboundFilter', 'REOutboundFilter'
]

import time
import re

from pycstbox.log import DummyLogger, DEBUG
from pycstbox.evtmgr import CONTROL_EVENT_CHANNEL
from pycstbox.events import make_basic_event

from paho.mqtt import client as mqtt_client


class MQTTConnector(object):
    """ The base class taking care of bridging the CSTBox internal message bus and the
    MQTT one.

    This implementation uses `paho MQTT client <https://pypi.python.org/pypi/paho-mqtt>`_.
    """
    STATUS_DISCONNECTED, STATUS_CONNECTED, STATUS_ERROR = range(3)

    def __init__(self, broker, port, keep_alive=60, client_id=None, login=None, password=None, tls=None, topics=None, logger=None):
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


def _replace_parms(s, d):
    try:
        return s % d
    except TypeError:
        # these was no replaceable parameter
        return s


class InboundFilter(object):
    """ Prototype of the filter for incoming messages.

    Its role is to define how messages received from the MQTT domain are accepted, and
    for those which are, translated into messages injected in the CSTBox domain.

    .. IMPORTANT::

        The default implementation accept nothing by security. It has to be sub-classed by
        the application.
    """
    def accept_event(self, client, user_data, message):
        """ Tells if an incoming MQTT event is to be published on the CSTBox bus.

        The method returns the corresponding CSTBox event(s) which must be published on CSTBox
        message bus in case the MQTT event is accepted, and the target channel for each one.
        The return events are either instances of :py:class:`pycstbox.events.BasicEvent` or
        the equivalent tuples (see class definition for their attributes). To ensure the consistency
        of the time line, the CSTBox events will be dated by the event manager at publication time.
        Consequently, the time stamp included in the MQTT message will be discarded.

        The method result is an iterable of tuples, each one containing the event and the name of the
        channel on which it must be published.

        If the incoming MQTT event is not accepted, the method must return None or an empty list.

        :param client: the MQTT client instance
        :param user_data: application specific data which can be attached to the client
        :param message: the MQTT message
        :return: the list of CSTBox events to be published
        :rtype: iterable of (CSTBox event, channel name) tuples
        """
        return []


class REInboundFilter(InboundFilter):
    """ Regular expressions based inbound filter.

    It is based on regex based rules applied to the message topic, which give in return the variable
    type and variable name of the message to be emitted, with the channel on which this must be done.

    .. IMPORTANT:

        Since there is no way to define a generic method for extracting the event payload from the MQTT message,
        this is delegated to the :py:meth:`make_event_payload` abstract method which must be implemented
        by the real inbound filter.
    """
    def __init__(self, rules, logger=None):
        """ Initializes the rules list by compiling their specifications.

        The specifications are provided as an iterable of tuples, which items are :

            - a string containing the regex to be applied to the MQTT message topic
            - an events dispatch list, composed of tuples specifying each of the corresponding CSTBox events
              to be produced. Each tuple is composed of :

                - the variable type
                - the variable name
                - the channel on which the event will be emitted. If omitted, it is defaulted to
                  :py:attr:`pycstbox.evtmgr.CONTROL_EVENT_CHANNEL`

              Using an empty dispatch list is allowed, which is equivalent to block the incoming message.
              This can be useful for dynamically dispatch strategies, so that the rule can be left in place but will
              produce no CSTBox event in result to the matching MQTT message.

        :param rules: an iterable of tuples, as described above
        :param logger: optional logging object
        :raises ValueError: if invalid regex, or if the topic format contains replaceable parameters
            not found in the regex
        """
        self._logger = logger or DummyLogger()
        fmt_parms_re = re.compile(r'%\(([a-zA-Z0-9]+)\)[sfdx]')

        # cache for speeding up computations
        self._cache = {}

        # filtering and transformation transformation rules
        self._rules = []

        for s_regex, dispatch in rules:
            # ignore empty dispatch lists which can be used for explicitly block an incoming message
            if not dispatch:
                continue

            s_regex = s_regex.strip() if s_regex else None
            if not s_regex:
                raise ValueError('missing regex in rule')

            try:
                # compile the regex => ths will checks its syntax
                pattern = re.compile(s_regex)
            except re.error as e:
                raise ValueError('invalid regex (%s) : %s' % (s_regex, e))

            rule_event_specs = []
            for evt_specs in dispatch :
                var_type, var_name, channel = (evt_specs + (CONTROL_EVENT_CHANNEL,))[:3]
                # clean all strings
                var_type, var_name, channel = t = tuple(
                    s.strip() if s else None for s in (var_type, var_name, channel)
                )

                # checks that the rule is fully specified
                if not all(t):
                    raise ValueError("invalid event specification (var_type='%s'/var_name='%s'/channel='%s')" % t)

                # checks that replaceable parameters in variable type and name are all present
                # in the captured groups of the rule
                for fmt in var_type, var_name:
                    missing = set(fmt_parms_re.findall(fmt)) - set(pattern.groupindex.keys())
                    if missing:
                        raise ValueError('parameters not found in rule regex (%s)' % ', '.join(missing))

                # all is correct => register the event specification
                rule_event_specs.append(t)

            # the rule is valid => we can register it
            self._rules.append((pattern, rule_event_specs))

    def dump_rules(self):
        self._logger.debug('Filtering rules :')
        for regex, dispatch in self._rules:
            self._logger.debug('- %s -> %s' % (regex.pattern, dispatch))

    def accept_event(self, client, user_data, message):
        result = []

        mqtt_topic = message.topic
        for regex, dispatch in self._rules:
            r = regex.match(mqtt_topic)
            if r:
                gd = r.groupdict
                for evt_specs in dispatch:
                    var_type, var_name, channel = (evt_specs + (CONTROL_EVENT_CHANNEL,))[:3]
                    # expand templates with captured groups
                    var_type = _replace_parms(var_type, gd)
                    var_name = _replace_parms(var_name, gd)

                    value, extra_info = self.make_event_payload(client, user_data, message, var_type, var_name)
                    event = make_basic_event(var_type, var_name, value, **(extra_info or {}))
                    result.append((event, channel))

        return result

    def make_event_payload(self, client, user_data, message, var_type, var_name):
        """ Returns the event payload of the CSTBox event to be produced, based on the content of the incoming
        MQTT message.

        To make things easier for implementors, the result is expected as a tuple, which first item is the value,
        and the second an optional dictionary of additional information. The effective event payload will be built
        by the framework, by assembling these two pieces of data.

        This method is called by :py:meth:`accept_event` when a rule matches and corresponding variable
        type and name could thus have been determined.

        :param client: the MQTT client instance
        :param user_data: application specific data which can be attached to the client
        :param message: the MQTT message
        :param str var_type: the variable type of the CSTBox event
        :param str var_name: the variable name of the CSTBox event
        :return: the value and optional additional information to be used for the CSTBox event
        :type: tuple
        """
        raise NotImplementedError()


class OutboundFilter(object):
    """ Prototype of the filter for outgoing messages.

    Its role is to define if a given CSTBox message can be published on the MQTT domain,
    and in this case which is the relevant MQTT topic to be used.

    .. IMPORTANT::

        To be sub-classed, since the default implementation forbid any event
        to go outside of the CSTBox domain.
    """
    def get_mqtt_topic(self, timestamp, var_type, var_name, data):
        """ Returns the MQTT topic conforming the broker conventions and corresponding to the CSTBox
        candidate message.

        Return None if the message is not to be passed to the MQTT world (which is the behaviour if the
        default implementation).

        :param int timestamp: the event time stamp
        :param str var_type: the type of the variable the event is related to
        :param str var_name: the name of the variable
        :param dict data: the event specific payload
        :return: the MQTT topic to be used, or None if the message is not allowed to go outside
        :rtype: str or None
        """
        return None


class REOutboundFilter(OutboundFilter):
    """ Regular expressions based outbound filter.

    It is based on regex based rules, applied to the variable type and variable name,
    which give the MQTT topic to be used for publication. If no matching is found, the message
    is not published.

    The rules are applied in the sequence defined by the rules table, by checking if the compound key
    built as `<var_type>:<var_name>` matches with the rule regex. The corresponding MQTT topîc will be used
    when sending the message.

    Regex can include named capturing groups, which value are used for computing the the topic value. For
    using this feature, the topic must include named replaceable parameters, which name match regex group ones.
    """
    def __init__(self, rules, logger=None):
        """ Initializes the rules list by compiling their specifications.

        The specifications are provided as an iterable of tuples, which items are :

            - a string containing the regex to be applied to fully qualified variable names
            - the MQTT topic string (or topic format string) to be used for publication

        :param rules: an iterable of tuples, as described above
        :param logger: optional logging object
        :raises ValueError: if invalid regex, or if the topic format contains replaceable parameters
            not found in the regex
        """
        self._logger = logger or DummyLogger()
        fmt_parms_re = re.compile(r'%\(([a-zA-Z0-9]+)\)[sfdx]')

        # cache for speeding up computations
        self._cache = {}

        # filtering and  transformation rules
        self._rules = []

        for s_regex, topic in rules:
            # clean all strings
            s_regex, topic = (s.strip() if s else None for s in (s_regex, topic))

            # checks that the rule is fully specified
            if not s_regex or not topic:
                raise ValueError("invalid rule (regex='%s'/topic='%s')" % (s_regex, topic))

            try:
                # compile the regex => ths will checks its syntax
                pattern = re.compile(s_regex)
                # checks that replaceable parameters in topic are included in the captured groups of the rule
                missing = set(fmt_parms_re.findall(topic)) - set(pattern.groupindex.keys())
                if missing:
                    raise ValueError('topic format parameters not found in rule regex (%s)' % ', '.join(missing))
                # the rule is valid => we can register it
                self._rules.append((pattern, topic))

            except re.error as e:
                raise ValueError('invalid regex (%s) : %s' % (s_regex, e))

    def dump_rules(self):
        self._logger.debug('Filtering rules :')
        for regex, topic in self._rules:
            self._logger.debug('- %s -> %s' % (regex.pattern, topic))

    @staticmethod
    def _make_key(var_type, var_name):
        return var_type + ':' + var_name

    def get_mqtt_topic(self, timestamp, var_type, var_name, data):
        """ Returns the MQTT topic matching the event variable type and name, based on the
        known regex rules.

        For efficiency sake, the result is cached for avoiding scanning the rules tables and applying the
        regex for already processed variable types and names.
        """
        key = self._make_key(var_type, var_name)
        try:
            topic = self._cache[key]
        except KeyError:
            for regex, topic_fmt in self._rules:
                r = regex.match(key)
                if r:
                    # computes and cache the result
                    topic = _replace_parms(topic_fmt, r.groupdict())
                    self._cache[key] = topic
                    self._logger.debug("matching rule cached for '%s' (%s) -> %s", key, regex.pattern, topic)
                    return topic

            self._cache[key] = None
            self._logger.debug("'%s' -> !! blocked !!", key)
            return None
        else:
            self._logger.debug("cache hit for '%s' -> %s", key, topic or '!! blocked !!')
            return topic


class MQTTGatewayError(Exception):
    pass

class MQTTConnectionError(MQTTGatewayError):
    pass

class MQTTNotConnectedError(MQTTGatewayError):
    pass
