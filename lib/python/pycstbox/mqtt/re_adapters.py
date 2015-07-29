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

""" Specialized adapters using regular expression based rules for filtering and processing
the CSTBox events and MQTT messages crossing the boundary.

They implement the complete mechanism for identifying and applying the appropriate rule
depending on the processed piece of data, but it is up to the application using them to
provide the list of rules, and among others, the plugable code for translating the data between
both worlds.
"""

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

__all__ = ['REInboundAdapter', 'REOutboundAdapter']

from collections import namedtuple
import re

from pycstbox.events import BasicEvent
from pycstbox.evtmgr import CONTROL_EVENT_CHANNEL
from pycstbox.log import Loggable
from .core import InboundAdapter, OutboundAdapter
from .errors import EventHandlerError
from .config import *


class RegexMixin(object):
    """ A mixin handling services related to regex manipulations.
    """
    @staticmethod
    def compile_regex(pattern):
        pattern = pattern.strip() if pattern else None
        if not pattern:
            raise ValueError('missing regex in rule')

        try:
            # compile the regex => ths will checks its syntax
            return re.compile(pattern)

        except re.error as e:
            raise ValueError('invalid regex (%s) : %s' % (pattern, e))


class REInboundAdapter(InboundAdapter, RegexMixin, Loggable):
    """ Regular expressions based inbound adapter.

    It is based on regex based rules applied to the message topic, which give in return the variable
    type and variable name of the message to be emitted, with the channel on which this must be done.

    The CSTBox event payload building is delegated to a callable attached to each event production rule
    (see :py:class:`EventProductionRule`), based on the content of the processed CSTBox event.
    This callable must accept the following parameters :

        client:
            the MQTT client instance

        message:
            the MQTT message

        user_data:
            application specific data which can be attached to the client

        var_type:
            the variable type of the CSTBox event

        var_name:
            the variable name of the CSTBox event

        adapter:
            the adapter calling it

    The callable must return a dictionary containing the resulting payload.

    ..WARNING:

        If the builder is implemented as a method of the adapter, don't forget to tag it as a static method to avoid
        `self` being passed as the first parameter.

    .. IMPORTANT:

        Since there is no way to define a generic method for extracting the event payload from the MQTT message,
        this is delegated to the :py:meth:`make_event_payload` abstract method which must be implemented
        by the real inbound filter.
    """
    def __init__(self, rules=None):
        """ Initializes the rules list by compiling their specifications.

        :param rules: optional rules list
        :raises ValueError: if invalid regex, or if the topic format contains replaceable parameters
            not found in the regex
        """
        Loggable.__init__(self)

        # cache for speeding up computations
        self._cache = {}

        # filtering and transformation transformation rules
        self.rules = rules[:] if rules else []

    def _configure(self, cfg):
        """ Processes and applies the adapter specific configuration.

        It contains the following key(s):

            class_name:
                the fully qualified name of the class implementing the adapter

            rules:
                a list of rules groups

        Each rules group allows to define a set of rules related to the same MQTT subscription
        pattern, which can include wildcards as detailed in the MQTT documentation. A rules group
        is composed of two items :

            - the MQTT subscription pattern
            - the associated rules list

        .. IMPORTANT:

            The provided subscription pattern will always be interpreted as a partial pattern. In other words,
            at MQTT subscription time, a trailing '#' will be appended if not yet present.

            In addition, il will be prepended to the regex contained in the rules list to form the effective
            regex used to analyse the incoming message.

        The rules list is a list of tuples, composed of the regex applied to the topic of the processed message,
        and the list of production rules for the events to be created when the corresponding MQTT message is received.
        These events production rules are provided as a tuple containing the string representation of
        :py:class:`EventProductionRule` attributes, namely :

            - the var_type of the produced event
            - the var_name
            - the qualified name of the function or method producing the event payload
            - the event channel, defaulted to `events.CONTROL_EVENT_CHANNEL` if not provided

        .. IMPORTANT:

            As explained above, the regex will be combined with the one derived from the rules group subscription
            pattern to form the effective regex applied to incoming messages.

        :param dict cfg: the adapter configuration sub-tree

        :raises TypeError: if the instance class differs from the one indicated in the configuration
        """
        # starts by merging all rule groups in a global rules list compatible with set_rules()
        rules = []
        mqtt_subscr = []
        for mqtt_subscr_topic, rules_group in cfg[CFG_RULES]:
            if mqtt_subscr_topic.endswith('/#'):
                pattern_prefix = mqtt_subscr_topic[:-1]
                mqtt_subscr.append(mqtt_subscr_topic)
            elif mqtt_subscr_topic.endswith('/'):
                pattern_prefix = mqtt_subscr_topic
                mqtt_subscr.append(mqtt_subscr_topic + '#')
            else:
                pattern_prefix = mqtt_subscr_topic + '/'
                mqtt_subscr.append(mqtt_subscr_topic + '/#')

            pattern_prefix = r'^' + pattern_prefix

            for group_topic_pattern, epr_list in rules_group:
                resolved_eprs = []
                for epr_desc in epr_list:
                    var_type, var_name, builder_fqn, channel = (epr_desc + [CONTROL_EVENT_CHANNEL])[:4]
                    builder = symbol_for_name(builder_fqn)
                    resolved_eprs.append(EventProductionRule(var_type, var_name, builder, channel))

                rules.append((pattern_prefix + group_topic_pattern.lstrip('/'), resolved_eprs))

        # now configure us with the resulting rules list
        self.rules = rules

        # and store the list of MQTT topics we need to subscribe
        self._mqtt_subscr_list = mqtt_subscr

    def dump_rules(self):
        self.logger.debug('Filtering rules :')
        for regex, dispatch in self.rules:
            self.logger.debug('- %s -> %s' % (regex.pattern, dispatch))

    def handle_message(self, client, user_data, message):
        mqtt_topic = message.topic
        for regex, dispatch in self.rules:
            r = regex.match(mqtt_topic)
            if r:
                gd = r.groupdict
                for epr in dispatch:
                    # expand templates with captured groups
                    var_type = _replace_parms(epr.var_type, gd)
                    var_name = _replace_parms(epr.var_name, gd)

                    # build the event payload
                    payload = epr.payload_builder(client, message, user_data, var_type, var_name, self) or {}

                    yield BasicEvent(var_type, var_name, payload), epr.channel

                # ignore the other rules and stop
                break


class REOutboundAdapter(OutboundAdapter, RegexMixin, Loggable):
    """ Regular expressions based outbound adapter.

    It uses regex based rules applied to a signature built with the channel, var_type and var_name of the event. When a
    match is found, the associated message production rule (MPR) is applied. This MPR specifies the MQTT topic
    to be used for the message to be sent, and the process to be executed to build the message payload.

    The MQTT message can be dynamically computed by including capture groups in the regex and the corresponding
    placeholders in the MQTT topic specified in the MPR.

    In the absence of match, the involved event is discarded and not sent to the MQTT world.

    The rules are applied in the sequence defined by the rules table.

    The MQTT payload building is delegated to the callable attached to the matched rule. It must accept the following
    parameters :

        topic:
            the MQTT topic, fully expanded if the matching rule uses capture groups

        event:
            a tuple containing the CSTBox event parts, in this order : timestamp, channel, var_type, var_name, payload

        group_dict:
            the rule regular expression capture group dictionary (empty if capture groups are not used)

        adapter:
            the adapter calling it

    It must return the MQTT payload as a dictionary.

    ..WARNING:

        If the builder is implemented as a method of the adapter, don't forget to tag it as a static method to avoid
        `self` being passed as the first parameter.
    """
    def __init__(self, rules=None):
        """ Initializes the rules list by compiling their specifications.

        The specifications are provided as an iterable of 3 parts tuples, which items are :

            - a string containing the regex to be applied to fully qualified variable names,
            which can contain capture groups intended to be used for the topic string expansion
            - the MQTT topic string (or topic format string) to be used for publication
            - the callable used to build the MQTT message payload

        :param rules: an iterable of tuples, as described above
        :param logger: optional logging object
        :raises ValueError: if invalid regex, or if the topic format contains replaceable parameters
            not found in the regex
        """
        Loggable.__init__(self)

        # cache for speeding up computations
        self._cache = {}

        # filtering and  transformation rules
        self.rules = rules or []

    def _configure(self, cfg):
        """ Processes and applies the adapter specific configuration.

        The passed configuration data is a dictionary which contains the following key(s):

            class_name:
                the fully qualified name of the class implementing the adapter

            rules:
                the dictionary of rules groups

        The rules groups dictionary specifies the list of rules to be applied to events circulating on a given channel.
        Each rule inside the group is described by a tuple composed of :

            - the pattern of the regex applied to the event combined signature, built by concatenating its var_type
            and var_name
            - the topic of the corresponding MQTT message to be sent
            - the qualified name of the function or method producing the message payload based on the event's one

        The order of the rule is important, since the first match stops the search.

        If the processed configuration is valid, the existing rules will be replaced by the resulting ones. In case
        of error, they are kept unmodified.

        :param dict cfg: the adapter configuration sub-tree

        :raises TypeError: if the instance class differs from the one indicated in the configuration
        """
        rules = []
        key_pattern = ''
        default_builder = cfg.get(CFG_DEFAULT_BUILDER, None)
        try:
            for channel, rules_list in cfg[CFG_RULES].iteritems():
                for rule in rules_list:
                    try:
                        evt_signature_pattern, topic = (s.strip() for s in rule[:2])
                    except ValueError:
                        raise ConfigurationError('incomplete rule (%s)' % rule)

                    if not all((evt_signature_pattern, topic)):
                        raise ConfigurationError(
                            'empty member(s) found (p=%s t=%s)' % (evt_signature_pattern, topic)
                        )

                    try:
                        builder_fqn = rule[2].strip()
                    except IndexError:
                        builder_fqn = ""

                    if builder_fqn:
                        builder = self.resolve_builder(builder_fqn)
                    else:
                        if default_builder:
                            builder = default_builder
                        else:
                            raise ConfigurationError(
                                'no builder specified and no default builder configured for rule (%s)' % rule
                            )

                    key_pattern = channel + EVENT_KEY_SEP + evt_signature_pattern
                    key_re = re.compile(key_pattern)
                    # checks that replaceable parameters in topic are all included in the captured groups of the rule
                    missing = set(_FMT_PARMS_RE.findall(topic)) - set(key_re.groupindex.keys())
                    if missing:
                        raise ConfigurationError('topic format parameter(s) not found in rule regex: %s' % ', '.join(missing))

                    rules.append(OutboundRule(key_re, MessageProductionRule(topic, builder)))

        except ConfigurationError as e:
            self.logger.error(e)
            raise

        except re.error as e:
            error = 'invalid regex (%s): %s' % (e, key_pattern)
        except (ImportError, NameError) as e:
            error = 'builder not found: %s' % e
        except Exception as e:
            error = 'unexpected configuration error: %s' % e

        else:
            error = None

        if not error:
            # now configure us with the resulting rules list
            self.rules = rules
        else:
            self.logger.error(error)
            raise ConfigurationError(error)

    def dump_rules(self):
        self.logger.debug('Filtering rules :')
        for regex, mpr in self.rules:
            self.logger.debug('- %s -> %s' % (regex.pattern, mpr))

    def handle_event(self, timestamp, channel, var_type, var_name, data):
        """ For efficiency sake, this implementation caches the result for avoiding scanning the rules tables
        and applying the regex for already known events signatures.
        """
        topic = gd = payload_builder = None

        key = _make_event_key(channel, var_type, var_name)
        found = False
        try:
            topic, gd, payload_builder = self._cache[key]
            found = True
            self.logger.debug("cache hit for '%s' -> %s", key, topic)

        except KeyError:
            for regex, mpr in self.rules:
                topic, payload_builder = mpr
                r = regex.match(key)
                if r:
                    # computes and caches the result
                    gd = r.groupdict()
                    topic = _replace_parms(topic, gd)
                    self._cache[key] = (topic, gd, payload_builder)

                    found = True
                    self.logger.debug("added rule to cache for '%s' (%s) -> %s", key, regex.pattern, topic)
                    break

        if found:
            try:
                payload = payload_builder(
                    topic, EventTuple(timestamp, channel, var_type, var_name, data), gd, self
                ) or {}
            except Exception as e:
                msg = 'payload builder failure: %s (event key=%s)' % (e, key)
                self.logger.error(msg)
                raise EventHandlerError(msg)
            else:
                return topic, payload
        else:
            return None


EventTuple = namedtuple('EventTuple', 'timestamp, channel, var_type, var_name, payload')


class EventProductionRule(namedtuple('EventProductionRule', 'var_type var_name payload_builder channel')):
    """ CSTBox event production rule.
    """
    __slots__ = ()

    def __new__(cls, var_type, var_name, payload_builder, channel=CONTROL_EVENT_CHANNEL):
        """
        :param var_type: the variable type
        :param var_name: the variable name
        :param payload_builder: a callable returning the event payload, based on the MQTT message content
        :param channel: the channel on which the event will be emitted. If omitted, it is defaulted to
        :py:attr:`pycstbox.evtmgr.CONTROL_EVENT_CHANNEL`
        """
        var_type, var_name, channel = t = var_type.strip(), var_name.strip(), channel.strip()
        if not all(t):
            raise ValueError("invalid production rule (var_type='%s'/var_name='%s'/channel='%s')" % t)
        if not payload_builder:
            raise ValueError("rule misses the payload_builder (var_type='%s'/var_name='%s'" % (var_type, var_name))

        return super(EventProductionRule, cls).__new__(
            cls, var_type, var_name, payload_builder, channel
        )


OutboundRule = namedtuple('OutboundRule', 'regex mpr')


class MessageProductionRule(namedtuple('MessageProductionRule', 'topic payload_builder')):
    """ MQTT message production rule.
    """
    __slots__ = ()

    def __new__(cls, topic, payload_builder):
        topic = topic.strip()
        if not all((topic, payload_builder)):
            raise ValueError("invalid production rule (topic='%s'/payload_builder='%s')" % (topic, payload_builder))
        return super(MessageProductionRule, cls).__new__(cls, topic, payload_builder)


def _replace_parms(s, d):
    try:
        return s % d
    except TypeError:
        # these was no replaceable parameter
        return s

EVENT_KEY_SEP = ':'


def _make_event_key(channel, var_type, var_name):
    return EVENT_KEY_SEP.join((channel, var_type, var_name))

# expansion allowed formats (ex: %(foo)s, %(bar)x,...)
_FMT_PARMS_RE = re.compile(r'%\(([a-zA-Z0-9]+)\)[sfdx]')

