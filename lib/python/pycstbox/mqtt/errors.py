# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


class MQTTGatewayError(Exception):
    """ Root exception for package ones.
    """


class MQTTConnectionError(MQTTGatewayError):
    """ Risen when we cannot connect to the broker.
    """


class MQTTNotConnectedError(MQTTGatewayError):
    """ Risen when the requested operation cannot be done because no connection to the broker.
    """


class InboundAdapterError(MQTTGatewayError):
    """ Root exception for InboundAdapter.
    """


class OutboundAdapterError(MQTTGatewayError):
    """ Root exception for OutboundAdapter.
    """


class EventHandlerError(OutboundAdapterError):
    """ Something goes wrong while handing an event.
    """


class ConfigurationError(MQTTGatewayError):
    """ Specialized exception for all problems related to configuration
    """