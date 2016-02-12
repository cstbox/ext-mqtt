# -*- coding: utf-8 -*-

""" Configuration data definitions.

The configuration is represented by a dictionary which minimal structure is defined as
follows :

    {
        "svcobj_class_name": "fully.qualified.path.to.service_object.class",
        "broker": {
            "host": "MQTT_broker_hostname_or_IP",
            "port": <MQTT_broker_connection_port_defaulted_to_61613>,
            "client_id": "<optional>"
        },
        "auth": {
            "user": "",
            "password": ""
        },
        "tls": {
            "ca_certs": "",
            "certfile": "",
            "keyfile": ""
        },
        "adapters": {
            "inbound": <inbound_adapter_config_dictionary>,
            "outbound": <outbound_adapter_config_dictionary>
        }
    }
"""

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

from json import load as json_load

from pycstbox.sysutils import symbol_for_name

from .errors import ConfigurationError


# Configuration dictionary keys

CFG_SERVICE_OBJECT_CLASS = 'svcobj_class'
CFG_SIMULATE = 'simulate'
CFG_BROKER = 'broker'
CFG_HOST = 'host'
CFG_PORT = 'port'
CFG_CLIENT_ID = 'client_id'
CFG_AUTH = 'auth'
CFG_USER = 'user'
CFG_PASSWORD = 'password'
CFG_TLS = 'tls'
CFG_TLS_CA_CERTS = 'ca_certs'
CFG_TLS_CERTFILE = 'certfile'
CFG_TLS_KEYFILE = 'keyfile'
CFG_QOS = 'qos'
CFG_ADAPTERS = 'adapters'
CFG_INBOUND = 'inbound'
CFG_OUTBOUND = 'outbound'
CFG_ADAPTER_CLASS = "adapter_class"
CFG_DEFAULT_BUILDER = 'default_builder'
CFG_RULES = 'rules'
CFG_CHANNELS = 'channels'

DEFAULT_BROKER_PORT = 61613
DEFAULT_CLIENT_ID = ""


class Configurable(object):
    """ This mixin defines the operations available for entities which can be configured.
    """
    def configure(self, cfg):
        """ Configures the object, based on the information provided.

        Configuration data are provided as a dictionary, which basic key set is defined in this module.

        :param dict cfg: the configuration dictionary
        """


def load_configuration(cfgfile_path):
    """ Loads the configuration from a JSON file and returns it as a dictionary.

    This function takes care of resolving the service object class name and puts the resolved instance in
    the returned configuration dictionary in place of the qualified name.

    :param str cfgfile_path: configuration file path
    :returns: the configuration data
    :rtype: dict
    :raises ValueError: if path is not provided of configuration data are invalid
    :raises IOError: if not found or not a file
    :raises ConfigurationError: if the configuration is not valid
    """
    if not cfgfile_path:
        raise ValueError('missing parameter : cfgfile_path')

    with file(cfgfile_path, 'rt') as fp:
        cfg = json_load(fp)

    # resolves the service object class name
    try:
        cfg[CFG_SERVICE_OBJECT_CLASS] = symbol_for_name(cfg[CFG_SERVICE_OBJECT_CLASS])
    except (ImportError, NameError) as e:
        raise ConfigurationError(e)
    else:
        return cfg
