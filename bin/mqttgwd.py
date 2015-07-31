#!/usr/bin/env python
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

""" MQTT message bus gateway daemon. """

import sys
import logging
import os.path

from pycstbox import log, cli, dbuslib
from pycstbox.service import ServiceContainer

from pycstbox.mqtt import dbus_binding
from pycstbox.mqtt.errors import MQTTGatewayError
from pycstbox.mqtt.config import load_configuration, CFG_SERVICE_OBJECT_CLASS

if __name__ == '__main__':
    log.setup_logging(os.path.basename(__file__))

    parser = cli.get_argument_parser('CSTBox MQTT gateway')
    cli.add_config_file_option_to_parser(parser, dflt_name="/etc/cstbox/mqtt_gateway.cfg")

    args = parser.parse_args()

    try:
        dbuslib.dbus_init()

        # load the gateway configuration
        cfg = load_configuration(args.cfg)

        # instantiate the service object
        svc_obj = cfg[CFG_SERVICE_OBJECT_CLASS](cfg)

        # build the service container
        svc = ServiceContainer(
            dbus_binding.SERVICE_NAME,
            dbuslib.get_bus(),
            [(svc_obj, dbus_binding.OBJECT_PATH)]
        )

        svc.log_setLevel_from_args(args)

        # start the node
        svc.start()

    except KeyError as e:
        logging.exception("invalid configuration: %s", e)
        sys.exit(e)

    except MQTTGatewayError as e:
        # already logged => exit now
        sys.exit(e)

    except Exception as e:  # pylint: disable=W0703
        logging.exception(e)
        sys.exit(e)

