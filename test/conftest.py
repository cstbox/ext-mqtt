# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import os
import pytest


def pytest_addoption(parser):
    parser.addoption("--show-log", action="store_true", default=False, help="display pycstbox logs")


@pytest.fixture
def show_log(request):
    return request.config.getoption("--show-log")


def pytest_runtest_setup(item):
    print("----------- setting up %s (kwd=%s)" % (item, [kwd for kwd in item.keywords]))


@pytest.fixture
def cfg():
    import json
    with file(os.path.join(os.path.dirname(__file__), 'fixtures/gateway.cfg')) as fp:
        cfg_dict = json.load(fp)
    assert cfg_dict
    return cfg_dict


