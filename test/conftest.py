# -*- coding: utf-8 -*-

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'

import pytest


def pytest_addoption(parser):
    parser.addoption("--show-log", action="store_true", default=False, help="display pycstbox logs")


@pytest.fixture
def show_log(request):
    return request.config.getoption("--show-log")
