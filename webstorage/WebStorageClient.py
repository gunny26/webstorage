#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""


class HTTPError(Exception):
    """indicates general exception"""
    pass


class HTTP404(Exception):
    """indicates not found"""
    pass
