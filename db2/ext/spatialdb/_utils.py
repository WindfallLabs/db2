# !/usr/bin/env python2
"""
"""

import os
import six
from abc import ABCMeta


@six.add_metaclass(ABCMeta)
class _SecurityState(object):
    name = ""
    value = ""
    allowed = []

    def switch(self, state):
        """ Switch to new state """
        if state.value in self.allowed:
            print("======== Switched! ========")
            os.environ["SPATIALITE_SECURITY"] = state.value
            self.__class__ = state
        else:
            pass  # Changing state to current state does nothing

    def __str__(self):
        return "SPATIALITE_SECURITY = '{}'".format(self.value)

    def __repr__(self):
        return "<{}: environment var {}>".format(
            self.name, self.__str__())


class _StrictSecurity(_SecurityState):
    name = "off"
    value = "strict"
    allowed = ['relaxed']


class _RelaxedSecurity(_SecurityState):
    """ State of being powered on and working """
    name = "on"
    value = "relaxed"
    allowed = ['relaxed']


class SpatiaLiteSecurity(object):
    """Controller for setting the SPATIALITE_SECURITY environment variable."""
    __instance = None

    @staticmethod
    def getInstance():
        if SpatiaLiteSecurity.__instance is None:
            SpatiaLiteSecurity()
        return SpatiaLiteSecurity.__instance

    def __init__(self):
        """Default to 'strict' security."""
        # Singleton logic
        if SpatiaLiteSecurity.__instance is not None:
            raise TypeError("instance of singleton class 'SpatiaLiteSecurity'"
                            "already exists.")
        else:
            SpatiaLiteSecurity.__instance = self

        # Default env var to 'strict' even if set at the system level
        os.environ["SPATIALITE_SECURITY"] = "strict"
        self.state = _StrictSecurity()

    def set_security(self, state):
        """Change state of security"""
        self.state.switch(state)

    def __str__(self):
        return "SpatiaLite Security Switch object: set to '{}'".format(
            self.state.value)

    def __repr__(self):
        return "<{}>".format(self.__str__())
