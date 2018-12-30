# -*- coding: utf-8 -*-
"""
:mod:`mahler.registry.mongodb.registrar -- TODO 
===============================================

.. module:: mongodb
    :platform: Unix
    :synopsis: TODO

TODO: Write long description
"""
from mahler.core.registrar import RegistrarDB


class MongoDBRegistrarDB(RegistrarDB):
    """
    """

    def __init__(self):
        """
        """
        pass

    def register_task(self, task):
        """
        """
        raise NotImplementedError()

    def retrieve_tasks(self, tags, status=None):
        """
        """
        raise NotImplementedError()

    def add_event(self, event_type, event_object):
        """
        """
        raise NotImplementedError()

    def retrieve_events(self, event_type, task):
        """
        """
        raise NotImplementedError()

    def set_output(self, task, output):
        """
        """
        raise NotImplementedError()

    def set_volume(self, task, volume):
        """
        """
        raise NotImplementedError()

    def retrieve_output(self, task):
        """
        """
        raise NotImplementedError()
