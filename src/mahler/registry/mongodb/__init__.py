# -*- coding: utf-8 -*-
"""
:mod:`mahler.registry.mongodb -- TODO
=====================================

.. module:: mongodb
    :platform: Unix
    :synopsis: TODO

TODO: Write long description
"""
from ._version import get_versions
from .registrar import MongoDBRegistrarDB

VERSIONS = get_versions()
del get_versions

__descr__ = 'TODO'
__version__ = VERSIONS['version']
__license__ = 'GNU GPLv3'
__author__ = u'Xavier Bouthillier'
__author_short__ = u'Xavier Bouthillier'
__author_email__ = 'xavier.bouthillier@umontreal.ca'
__copyright__ = u'2018, Xavier Bouthillier'
__url__ = 'https://github.com/bouthilx/mahler.registry.mongodb'


def build(*args, **kwargs):
    """Build the RegistrarDB object"""
    return MongoDBRegistrarDB(*args, **kwargs)
