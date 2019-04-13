# -*- coding: utf-8 -*-
"""
:mod:`mahler.registry.mongodb -- TODO
=====================================

.. module:: mongodb
    :platform: Unix
    :synopsis: TODO

TODO: Write long description
"""
import logging
import os
import socket

import mahler.core
import mahler.core.utils.config

import mahler.registry.mongodb.cli.init

from ._version import get_versions
from .registrar import MongoDBRegistrarDB


logger = logging.getLogger(__name__)


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

DEF_CONFIG_FILES_PATHS = [
    os.path.join(mahler.core.DIRS.site_data_dir, 'registry', 'mongodb', 'config.yaml.example'),
    os.path.join(mahler.core.DIRS.site_config_dir, 'registry', 'mongodb', 'config.yaml'),
    os.path.join(mahler.core.DIRS.user_config_dir, 'registry', 'mongodb', 'config.yaml')
    ]


def build(*args, **kwargs):
    """Build the RegistrarDB object"""
    return MongoDBRegistrarDB(*args, **kwargs)


def build_init_parser(parser):
    """Return the parser that needs to be used for this command"""
    mongodb_init_parser = parser.add_parser('mongodb', help='mongodb init help')

    mongodb_init_parser.set_defaults(subfunc=mahler.registry.mongodb.cli.init.main)


def define_config():
    config = mahler.core.utils.config.Configuration()
    config.add_option(
        'name', type=str, default='mahler', env_var='MAHLER_REGISTRY_MONGODB_NAME')
    try:
        default_host = socket.gethostbyname(socket.gethostname())
    except socket.gaierror as e:
        logger.warning("Cannot infer host IP, using 127.0.01 as default")
        default_host = "127.0.0.1"

    config.add_option(
        'host', type=str, default=default_host,
        env_var='MAHLER_REGISTRY_MONGODB_HOST')

    config.add_option(
        'port', type=int, default=27017,
        env_var='MAHLER_REGISTRY_MONGODB_PORT')

    return config


def parse_config_files(config):
    mahler.core.utils.config.parse_config_files(
        config, mahler.core.DEF_CONFIG_FILES_PATHS,
        base='registry.mongodb')

    mahler.core.utils.config.parse_config_files(
        config, DEF_CONFIG_FILES_PATHS,
        )
