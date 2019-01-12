# -*- coding: utf-8 -*-
"""
:mod:`mahler.registry.mongodb.cli.init -- Command to initialize DB
==================================================================

.. module:: mongodb
    :platform: Unix
    :synopsis: Command to initialize DB

TODO: Write long description
"""


def db_is_being_indexed(db):
    for command_info in db.current_op(include_all=True)['inprog']:
        if 'createIndexes' in command_info.get('command', ''):
            return True

    return False


def main(registrar, **kargs):
    registrardb = registrar._db
    print('Creating indexes...')
    registrardb.init()
    while db_is_being_indexed(registrardb._db):
        time.sleep(1)
    print('done')
