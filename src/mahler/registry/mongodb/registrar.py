# -*- coding: utf-8 -*-
"""
:mod:`mahler.registry.mongodb.registrar -- TODO 
===============================================

.. module:: registrar
    :platform: Unix
    :synopsis: TODO

TODO: Write long description

"""
import logging
import pprint

import bson.objectid
from pymongo import MongoClient
import pymongo.errors

from mahler.core.registrar import RegistrarDB
from mahler.core.utils.errors import RaceCondition
import mahler.core.status


logger = logging.getLogger(__name__)


NON_INITIALIZED_WARNING = """\
Database is not initialized and becaus of that queries will be very inefficient.
To initialize the database, use the command:

$ mahler registry mongodb init\
"""


class MongoDBRegistrarDB(RegistrarDB):
    """
    """

    def __init__(self, name, host):
        """
        """
        self._client = MongoClient(host, socketTimeoutMS=20000)  # 20 seconds
        self._db = self._client[name]

        if logger.isEnabledFor(logging.DEBUG):
            self._verify_initialization()

    def _verify_initialization(self):
        for i, index_information in enumerate(self._db.tasks.report.list_indexes()):
            if i > 1:
                return True

        logger.warning(NON_INITIALIZED_WARNING)
        return False

    def init(self):
        for subcollection in ['status', 'tags', 'stdout', 'stderr', 'host', 'metrics']:
            dbcollection = self._db['tasks.{}'.format(subcollection)]
            dbcollection.create_index([('task_id', pymongo.ASCENDING)], background=True)
            dbcollection.create_index([('key', pymongo.ASCENDING)], unique=True, background=True)

        self._db.tasks.report.timestamp.create_index(
            [('task_id', pymongo.ASCENDING),
             ('_id', pymongo.ASCENDING)], background=True)

        self._db.tasks.report.timestamp.create_index(
            [('task_id', pymongo.ASCENDING),
             ('_id', pymongo.DESCENDING)], background=True)

        self._db.tasks.status.create_index(
            [('runtime_timestamp', pymongo.DESCENDING)], background=True)
        self._db.tasks.tags.create_index([('item.tag', pymongo.ASCENDING)], background=True)

        # self._db.tasks.report.create_index(
        #     [('registry.reported_on', pymongo.ASCENDING)], background=True)

        self._db.tasks.report.create_index(
            [('registry.reported_on', pymongo.ASCENDING),
             ('facility.host.env.clustername', pymongo.ASCENDING),
             ('registry.status', pymongo.ASCENDING),
             ('registry.tags', pymongo.ASCENDING),
             ('registry.container', pymongo.ASCENDING)], background=True)

        self._db.tasks.report.create_index(
            [('facility.host.env.clustername', pymongo.ASCENDING),
             ('registry.status', pymongo.ASCENDING),
             ('registry.tags', pymongo.ASCENDING),
             ('registry.container', pymongo.ASCENDING)], background=True)

        self._db.tasks.report.create_index(
            [('registry.status', pymongo.ASCENDING),
             ('registry.tags', pymongo.ASCENDING),
             ('registry.container', pymongo.ASCENDING)], background=True)

        self._db.tasks.report.create_index(
            [('registry.tags', pymongo.ASCENDING),
             ('registry.container', pymongo.ASCENDING)], background=True)

        self._db.tasks.report.create_index(
            [('registry.container', pymongo.ASCENDING)], background=True)


        # Create unique index on ref_id

    # There is no need to register a task again, only update moving parts
    def register_task(self, task):
        """
        """
        task_doc = task.to_dict(report=False)
        self._db.tasks.insert_one(task_doc)
        # TODO: Test whether task id is valid
        task.id = task_doc['_id']

    def update_report(self, task_report, update_output=False, upsert=False):
        if upsert:
            task_report['_id'] = task_report.pop('id')
            try:
                self._db.tasks.report.insert_one(task_report)
            except pymongo.errors.DuplicateKeyError as e:
                message = 'Report {} already registered'.format(task_report['_id'])
                logger.info(message)
                raise RaceCondition(message)

            return

        registry_fields_to_update = ['started_on', 'stopped_on', 'updated_on', 'reported_on',
                                     'duration', 'status', 'tags']

        query = {'_id': task_report['id']}

        update = {
            '$set': {
                'registry.{}'.format(name): task_report['registry'][name]
                for name in registry_fields_to_update
                }
            }

        update['$set']['facility.host'] = task_report['facility']['host']

        if update_output:
            update['$set']['output'] = task_report['output']
            update['$set']['volume'] = task_report['volume']

        result = self._db.tasks.report.update_one(query, update)
        if result.matched_count < 1:
            raise RuntimeError(
                "Report does not exist, it cannot be updated. Use upsert=True if you intended "
                "to create a new report.")

    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None, limit=None,
                       sort=None, host=None, use_report=True, projection=None):
        """
        """
        if limit is not None and limit < 1:
            raise StopIteration

        query = {}

        # projection should have status, tags, host and priority set to 0 by default
        # We don't use them when building the task so we don't need to fetch.
        if projection and all(v == 0 for v in projection.values()):
            projection.setdefault('registry.status', 0)
            projection.setdefault('registry.tags', 0)

        if projection and 'id' in projection:
            projection['_id'] = projection.pop('id')

        if id is not None:
            if not isinstance(id, bson.objectid.ObjectId):
                id = bson.objectid.ObjectId(id)
            query['_id'] = id

        if id is None and tags:
            query['registry.tags'] = {'$all': tags}

        if id is None and container and isinstance(container, (list, tuple)):
            query['registry.container'] = {'$in': container}
        elif id is None and container:
            query['registry.container'] = {'$eq': container}

        if id is None and status and isinstance(status, (list, tuple)):
            query['registry.status'] = {'$in': [status_i.name for status_i in status]}
        elif id is None and status:
            query['registry.status'] = {'$eq': status.name}

        if id is None and host and isinstance(host, (list, tuple)):
            query['facility.host.env.clustername'] = {'$in': host}
        elif id is None and host:
            query['facility.host.env.clustername'] = {'$eq': host}

        if '_id' not in query or use_report:
            cursor = self._db.tasks.report.find(query, projection=projection)
        else:
            cursor = self._db.tasks.find(query, projection=projection)

        if sort:
            cursor = cursor.sort(sort)

        if limit:
            cursor = cursor.limit(int(limit))

        for task_document in cursor:
            task_document['id'] = task_document.pop('_id')
            yield task_document

    def retrieve_tasks_backup(self, id=None, tags=tuple(), container=None, status=None,
                              use_report=True):
        if id is not None:
            if not isinstance(id, bson.objectid.ObjectId):
                id = bson.objectid.ObjectId(id)
            task = list(self._db.tasks.find({'_id': id}))

            if not task:
                raise RuntimeError

            task = task[0]
            task['id'] = task.pop('_id')
            yield task
            raise StopIteration

        task_ids = set()

        # query all tag events
        # query all status events

        query = {'item.tag': {'$in': tags}}
        tag_events = self._db.tasks.tags.find(query)

        if tags:
            # TODO: Try to serialize to output tasks without processing all events
            tasks = dict()
            for tag_event in tag_events:
                if tag_event['task_id'] not in tasks:
                    tasks[tag_event['task_id']] = dict(tags=[])
                
                tasks[tag_event['task_id']]['tags'].append(tag_event['item']['tag'])

            task_ids = set()
            for task_id, task in tasks.items():
                if all(tag in task['tags'] for tag in tags):
                    task_ids.add(task_id)
        else:
            task_ids = (doc['_id'] for doc in self._db.tasks.find(projection={'_id': 1}))

        for task_id in task_ids:
            task = None
            if container is not None:
                task = list(self._db.tasks.find({'_id': task_id, 'registry.container': container}))
                assert len(task) < 2
                if not task:
                    continue

            if status is not None:
                status_events = self._db.tasks.status.find(
                    {'task_id': task_id}, sort=[('runtime_timestamp', -1)]).limit(1)
                status_events = list(status_events)

                if not status_events or status_events[0]['item']['name'] != status.name:
                    continue

            if task is None:
                task = list(self._db.tasks.find({'_id': task_id}))
                assert len(task) == 1

            task = task[0]
            task['id'] = task.pop('_id')
            yield task

    def add_event(self, event_type, event_object):
        # event_object['creation_timestamp'] = str(event_object['creation_timestamp'])
        # event_object['runtime_timestamp'] = str(event_object['runtime_timestamp'])
        try:
            self._db['tasks.{}'.format(event_type)].insert_one(event_object)
            event_object['id'] = event_object.pop('_id')
        except pymongo.errors.DuplicateKeyError as e:
            message = ('Another {} was registered concurrently for '
                       'the task {}'.format(event_type, event_object['task_id']))
            raise RaceCondition(message) from e

    def retrieve_events(self, event_type, task, sort=None, limit=None, updated_after=None):
        # TODO: Convert str -> datetimes 
        task_id = task.id
        if not isinstance(task_id, bson.objectid.ObjectId):
            task_id = bson.objectid.ObjectId(task_id)
        query = {'task_id': task_id}

        if updated_after:
            query['_id'] = {'$gt': bson.objectid.ObjectId(updated_after)}

        cursor = self._db['tasks.{}'.format(event_type)].find(query)

        if sort:
            cursor = cursor.sort(sort)

        if limit:
            cursor = cursor.limit(int(limit))

        for event in cursor:
            event['id'] = event.pop('_id')
            yield event

    def set_output(self, task, output):
        self._db.tasks.update({'_id': task.id}, {'$set': {'output': output}})
        task._output = output
        # query = (where('id') == task.id) & where('output') == None
        # ids = self._db.tasks.update(set('output', output), query)
        # assert ids == [task.id]

    def set_volume(self, task, volume):
        raise NotImplementedError()
        # query = (where('id') == task.id) & where('volume') == None
        # ids = self._db.table('tasks').update(set('volume', volume), query)
        # assert ids == [task.id]

    def retrieve_output(self, task):
        task_id = task.id
        if not isinstance(task_id, bson.objectid.ObjectId):
            task_id = bson.objectid.ObjectId(task_id)
        query = {'_id': task_id}
        docs = list(self._db.tasks.find(query, {'output': 1}).limit(1))
        if docs:
            return docs[0]['output']

        return None

    def retrieve_volume(self, task):
        # TODO
        return None

    def close(self):
        self._client.close()
