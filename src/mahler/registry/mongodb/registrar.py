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

from mahler.core.registrar import RegistrarDB, RaceCondition
import mahler.core.status


logger = logging.getLogger(__name__)


class MongoDBRegistrarDB(RegistrarDB):
    """
    """

    def __init__(self, name, host):
        """
        """
        self._client = MongoClient(host)
        self._db = self._client.mahler_test

        for subcollection in ['status', 'tags', 'stdout', 'stderr']:
            dbcollection = self._db['tasks.{}'.format(subcollection)]
            dbcollection.create_index([('task_id', pymongo.ASCENDING)], background=True)

        self._db.tasks.status.create_index(
            [('runtime_timestamp', pymongo.DESCENDING)], background=True)
        self._db.tasks.tags.create_index([('item.tag', pymongo.ASCENDING)], background=True)

        self._db.tasks.report.create_index(
            [('registry.status', pymongo.ASCENDING)], background=True)
        self._db.tasks.report.create_index(
            [('registry.tags', pymongo.ASCENDING)], background=True)
        self._db.tasks.report.create_index(
            [('registry.container', pymongo.ASCENDING)], background=True)

    # There is no need to register a task again, only update moving parts
    def register_task(self, task):
        """
        """
        task_doc = task.to_dict(report=False)
        self._db.tasks.insert_one(task_doc)
        # TODO: Test whether task id is valid
        task.id = task_doc['_id']

    def update_report(self, task, upsert=False):
        updated = False
        if upsert:
            task_report = task.to_dict()
            task_report['_id'] = task_report.pop('id')
            try:
                self._db.tasks.report.insert_one(task_report)
                updated = True
            except pymongo.errors.DuplicateKeyError as e:
                logger.info('Report {} already registered'.format(task.id))

        if not updated:
            query = {'_id': task.id}

            update = {
                '$set': {
                    'registry.status': task.status.name,
                    'registry.tags': sorted(task.tags)
                    }
                }

            self._db.tasks.report.update_one(query, update)

    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None, limit=None,
                       use_report=True, projection=None):
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

        logger.debug('Querying tasks.report with query:\n{}'.format(pprint.pformat(query)))
        if projection:
            logger.debug(
                'Querying tasks.report with projection:\n{}'.format( pprint.pformat(projection)))

        cursor = self._db.tasks.report.find(query, projection=projection)

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
        event_object['_id'] = "{}.{}".format(str(event_object['task_id']), event_object['id'])
        try:
            self._db['tasks.{}'.format(event_type)].insert_one(event_object)
        except pymongo.errors.DuplicateKeyError as e:
            message = ('Another {} was registered concurrently for '
                       'the task {}'.format(event_type, event_object['task_id']))
            raise RaceCondition(message) from e

    def retrieve_events(self, event_type, task):
        # TODO: Convert str -> datetimes 
        for event in self._db['tasks.{}'.format(event_type)].find({'task_id': task.id}):
            yield event

    def set_output(self, task, output):
        self._db.tasks.update({'_id': task.id}, {'$set': {'output': output}})
        # query = (where('id') == task.id) & where('output') == None
        # ids = self._db.tasks.update(set('output', output), query)
        # assert ids == [task.id]

    def set_volume(self, task, volume):
        raise NotImplementedError()
        # query = (where('id') == task.id) & where('volume') == None
        # ids = self._db.table('tasks').update(set('volume', volume), query)
        # assert ids == [task.id]

    def retrieve_output(self, task):
        docs = self._db.tasks.find({'_id': task.id}, {'output': 1})
        if docs:
            return docs[0]['output']

        return None

    def retrieve_volume(self, task):
        # TODO
        return None
