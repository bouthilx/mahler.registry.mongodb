# -*- coding: utf-8 -*-
"""
:mod:`mahler.registry.mongodb.registrar -- TODO 
===============================================

.. module:: registrar
    :platform: Unix
    :synopsis: TODO

TODO: Write long description

"""
from pymongo import MongoClient
import pymongo.errors

from mahler.core.registrar import RegistrarDB, RaceCondition
import mahler.core.status


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

        self._db.tasks.status.create_index([('runtime_timestamp', pymongo.DESCENDING)], background=True)
        self._db.tasks.tags.create_index([('item.tag', pymongo.ASCENDING)], background=True)

    # There is no need to register a task again, only update moving parts
    def register_task(self, task):
        """
        """
        task_doc = task.to_dict()
        self._db.tasks.insert_one(task_doc)
        # TODO: Test whether task id is valid
        task.id = task_doc['_id']

    # TODO: Register reports

    def retrieve_tasks(self, tags, container=None, status=None):
        """
        """
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
