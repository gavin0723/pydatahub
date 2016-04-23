# encoding=utf8

""" The data manager
    Author: lipixun
    Created Time : 二  3/15 20:47:44 2016

    File Name: manager.py
    Description:

"""

import logging

from time import time
from uuid import uuid4
from Queue import Queue
from datetime import datetime
from threading import RLock

from datahub.updates import SetAction
from datahub.repository import Repository
from datahub.conditions import AndCondition, LargerCondition
from datahub.errors import InvalidParameterError, FeatureNotEnabledError, FeatureNotSupportedError

from spec import *
from model import Resource, Metadata, ResourceWatchChangeSet
from pipeline import FeatureInvokePipeline, FeatureInvokeHandler

FEATURE_INVOKE_HANDLER_FIELD = '_datahub_datamanager_feature_invoke_handlers'

class DataManagerMetaClass(type):
    """The data manager metaclass
    This class will set the following meta attributes of data model:
        _datahub_datamanager_feature_invoke_handlers            The feature invoke handlers
    """
    def __new__(cls, name, bases, attrs):
        """Create a new DataModel object
        """
        handlers = {}
        for base in bases:
            if hasattr(base, FEATURE_INVOKE_HANDLER_FIELD):
                handlers.update(getattr(base, FEATURE_INVOKE_HANDLER_FIELD))
        for key, field in attrs.iteritems():
            if isinstance(field, FeatureInvokeHandler):
                handlers[key] = field
        # Add handlers to attrs
        attrs[FEATURE_INVOKE_HANDLER_FIELD] = handlers
        # Super
        return type.__new__(cls, name, bases, attrs)

class DataManager(object):
    """The data manager
    """
    logger = logging.getLogger('datahub.manager.dataManager')

    __metaclass__ = DataManagerMetaClass

    def __init__(self, repository, enables = None, queueClass = None, pipeline = None):
        """Create a new DataManager
        Parameters:
            repository                      The repository
            enables                         The list of enabled feature names
            queueClass                      The waiting queue class
            pipeline                        The pipeline
        """
        # Check the repository model
        if not issubclass(repository.modelClass, Resource):
            raise TypeError('Model must be derived from Resource')
        # Initialize
        self._repository = repository
        self._enables = enables
        self._timestamp = time()
        self._queueClass = queueClass or Queue
        self._pipeline = pipeline.clone() if pipeline else FeatureInvokePipeline()
        # Update the pipelines
        if hasattr(self, FEATURE_INVOKE_HANDLER_FIELD):
            for handler in getattr(self, FEATURE_INVOKE_HANDLER_FIELD).itervalues():
                self._pipeline.addHandler(handler)
        # The watching
        self._watchQueueLock = RLock()
        self._watchQueues = {}      # The key is a uid, value is (condition, queue)

    def __trigger__(self, handler, args):
        """Trigger an event
        """
        try:
            handler(args)
        except:
            self.logger.exception('Failed to trigger event [%s] via handler [%s]', args.name, handler)

    @property
    def modelClass(self):
        """Get the model class
        """
        return self._repository.modelClass

    @property
    def repository(self):
        """Return the repository
        """
        return self._repository

    @property
    def enables(self):
        """Get the enables
        """
        return self._enables

    def trigger(self, name, args):
        """Trigger an event
        """
        pass

    def updateWatch(self, name, ts, modelID, oldModel, newModel):
        """Update the watch status
        Parameters:
            name                        The watch name
            ts                          The timestamp
            oldModel                    The old model
            newModel                    The new model
        Returns:
            Nothing
        """
        # Create the change set
        changeSet = ResourceWatchChangeSet(name = name, timestamp = ts, modelID = modelID, oldModel = oldModel, newModel = newModel)
        changeSet.validate()
        # Add to the queue
        with self._watchQueueLock:
            for condition, queue in self._watchQueues.itervalues():
                if not condition or name == WATCH_RESET:
                    queue.put(changeSet)
                elif condition and name != WATCH_RESET:
                    # We check the condition on different models (old/new) for different change type
                    if name == WATCH_CREATED or name == WATCH_PRESERVED:
                        if newModel and newModel.match(condition):
                            queue.put(changeSet)
                    elif name == WATCH_REPLACED or name == WATCH_UPDATED:
                        # Replaced, Updated
                        if newModel and newModel.match(condition):
                            queue.put(changeSet)
                        elif oldModel and oldModel.match(condition):
                            queue.put(changeSet)
                    else:
                        # Deleted
                        if oldModel and oldModel.match(condition):
                            queue.put(changeSet)

    def invokeFeature(self, name, params = None):
        """Invoke a feature
        """
        params = params or {}
        #if not name in self.repository.FEATURE_METHODS:
        #    raise FeatureNotSupportedError(name)
        # Call this feature via pipeline
        #return self._pipeline(name, params or {}, lambda f,p,m,n: getattr(self, self.repository.FEATURE_METHODS[name])(**params), self)
        return getattr(self.repository, self.repository.FEATURE_METHODS[name])(**params)

    def existByID(self, id):
        """Exist by id
        Parameters:
            id                              The id
        Returns:
            True / False
        """
        if not type(self.repository).existByID.isImplemented or (self._enables and not type(self.repository).existByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).existByID.name)
        if not id:
            raise InvalidParameterError(reason = 'Require parameter [id]')
        # Call
        return self.repository.existByID(id)

    def existsByID(self, ids):
        """Exists by id
        Parameters:
            ids                             A list of id
        Returns:
            A list of exist ids
        """
        if not type(self.repository).existsByID.isImplemented or (self._enables and not type(self.repository).existsByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).existsByID.name)
        if not ids:
            raise InvalidParameterError(reason = 'Require parameter [ids]')
        # Call
        return self.repository.existsByID(ids)

    def existsByQuery(self, condition):
        """Exists by query
        Parameters:
            condition                       The condition
        Returns:
            True / False
        """
        if not type(self.repository).existsByQuery.isImplemented or (self._enables and not type(self.repository).existsByQuery.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).existsByQuery.name)
        if not condition:
            raise InvalidParameterError(reason = 'Require parameter [condition]')
        # Call
        return self.repository.existsByQuery(condition)

    def getByID(self, id):
        """Get by id
        Parameters:
            id                              The id
        Returns:
            The model object, None will be returned if not found
        """
        if not type(self.repository).getByID.isImplemented or (self._enables and not type(self.repository).getByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).getByID.name)
        if not id:
            raise InvalidParameterError(reason = 'Require parameter [id]')
        # Call
        return self.repository.getByID(id)

    def getsByID(self, ids, sorts = None):
        """Get a couple of models by id
        Parameters:
            ids                             A list of id
        Returns:
            A dict which key is id, value is Model object, None will be returned if not found
        """
        if not type(self.repository).getsByID.isImplemented or (self._enables and not type(self.repository).getsByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).getsByID.name)
        if not ids:
            raise InvalidParameterError(reason = 'Require parameter [ids]')
        # Call
        return self.repository.getsByID(ids, sorts)

    def getsByQuery(self, condition, sorts = None, start = 0, size = 10):
        """Gets by query
        Parameters:
            condition                       The condition
        Returns:
            A dict which key is id, value is Model object, None will be returned if not found
        """
        if not type(self.repository).getsByQuery.isImplemented or (self._enables and not type(self.repository).getsByQuery.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).getsByQuery.name)
        if not condition:
            raise InvalidParameterError(reason = 'Require parameter [condition]')
        # Call
        return self.repository.getsByQuery(condition, sorts, start, size)

    def gets(self, start = 0, size = 10, sorts = None):
        """Get all
        """
        if not type(self.repository).gets.isImplemented or (self._enables and not type(self.repository).gets.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).gets.name)
        # Call
        return self.repository.gets(start, size, sorts)

    def create(self, model, overwrite = False, configs = None):
        """Create a new model
        Parameters:
            model                           The model object
        Returns:
            The model object which is created
        """
        if not type(self.repository).create.isImplemented or (self._enables and not type(self.repository).create.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).create.name)
        if not model:
            raise InvalidParameterError(reason = 'Require parameter [model]')
        if not isinstance(model, self.modelClass):
            raise InvalidParameterError(reason = 'Invalid parameter type [model]')
        # Set the model metadata
        if not model.metadata:
            model.metadata = Metadata()
        model.metadata.createTime = datetime.now()
        model.metadata.timestamp = time()
        # Create the model
        model = self.repository.create(model, overwrite, configs)
        # Set the ts
        self._timestamp = max(model.metadata.timestamp, self._timestamp)
        # Trigger an event
        self.trigger(EVENT_CREATED, EventArgs(EVENT_CREATED, type(self.repository).create, [ model ], 1, [ model.id ]))
        # Update the watch
        self.updateWatch(WATCH_CREATED, model.metadata.timestamp, model.id, None, model)
        # Done
        return model

    def replace(self, model, configs = None):
        """Replace a model by id
        Returns:
            The replaced model object
        """
        if not type(self.repository).replace.isImplemented or (self._enables and not type(self.repository).replace.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).replace.name)
        if not model:
            raise InvalidParameterError(reason = 'Require parameter [model]')
        if not isinstance(model, self.modelClass):
            raise InvalidParameterError(reason = 'Invalid parameter type [model]')
        # Set the model metadata
        if not model.metadata:
            model.metadata = Metadata()
        ts = time()
        model.metadata.createTime = datetime.now()
        model.metadata.timestamp = ts
        # Replace the model
        res = self.repository.replace(model, configs)
        # Set the ts
        self._timestamp = max(ts, self._timestamp)
        # Trigger an event
        #self.trigger(EVENT_REPLACED, EventArgs(EVENT_REPLACED, type(self.repository).replace, [ model ], 1, [ model.id ]))
        # Update the watch
        self.updateWatch(WATCH_REPLACED, model.metadata.timestamp, model.id, res.before, res.after)
        # Done
        return res

    def updateByID(self, id, updates, configs = None):
        """Update a model by id
        """
        if not type(self.repository).updateByID.isImplemented or (self._enables and not type(self.repository).updateByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).updateByID.name)
        if not id:
            raise InvalidParameterError(reason = 'Require [id]')
        if not updates:
            raise InvalidParameterError(reason = 'Require [updates]')
        # Add update for ts
        ts = time()
        updates = list(updates)
        updates.append(SetAction(key = 'metadata.timestamp', value = ts))
        # Update
        res = self.repository.updateByID(id, updates, configs)
        # Set the ts
        self._timestamp = max(ts, self._timestamp)
        # Trigger an event
        #self.trigger(EVENT_UPDATED, EventArgs(EVENT_UPDATED, type(self.repository).updateByID, [ model ], 1, [ model.id ]))
        # Update the watch
        self.updateWatch(WATCH_UPDATED, ts, res.before.id, res.before, res.after)
        # Done
        return res

    def updatesByID(self, ids, updates, configs = None):
        """Update a couple of models by id
        """
        if not type(self.repository).updatesByID.isImplemented or (self._enables and not type(self.repository).updatesByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).updatesByID.name)
        if not ids:
            raise InvalidParameterError(reason = 'Require [ids]')
        if not updates:
            raise InvalidParameterError(reason = 'Require [updates]')
        # Add update for ts
        ts = time()
        updates = list(updates)
        updates.append(SetAction(key = 'metadata.timestamp', value = time()))
        # Update
        res = self.repository.updatesByID(ids, updates, configs)
        if res.count == 0:
            return res
        # Set the ts
        self._timestamp = max(ts, self._timestamp)
        # Trigger an event
        #self.trigger(EVENT_UPDATED, EventArgs(
        #    EVENT_UPDATED,
        #    type(self.repository).updatesByID,
        #    res.models,
        #    res.count,
        #    [ x.id for x in res.models ] if res.models else None
        #    ))
        # Update the watch
        if res.updates:
            for update in res.updates:
                self.updateWatch(WATCH_UPDATED, ts, update.before.id, update.before, update.after)
        else:
            # When doing fast-update, the ids here is not always correct
            for id in ids:
                self.updateWatch(WATCH_UPDATED, ts, id, None, None)
        # Done
        return res

    def updatesByQuery(self, condition, updates, configs = None):
        """Update a couple of models by query
        """
        if not type(self.repository).updatesByQuery.isImplemented or (self._enables and not type(self.repository).updatesByQuery.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).updatesByQuery.name)
        if not condition:
            raise InvalidParameterError(reason = 'Require [condition]')
        if not updates:
            raise InvalidParameterError(reason = 'Require [updates]')
        # Add update for ts
        ts = time()
        updates = list(updates)
        updates.append(SetAction(key = 'metadata.timestamp', value = time()))
        # Update
        res = self.repository.updatesByQuery(condition, updates, configs)
        if res.count == 0:
            return res
        # Set the ts
        self._timestamp = max(ts, self._timestamp)
        # Trigger an event
        #self.trigger(EVENT_UPDATED, EventArgs(
        #    EVENT_UPDATED,
        #    type(self.repository).updatesByID,
        #    res.models,
        #    res.count,
        #    [ x.id for x in res.models ] if res.models else None
        #    ))
        # Update the watch
        if res.updates:
            for update in res.updates:
                self.updateWatch(WATCH_UPDATED, ts, update.before.id, update.before, update.after)
        else:
            # Reset
            self.updateWatch(WATCH_RESET, ts, None, None, None)
        # Done
        return res

    def deleteByID(self, id, configs = None):
        """Delete a model
        """
        if not type(self.repository).deleteByID.isImplemented or (self._enables and not type(self.repository).deleteByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).deleteByID.name)
        if not id:
            raise InvalidParameterError(reason = 'Require [id]')
        ts = time()
        # Delete
        model = self.repository.deleteByID(id, configs)
        # Set the ts
        self._timestamp = max(ts, self._timestamp)
        # Trigger an event
        self.trigger(EVENT_DELETED, EventArgs(EVENT_DELETED, type(self.repository).deleteByID, model, 1, [ id ]))
        # Update the watch
        if model:
            self.updateWatch(WATCH_DELETED, ts, model.id, model, None)
        else:
            self.updateWatch(WATCH_DELETED, ts, id, None, None)
        # Done
        return model

    def deletesByID(self, ids, configs = None):
        """Delete a couple of models
        """
        if not type(self.repository).deletesByID.isImplemented or (self._enables and not type(self.repository).deletesByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).deletesByID.name)
        if not ids:
            raise InvalidParameterError(reason = 'Require [ids]')
        ts = time()
        # Delete
        res = self.repository.deletesByID(ids, configs)
        if res.count == 0:
            return res
        # Set the ts
        self._timestamp = max(ts, self._timestamp)
        # Trigger an event
        # NOTE:
        #   Here, the ids is the max deletes collection
        #self.trigger(EVENT_DELETED, EventArgs(
        #    EVENT_DELETED,
        #    type(self.repository).deletesByID,
        #    res.models,
        #    res.count,
        #    [ x.id for x in res.models ] if res.models else None
        #    ))
        # Update the watch
        if res.models:
            for model in res.models:
                self.updateWatch(WATCH_DELETED, ts, model.id, model, None)
        else:
            # When doing fast-delete, the ids here is not always correct
            for id in ids:
                self.updateWatch(WATCH_DELETED, ts, model.id, None, None)
        # Done
        return res

    def deletesByQuery(self, condition, configs = None):
        """Delete a couple of models by query
        """
        if not type(self.repository).deletesByQuery.isImplemented or (self._enables and not type(self.repository).deletesByQuery.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).deletesByQuery.name)
        if not condition:
            raise InvalidParameterError(reason = 'Require [condition]')
        ts = time()
        # Delete
        res = self.repository.deletesByQuery(condition, configs)
        if res.count == 0:
            return res
        # Set the ts
        self._timestamp = max(ts, self._timestamp)
        # Trigger an event
        self.trigger(EVENT_DELETED, EventArgs(
            EVENT_DELETED,
            type(self.repository).deletesByQuery,
            res.models,
            res.count,
            [ x.id for x in res.models ] if res.models else None,
            ))
        # Update the watch
        if res.models:
            for model in res.models:
                self.updateWatch(WATCH_DELETED, ts, model.id, model, None)
        else:
            self.updateWatch(WATCH_RESET, ts, None, None, None)
        # Done
        return res

    def count(self):
        """Count the model numbers
        """
        if not type(self.repository).count.isImplemented or (self._enables and not type(self.repository).count.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).count.name)
        # Count
        return self.repository.count()

    def countByID(self, ids):
        """Count the model numbers by id
        Parameters:
            ids                             A list of id
        Returns:
            The number
        """
        if not type(self.repository).countByID.isImplemented or (self._enables and not type(self.repository).countByID.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).countByID.name)
        if not ids:
            raise InvalidParameterError(reason = 'Require [ids]')
        # Count
        return self.repository.countByID(ids)

    def countByQuery(self, condition):
        """Count the model numbers by condition
        Parameters:
            condition                       The condition
        Returns:
            The number
        """
        if not type(self.repository).countByQuery.isImplemented or (self._enables and not type(self.repository).countByQuery.name in self._enables):
            raise FeatureNotEnabledError(type(self.repository).countByQuery.name)
        if not condition:
            raise InvalidParameterError(reason = 'Require [condition]')
        # Count
        return self.repository.countByQuery(condition)

    def watch(self, condition = None):
        """Watch the changes
        Parameters:
            condition                       The condition used to watch
        Returns:
            Yield of ResourceWatchChangeSet
        """
        # Create the waiting queue
        qid = str(uuid4())
        queue = self._queueClass()
        with self._watchQueueLock:
            self._watchQueues[qid] = (condition, queue)
        latestTimetamp = None
        # Watching the data
        try:
            self.logger.debug('Watch for [%s] started', self.modelClass.__name__)
            # Get the preseved items
            if condition:
                models = self.getsByQuery(condition, start = 0, size = 0)
            else:
                models = self.gets(start = 0, size = 0)
            # Iterate the models
            for model in models:
                # Set the latest timestamp
                # NOTE: Here, we MUST use <= to check the timestamp since updates/deletes actions will have the exactly same timestamp
                if not latestTimetamp or latestTimetamp <= model.metadata.timestamp:
                    latestTimetamp = model.metadata.timestamp
                # Yield return
                yield ResourceWatchChangeSet(name = WATCH_PRESERVED, timestamp = model.metadata.timestamp, modelID = model.id, oldModel = None, newModel = model)
            # Wait for the queue
            while True:
                changeSet = queue.get()
                # NOTE: Current implementation may cause the 'DELETE' sent to the watcher even if the models has already deletes before scanning the database.
                #       So, the watcher should deal with the case that the deleted models are not existed
                # NOTE: Here, we MUST use >= to check the timestamp since updates/deletes actions will have the exactly same timestamp
                if changeSet.name == WATCH_DELETED or not latestTimetamp or changeSet.timestamp >= latestTimetamp:
                    latestTimetamp = max(latestTimetamp or 0, changeSet.timestamp)
                    yield changeSet
            # Done
        except GeneratorExit:
            # A normal
            self.logger.debug('Watch for [%s] exited', self.modelClass.__name__)
        except:
            # Error when watching
            self.logger.exception('Failed to watch [%s]', self.modelClass.__name__)
        finally:
            # Clear
            with self._watchQueueLock:
                del self._watchQueues[qid]

    def getFeatures(cls):
        """Get the features
        """
        enabledFeatures = []
        for feature in self.repository.features:
            if feature.isImplemented and (not self._enables or feature.name in self._enables):
                enabledFeatures.append(feature)
        # Done
        return enabledFeatures

    @classmethod
    def invokeHandler(cls, features):
        """The decorator for adding an invoke handler
        """
        def decorator(method):
            """The decorator method
            """
            cls.addInvokeHandler(features, handler)
            return method
        # Done
        return decorator

    @classmethod
    def addInvokeHandler(cls, features, handler):
        """Add an invoke handler
        """
        if not hasattr(cls, '__featurepipeline__'):
            setattr(cls, '__featurepipeline__', )
