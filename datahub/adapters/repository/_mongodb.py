# encoding=utf8

""" The mongodb repository adapter
    Author: lipixun
    Created Time : 二  3/15 16:29:23 2016

    File Name: _mongodb.py
    Description:

"""

import logging

from itertools import chain

from pymongo import IndexModel, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from datahub.model import DumpContext
from datahub.errors import BadValueError, DuplicatedKeyError, ModelNotFoundError
from datahub.updates import PushAction, PushsAction, PopAction, SetAction, ClearAction
from datahub.conditions import AndCondition, OrCondition, NotCondition, KeyValueCondition, KeyValuesCondition, ExistCondition, \
    NonExistCondition, LargerCondition, SmallerCondition
from datahub.repository import Repository, ReplaceResult, UpdateResult, UpdatesResult, DeletesResult

class MongodbRepository(Repository):
    """The mongodb repository
    """
    def __init__(self, modelClass, database, sorts = None):
        """Create a new MongodbRepository
        """
        super(MongodbRepository, self).__init__(modelClass, sorts)
        # Check the metadata
        metadata = modelClass.getMetadata()
        if not metadata:
            raise ValueError('Require metadata of the model [%s]' % modelClass.__name__)
        # Get the collection
        if not metadata.namespace:
            raise ValueError('Require namespace in the model [%s] metadata' % modelClass.__name__)
        self.collection = database[metadata.namespace]
        # Get the indices
        if metadata.indices or metadata.expires:
            indices = []
            if metadata.indices:
                indices.extend([ IndexModel([ (x, ASCENDING) for x in idx.keys ], unique = idx.unique, sparse = idx.sparse) for idx in metadata.indices ])
            if metadata.expires:
                indices.extend([ IndexModel([ (x, ASCENDING) ], expireAfterSeconds = 0) for x in metadata.expires ])
            # Create the indices
            self.collection.create_indexes(indices)

    def __getquerybycondition__(self, condition):
        """Get query by condition
        """
        if isinstance(condition, AndCondition):
            return { '$and': [ self.__getquerybycondition__(x) for x in condition.conditions ] }
        elif isinstance(condition, OrCondition):
            return { '$or': [ self.__getquerybycondition__(x) for x in condition.conditions ] }
        elif isinstance(condition, NotCondition):
            return { '$nor': [ self.__getquerybycondition__(condition.condition) ] }
        elif isinstance(condition, KeyValueCondition):
            if condition.equals:
                return { condition.key: condition.value }
            else:
                return { condition.key: { '$ne': condition.value } }
        elif isinstance(condition, KeyValuesCondition):
            if condition.includes:
                return { condition.key: { '$in': condition.values } }
            else:
                return { condition.key: { '$nin': condition.values } }
        elif isinstance(condition, ExistCondition):
            return { condition.key: { '$exists': True } }
        elif isinstance(condition, NonExistCondition):
            return { condition.key: { '$exists': False } }
        elif isinstance(condition, LargerCondition):
            if condition.equals:
                return { condition.key: { '$gte': condition.value } }
            else:
                return { condition.key: { '$gt': condition.value } }
        elif isinstance(condition, SmallerCondition):
            if condition.equals:
                return { condition.key: { '$lte': condition.value } }
            else:
                return { condition.key: { '$lt': condition.value } }
        else:
            raise TypeError('Unknown condition type [%s]' % type(condition).__name__)

    def __optimizequery__(self, query):
        """Optimize the query
        """
        if len(query) == 1:
            # Check if a logic query
            key, value = query.keys()[0], query.values()[0]
            if key in ('$and', '$or'):
                # Optimize children
                value = [ self.__optimizequery__(x) for x in value ]
                # Merge queries
                ands, ors, others = [], [], []
                for q in value:
                    k, v = q.keys()[0], q.values()[0]
                    if k == '$and':
                        ands.extend(v)
                    elif k == '$or':
                        ors.extend(v)
                    else:
                        others.append(q)
                value = []
                value.extend(others)
                if key == '$and':
                    value.extend(ands)
                    if ors:
                        value.append({ '$or': ors })
                else:
                    value.extend(ors)
                    if ands:
                        value.append({ '$and': ands })
                # Done
                return { key: value }
            elif key == '$nor':
                nors, others = [], []
                # Optimize children
                value = [ self.__optimizequery__(x) for x in value ]
                for q in value:
                    k, v = q.keys()[0], q.values()[0]
                    if k == '$nor':
                        nors.extend(v)
                    if k == '$and':
                        others.extend(v)
                    else:
                        others.append(q)
                if not nors:
                    return { '$nor': others }
                else:
                    if others:
                        return { '$and': [ { '$nor': others } ] + nors }
                    else:
                        return { '$and': nors }
            # Done
        return query

    def getQueryByCondition(self, condition):
        """Get mongodb query by condition
        """
        # Get original query
        query = self.__getquerybycondition__(condition)
        # Optimize the query
        query = self.__optimizequery__(query)
        # Done
        return query

    def getSortBySort(self, sort):
        """Get mongodb sort by sort
        """
        return (sort.key, ASCENDING if sort.ascending else DESCENDING)

    def getUpdatesByUpdates(self, updates):
        """get mongodb updates by updates
        """
        mongoUpdates = []
        # Generate one by one
        for update in updates:
            if isinstance(update, PushAction):
                if not update.position is None:
                    # Set position
                    mongoUpdates.append(('$push', { update.key: { '$each': [ update.value ], '$position': update.position } }))
                else:
                    # No position
                    mongoUpdates.append(('$push', { update.key: update.value }))
            elif isinstance(update, PushsAction):
                if not update.position is None:
                    # Set position
                    mongoUpdates.append(('$push', { update.key: { '$each': update.values, '$position': update.position } }))
                else:
                    # No position
                    mongoUpdates.append(('$push', { update.key: { '$each': update.values } }))
            elif isinstance(update, PopAction):
                mongoUpdates.append(('$pop', { update.key: -1 if update.head else 1 }))
            elif isinstance(update, SetAction):
                mongoUpdates.append(( '$set', { update.key: update.value }))
            elif isinstance(update, ClearAction):
                mongoUpdates.append(('$unset', { update.key: {} }))
            else:
                raise TypeError('Unknown update action type [%s]' % type(update).__name__)
        # Merge to the update args
        mongoUpdateArgs = {}
        for op, arg in mongoUpdates:
            if not op in mongoUpdateArgs:
                mongoUpdateArgs[op] = {}
            mongoUpdateArgs[op].update(arg)
        # Done
        return mongoUpdateArgs

    @Repository.existByID.implement
    def existByID(self, id):
        """Exist by id
        Parameters:
            id                              The id
        Returns:
            True / False
        """
        return not self.collection.find_one(id, projection = {}) is None

    @Repository.existsByID.implement
    def existsByID(self, ids):
        """Exists by id
        Parameters:
            ids                             A list of id
        Returns:
            A list of exist ids
        """
        return [ x['_id'] for x in self.collection.find({ '_id': { '$in': ids } }, projection = {}) ]

    @Repository.existsByQuery.implement
    def existsByQuery(self, condition):
        """Exists by query
        Parameters:
            condition                       The condition
        Returns:
            A list of exist ids
        """
        return [ x['_id'] for x in self.collection.find(self.getQueryByCondition(condition), projection = {}) ]

    @Repository.getByID.implement
    def getByID(self, id):
        """Get by id
        """
        doc = self.collection.find_one(id)
        if doc:
            model = self.modelClass(doc)
            model.validate()
            return model

    @Repository.getsByID.implement
    def getsByID(self, ids, sorts = None):
        """Get a couple of models by id
        Parameters:
            ids                             A list of id
        Returns:
            Yield of model
        """
        for doc in self.collection.find({ '_id': { '$in': ids } }, sort = [ self.getSortBySort(x) for x in sorts or self.sorts or [] ]):
            model = self.modelClass(doc)
            model.validate()
            yield model

    @Repository.getsByQuery.implement
    def getsByQuery(self, condition, sorts = None, start = 0, size = 10):
        """Gets by query
        Parameters:
            condition                       The condition
        Returns:
            Yield of model
        """
        for doc in self.collection.find(self.getQueryByCondition(condition),
            sort = [ self.getSortBySort(x) for x in sorts or self.sorts or [] ],
            skip = start,
            limit = size
            ):
            model = self.modelClass(doc)
            model.validate()
            yield model

    @Repository.gets.implement
    def gets(self, start = 0, size = 10, sorts = None):
        """Get all models
        Returns:
            Yield of model
        """
        for doc in self.collection.find(sort = [ self.getSortBySort(x) for x in sorts or self.sorts or [] ], skip = start, limit = size):
            model = self.modelClass(doc)
            model.validate()
            yield model

    @Repository.create.implement
    def create(self, model, overwrite = False, configs = None):
        """Create a new model
        Parameters:
            model                           The model object
        Returns:
            The model object which is created
        """
        model.validate()
        if not overwrite:
            try:
                self.collection.insert_one(model.dump(DumpContext(datetime2str = False)))
            except DuplicateKeyError as error:
                raise DuplicatedKeyError(error.message)
        else:
            self.collection.replace_one({ '_id': model.id }, model.dump(DumpContext(datetime2str = False)), upsert = True)
        # Done
        return model

    @Repository.replace.implement
    def replace(self, model, configs = None):
        """Replace a model by id
        Parameters:
            model                           The model object
        Returns:
            ReplaceResult
        """
        # The document before replacing will be returned
        doc = self.collection.find_one_and_replace({ '_id': model.id }, model.dump(DumpContext(datetime2str = False)))
        if not doc:
            raise ModelNotFoundError
        # Load model
        oldModel = self.modelClass(doc)
        oldModel.validate()
        # Done
        return ReplaceResult(before = oldModel, after = model)

    @Repository.updateByID.implement
    def updateByID(self, id, updates, configs = None):
        """Update a model by id
        Parameters:
            id                              The model id
            updates                         The json updates
        Returns:
            The updated model if updated, None will be returned if not updated
        NOTE:
            Current implementation cannot detect if the model is updated or not (which has no changes)
        """
        doc = self.collection.find_one_and_update({ '_id': id }, self.getUpdatesByUpdates(updates))
        if not doc:
            raise ModelNotFoundError
        # Load the old model
        oldModel = self.modelClass(doc)
        oldModel.validate()
        # TODO: Create the updated model by applying the updates
        newModel = None
        #newModel = oldModel.clone()
        #newModel.update(updates)
        doc = self.collection.find_one(id)
        if doc:
            newModel = self.modelClass(doc)
            newModel.validate()
        # Done
        return UpdateResult(before = oldModel, after = newModel)

    @Repository.updatesByID.implement
    def updatesByID(self, ids, updates, configs = None):
        """Update a couple of models by id
        Parameters:
            ids                             The model id list
            updates                         The json updates
        Returns:
            The UpdatesResult object
        NOTE:
            In order to get the updated models, the document is updated one by one.
            So, if you wanna update lots of documents in a reasonable time and ignore the returned models,
            please set config:
                fastUpdate = True
        """
        if configs and configs.get('fastUpdate', False):
            # Use fast update
            return UpdatesResult(count = self.collection.update_many({ '_id': { '$in': ids } }, self.getUpdatesByUpdates(updates)).modified_count)
        else:
            # Normal update
            mongoUpdates = self.getUpdatesByUpdates(updates)
            models = []
            for id in ids:
                doc = self.collection.find_one_and_update({ '_id': id }, mongoUpdates)
                if doc:
                    # Load model, the model before updated
                    oldModel = self.modelClass(doc)
                    oldModel.validate()
                    # TODO: Create the updated model by applying the updates
                    #newModel = oldModel.clone()
                    #newModel.update(updates)
                    # Add
                    models.append((oldModel, None))
            # Done
            return UpdatesResult(updates = [ UpdateResult(before = x, after = y) for (x, y) in models ], count = len(models))

    @Repository.updatesByQuery.implement
    def updatesByQuery(self, condition, updates, configs = None):
        """Update a couple of models by query
        Parameters:
            condition                       The condition
            updates                         The json updates
        Returns:
            The UpdatesResult object
        NOTE:
            In order to get the updated models, the document is updated one by one.
            So, if you wanna update lots of documents in a reasonable time and ignore the returned models,
            please set config:
                fastUpdate = True
        """
        if configs and configs.get('fastUpdate', False):
            # Use fast update
            return UpdatesResult(count = self.collection.update_many(self.getQueryByCondition(condition), self.getUpdatesByUpdates(updates)).modified_count)
        else:
            # Get updates
            mongoUpdates = self.getUpdatesByUpdates(updates)
            # Iterate updating
            updatedIDs = []
            models = []         # A list of (before, after)
            while True:
                if not models:
                    updateCondition = condition
                else:
                    updateCondition = AndCondition(conditions = [
                        condition,
                        NotCondition(condition = KeyValuesCondition(key = '_id', values = updatedIDs))
                        ])
                doc = self.collection.find_one_and_update(self.getQueryByCondition(updateCondition), mongoUpdates)
                if doc:
                    # Load model, the model before updated
                    oldModel = self.modelClass(doc)
                    oldModel.validate()
                    # TODO: Create the updated model by applying the updates
                    #newModel = oldModel.clone()
                    #newModel.update(updates)
                    # Add
                    models.append((oldModel, None))
                    updatedIDs.append(oldModel.id)
                else:
                    # No more
                    break
            # Done
            return UpdatesResult(updates = [ UpdateResult(before = x, after = y) for (x, y) in models ], count = len(models))

    @Repository.deleteByID.implement
    def deleteByID(self, id, configs = None):
        """Delete a model
        Parameters:
            id                                  The id
        Returns:
            - The deleted model
        NOTE:
            In order to get the removed model, the document is fetched before removed
            If you could ignore the returned model, please set config:
                fastRemove = True
        """
        if configs and configs.get('fastRemove', False):
            # Fast remove
            if self.collection.delete_one({ '_id': id }).deleted_count == 0:
                raise ModelNotFoundError
        else:
            # Remove it
            doc = self.collection.find_one_and_delete({ '_id': id })
            if not doc:
                raise ModelNotFoundError
            # Load model
            model = self.modelClass(doc)
            model.validate()
            # Done
            return model

    @Repository.deletesByID.implement
    def deletesByID(self, ids, configs = None):
        """Delete a couple of models
        Parameters:
            ids                                 A list of id
        Returns:
            The DeletesResult
        NOTE:
            In order to get the removed models, the document is fetched before removed
            So, if you wanna remove lots of documents in a reasonable time and ignore the returned models,
            please set config:
                fastRemove = True
        """
        if configs and configs.get('fastRemove', False):
            # Fast remove
            return DeletesResult(count = self.collection.delete_many({ '_id': { '$in': ids } }).deleted_count)
        else:
            models = []
            for id in ids:
                doc = self.collection.find_one_and_delete({ '_id': id })
                if doc:
                    # Load model
                    model = self.modelClass(doc)
                    model.validate()
                    models.append(model)
            # Return
            return DeletesResult(models = models, count = len(models))

    @Repository.deletesByQuery.implement
    def deletesByQuery(self, condition, configs = None):
        """Delete a couple of models by query
        Parameters:
            condition                           The condition
        Returns:
            The DeletesResult
        NOTE:
            In order to get the removed models, the document is fetched before removed
            So, if you wanna remove lots of documents in a reasonable time and ignore the returned models,
            please set config:
                fastRemove = True
        """
        if configs and configs.get('fastRemove', False):
            # Fast remove
            return DeletesResult(count = self.collection.delete_many(self.getQueryByCondition(condition)).deleted_count)
        else:
            mongoQuery = self.getQueryByCondition(condition)
            models = []
            while True:
                doc = self.collection.find_one_and_delete(mongoQuery)
                if doc:
                    # Load the deleted model
                    model = self.modelClass(doc)
                    model.validate()
                    models.append(model)
                else:
                    # No more models
                    break
            # Return
            return DeletesResult(models = models, count = len(models))

    @Repository.count.implement
    def count(self):
        """Count all
        Returns:
            The number
        """
        return self.collection.count()

    @Repository.countByID.implement
    def countByID(self, ids):
        """Count the model numbers by id
        Parameters:
            ids                             A list of id
        Returns:
            The number
        """
        return self.collection.count({ '_id': { '$in': ids } })

    @Repository.countByQuery.implement
    def countByQuery(self, condition):
        """Count the model numbers by condition
        Parameters:
            condition                       The condition
        Returns:
            The number
        """
        return self.collection.count(self.getQueryByCondition(condition))

    @classmethod
    def getFeatures(cls):
        """Get the features
        """
        features = []
        for key in dir(cls):
            value = getattr(cls, key)
            if isinstance(value, Feature):
                features.append(value)
        return features
