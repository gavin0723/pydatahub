# encoding=utf8

""" The mongodb repository adapter
    Author: lipixun
    Created Time : 二  3/15 16:29:23 2016

    File Name: _mongodb.py
    Description:

"""

import logging

from pymongo import IndexModel, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from datahub.spec import *
from datahub.model import DumpContext
from datahub.errors import BadValueError, DuplicatedKeyError, ModelNotFoundError
from datahub.updates import PushAction, PushsAction, PopAction, SetAction, ClearAction
from datahub.conditions import AndCondition, OrCondition, NotCondition, KeyValueCondition, KeyValuesCondition, ExistCondition, \
    NonExistCondition, GreaterCondition, LesserCondition
from datahub.repository import Repository

class MongodbRepository(Repository):
    """The mongodb repository
    """
    logger = logging.getLogger('datahub.adapters.repository.mongodb')

    FEATURES = [
        # The store feature
        FEATURE_STORE_EXIST,
        FEATURE_STORE_GET,
        FEATURE_STORE_CREATE,
        FEATURE_STORE_REPLACE,
        FEATURE_STORE_UPDATE,
        FEATURE_STORE_DELETE,
        FEATURE_STORE_COUNT,
        # The query feature
        FEATURE_QUERY_EXIST,
        FEATURE_QUERY_GET,
        FEATURE_QUERY_UPDATE,
        FEATURE_QUERY_DELETE,
        FEATURE_QUERY_COUNT,
        ]

    def __init__(self, cls, database, sorts = None):
        """Create a new MongodbRepository
        """
        super(MongodbRepository, self).__init__(cls, sorts)
        # Check the metadata
        metadata = cls.getMetadata()
        if not metadata:
            raise ValueError('Require metadata of the model [%s]' % cls.__name__)
        # Get the collection
        if not metadata.namespace:
            raise ValueError('Require namespace in the model [%s] metadata' % cls.__name__)
        self.collection = database[metadata.namespace]
        # Get the indices
        indices = []
        indices.extend([ IndexModel([ (x, ASCENDING) for x in idx.keys ], unique = idx.unique, sparse = idx.sparse) for idx in metadata.getAttrs('index') ])
        indices.extend([ IndexModel([ (x.key, ASCENDING) ], expireAfterSeconds = x.expires) for x in metadata.getAttrs('expire') ])
        # Create the indices
        if indices:
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
        elif isinstance(condition, GreaterCondition):
            if condition.equals:
                return { condition.key: { '$gte': condition.value } }
            else:
                return { condition.key: { '$gt': condition.value } }
        elif isinstance(condition, LesserCondition):
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

    def getMongoQueryByCondition(self, condition):
        """Get mongodb query by condition
        """
        # Get original query
        query = self.__getquerybycondition__(condition)
        # Optimize the query
        query = self.__optimizequery__(query)
        # Done
        return query

    def getMongoSortBySortRule(self, sort):
        """Get mongodb sort by sort
        """
        return (sort.key, ASCENDING if sort.ascending else DESCENDING)

    def getMongoUpdatesByUpdates(self, updates):
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

    def exist(self, id = None, configs = None):
        """Exist
        Parameters:
            id                              The id
        Returns:
            True / False
        """
        if isinstance(id, (list, tuple)):
            query = { '_id': { '$in': id } }
        else:
            query = id
        # Query mongodb
        return not self.collection.find_one(query, projection = {}) is None

    def existByQuery(self, query, configs = None):
        """Exists by query
        Parameters:
            query                           The condition
        Returns:
            True / False
        """
        return not self.collection.find_one(self.getMongoQueryByCondition(query), projection = {}) is None

    def getOne(self, id, configs = None):
        """Get one by id
        Returns:
            Model object
        """
        doc = self.collection.find_one(id)
        if doc:
            model = self.cls(doc)
            model.validate()
            return model

    def get(self, id = None, start = 0, size = 0, sorts = None, configs = None):
        """Get by id
        Returns:
            Yield of Model object
        """
        if isinstance(id, (list, tuple)):
            # Get models
            for doc in self.collection.find(
                { '_id': { '$in': id } },
                sort = [ self.getMongoSortBySortRule(x) for x in sorts or self.sorts or [] ],
                skip = start,
                limit = size
                ):
                model = self.cls(doc)
                model.validate()
                yield model
        elif not id is None:
            # Get a single model
            # NOTE: Ignore the sorts parameters
            doc = self.collection.find_one(id)
            if doc:
                model = self.cls(doc)
                model.validate()
                yield model
        else:
            # Get all models
            for doc in self.collection.find(
                sort = [ self.getMongoSortBySortRule(x) for x in sorts or self.sorts or [] ],
                skip = start,
                limit = size
                ):
                model = self.cls(doc)
                model.validate()
                yield model

    def getByQuery(self, query, sorts = None, start = 0, size = 0, configs = None):
        """Gets by query
        Parameters:
            query                       The condition
        Returns:
            Yield of model
        """
        for doc in self.collection.find(self.getMongoQueryByCondition(query),
            sort = [ self.getMongoSortBySortRule(x) for x in sorts or self.sorts or [] ],
            skip = start,
            limit = size
            ):
            model = self.cls(doc)
            model.validate()
            yield model

    def create(self, model, configs = None):
        """Create a new model
        Parameters:
            model                           The model object
            overwrite                       Whether to overwrite the model or not if the model created is already existed
            configs                         A dict of configs
        Returns:
            Nothing
        Configs:
            overwrite                       Overwrite the model if exists, false by default
        """
        # Check model type
        if not isinstance(model, self.cls):
            raise TypeError('model must be an instance of class [%s]' % self.cls.__name__)
        # Validate model & dump
        model.validate()
        doc = model.dump(DumpContext(datetime2str = False))
        # Write to mongodb
        overwrite = configs.get('overwrite', False) if configs else False
        if not overwrite:
            try:
                self.collection.insert_one(doc)
            except DuplicateKeyError as error:
                raise DuplicatedKeyError(error.message, model.id)
        else:
            self.collection.replace_one({ '_id': model.id }, doc, upsert = True)
        # Done

    def replace(self, model, configs = None):
        """Replace a model by id
        Parameters:
            model                           The model object
            configs                         A dict of configs
        Returns:
            Nothing
        Errors:
            - ModelNotFoundError will be raised if the model not found
        Configs:
            autoCreate                      Auto create the document if not found, false by default
        """
        # Check model type
        if not isinstance(model, self.cls):
            raise TypeError('model must be an instance of class [%s]' % self.cls.__name__)
        # Validate model & dump
        model.validate()
        doc = model.dump(DumpContext(datetime2str = False))
        # Replace mongodb
        autoCreate = configs.get('autoCreate', False) if configs else False
        res = self.collection.replace_one({ '_id': model.id }, doc, upsert = autoCreate)
        if res.matched_count == 0 and res.modified_count == 0 and res.upserted_id is None:
            raise ModelNotFoundError
        # Done

    def update(self, id, updates, configs = None):
        """Update model
        Parameters:
            id                              The model id
            updates                         A list of UpdateAction
            configs                         A dict of configs
        Returns:
            The count of matched models
        """
        ups = self.getMongoUpdatesByUpdates(updates)
        if isinstance(id, (list, tuple)):
            res = self.collection.update_many({ '$in': { '_id': id } }, ups)
        else:
            res = self.collection.update_one({ '_id': id }, ups)
        # Return the count of matched models
        return res.matched_count

    def updateByQuery(self, query, updates, configs = None):
        """Update a couple of models by query
        Parameters:
            query                           The condition
            updates                         A list of UpdateAction
            configs                         A dict of configs
        Returns:
            The count of matched models
        """
        return self.collection.update_many(self.getMongoQueryByCondition(query), self.getMongoUpdatesByUpdates(updates)).modified_count

    def delete(self, id, configs = None):
        """Delete model
        Parameters:
            id                              The id
            configs                         A dict of configs
        Returns:
            The count of deleted models
        """
        if isinstance(id, (list, tuple)):
            res = self.collection.delete_many({ '_id': { '$in': id }})
        else:
            res = self.collection.delete_one({ '_id': id })
        # Done
        return res.deleted_count

    def deleteByQuery(self, query, configs = None):
        """Delete a couple of models by query
        Parameters:
            query                           The condition
            configs                         A dict of configs
        Returns:
            The count of deleted models
        """
        return self.collection.delete_many(self.getMongoQueryByCondition(query)).deleted_count

    def count(self, id = None, configs = None):
        """Count models
        Parameters:
            id                              The id or a list / tuple of id or None
            configs                         A dict of configs
        Returns:
            The count of the counting models
        """
        if isinstance(id, (list, tuple)):
            query = { '_id': { '$in': id } }
        elif not id is None:
            query = { '_id': id }
        else:
            query = None
        # Count
        return self.collection.count(query)

    def countByQuery(self, query, configs = None):
        """Count the model numbers by condition
        Parameters:
            query                           The condition
        Returns:
            The count of the counting models
        """
        return self.collection.count(self.getMongoQueryByCondition(query))

    def support(self, name):
        """Check if the feature is supported
        """
        return name in self.FEATURES
