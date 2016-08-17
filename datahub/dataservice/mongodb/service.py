# encoding=utf8

""" The mongodb datahub data service
    Author: lipixun
    Created Time : 二  8/16 11:26:39 2016

    File Name: service.py
    Description:

"""

from pymongo import IndexModel, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from datahub.spec import *
from datahub.model import DumpContext
from datahub.errors import BadValueError, DuplicatedKeyError, ModelNotFoundError, InvalidParameterError
from datahub.updates import PushAction, PushsAction, PopAction, SetAction, ClearAction
from datahub.conditions import AndCondition, OrCondition, NotCondition, KeyValueCondition, KeyValuesCondition, ExistCondition, \
    NonExistCondition, GreaterCondition, LesserCondition
from datahub.dataservice.interface import DataServiceInterface

class MongodbDataStorage(object):
    """The mongodb data storage
    """
    @classmethod
    def __getquerybycondition__(cls, condition):
        """Get query by condition
        """
        if isinstance(condition, AndCondition):
            return { "$and": [ cls.__getquerybycondition__(x) for x in condition.conditions ] }
        elif isinstance(condition, OrCondition):
            return { "$or": [ cls.__getquerybycondition__(x) for x in condition.conditions ] }
        elif isinstance(condition, NotCondition):
            return { "$nor": [ cls.__getquerybycondition__(condition.condition) ] }
        elif isinstance(condition, KeyValueCondition):
            if condition.equals:
                return { condition.key: condition.value }
            else:
                return { condition.key: { "$ne": condition.value } }
        elif isinstance(condition, KeyValuesCondition):
            if condition.includes:
                return { condition.key: { "$in": condition.values } }
            else:
                return { condition.key: { "$nin": condition.values } }
        elif isinstance(condition, ExistCondition):
            return { condition.key: { "$exists": True } }
        elif isinstance(condition, NonExistCondition):
            return { condition.key: { "$exists": False } }
        elif isinstance(condition, GreaterCondition):
            if condition.equals:
                return { condition.key: { "$gte": condition.value } }
            else:
                return { condition.key: { "$gt": condition.value } }
        elif isinstance(condition, LesserCondition):
            if condition.equals:
                return { condition.key: { "$lte": condition.value } }
            else:
                return { condition.key: { "$lt": condition.value } }
        else:
            raise TypeError("Unknown condition type [%s]" % type(condition).__name__)

    @classmethod
    def __optimizequery__(cls, query):
        """Optimize the query
        """
        if len(query) == 1:
            # Check if a logic query
            key, value = query.keys()[0], query.values()[0]
            if key in ("$and", "$or"):
                # Optimize children
                value = [ cls.__optimizequery__(x) for x in value ]
                # Merge queries
                ands, ors, others = [], [], []
                for q in value:
                    k, v = q.keys()[0], q.values()[0]
                    if k == "$and":
                        ands.extend(v)
                    elif k == "$or":
                        ors.extend(v)
                    else:
                        others.append(q)
                value = []
                value.extend(others)
                if key == "$and":
                    value.extend(ands)
                    if ors:
                        value.append({ "$or": ors })
                else:
                    value.extend(ors)
                    if ands:
                        value.append({ "$and": ands })
                # Done
                return { key: value }
            elif key == "$nor":
                nors, others = [], []
                # Optimize children
                value = [ cls.__optimizequery__(x) for x in value ]
                for q in value:
                    k, v = q.keys()[0], q.values()[0]
                    if k == "$nor":
                        nors.extend(v)
                    if k == "$and":
                        others.extend(v)
                    else:
                        others.append(q)
                if not nors:
                    return { "$nor": others }
                else:
                    if others:
                        return { "$and": [ { "$nor": others } ] + nors }
                    else:
                        return { "$and": nors }
            # Done
        return query

    @classmethod
    def getQueryByCondition(cls, condition):
        """Get mongodb query by condition
        """
        # Get original query
        query = cls.__getquerybycondition__(condition)
        # Optimize the query
        query = cls.__optimizequery__(query)
        # Done
        return query

    @classmethod
    def getUpdatesByUpdates(cls, updates):
        """get mongodb updates by updates
        """
        mongoUpdates = []
        # Generate one by one
        for update in updates:
            if isinstance(update, PushAction):
                if not update.position is None:
                    # Set position
                    mongoUpdates.append(("$push", { update.key: { "$each": [ update.value ], "$position": update.position } }))
                else:
                    # No position
                    mongoUpdates.append(("$push", { update.key: update.value }))
            elif isinstance(update, PushsAction):
                if not update.position is None:
                    # Set position
                    mongoUpdates.append(("$push", { update.key: { "$each": update.values, "$position": update.position } }))
                else:
                    # No position
                    mongoUpdates.append(("$push", { update.key: { "$each": update.values } }))
            elif isinstance(update, PopAction):
                mongoUpdates.append(("$pop", { update.key: -1 if update.head else 1 }))
            elif isinstance(update, SetAction):
                mongoUpdates.append(( "$set", { update.key: update.value }))
            elif isinstance(update, ClearAction):
                mongoUpdates.append(("$unset", { update.key: {} }))
            else:
                raise TypeError("Unknown update action type [%s]" % type(update).__name__)
        # Merge to the update args
        mongoUpdateArgs = {}
        for op, arg in mongoUpdates:
            if not op in mongoUpdateArgs:
                mongoUpdateArgs[op] = {}
            mongoUpdateArgs[op].update(arg)
        # Done
        return mongoUpdateArgs

    @classmethod
    def instance(cls, modelCls, mongodbContext):
        """Create a MongodbDataService by mongodb context
        """
        return MongodbDataService(modelCls, mongodbContext)

    @classmethod
    def collection(cls, modelCls, collection):
        """Create a MongodbDataService by mongodb client, database and collection
        """
        return cls.instance(modelCls, StaticMongodbCollectionContext(collection))

    @classmethod
    def exist(cls, collection, modelCls, id, **ctx):
        """Check if a model with id exists
        Returns:
            True / False
        """
        return not collection.find_one({ "_id": id }, projection = {}) is None

    @classmethod
    def getOne(cls, collection, modelCls, id, **ctx):
        """Get one model
        Returns:
            Model object or None
        """
        doc = collection.find_one(id)
        if doc:
            model = modelCls(doc)
            model.validate()
            return model

    @classmethod
    def gets(cls, collection, modelCls, ids = None, start = 0, size = 0, sorts = None, **ctx):
        """Get models
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        if ids is None:
            # Get all
            query = {}
        elif isinstance(ids, (tuple, list)):
            # Get ids
            query = { "_id": { "$in": ids } }
        else:
            # Get a single id
            query = { "_id": ids }
        # Get models
        for doc in collection.find(query, sort = [ (x.key, ASCENDING if x.ascending else DESCENDING) for x in sorts ] if sorts else None, skip = start, limit = size):
            model = modelCls(doc)
            model.validate()
            yield model

    @classmethod
    def getByQuery(cls, collection, modelCls, query, start = 0, size = 0, sorts = None, **ctx):
        """Get by query
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        for doc in collection.find(cls.getQueryByCondition(query),
            sort = [ (x.key, ASCENDING if x.ascending else DESCENDING) for x in sorts ] if sorts else None,
            skip = start,
            limit = size
            ):
            model = modelCls(doc)
            model.validate()
            yield model

    @classmethod
    def create(cls, collection, model, overwrite = False, **ctx):
        """Create a model
        Returns:
            The model id
        """
        # Validate model & dump
        model.validate()
        doc = model.dump(DumpContext(datetime2str = False))
        # Overwrite or not
        if not overwrite:
            try:
                collection.insert_one(doc)
            except DuplicateKeyError as error:
                raise DuplicatedKeyError(error.message, model.id)
        else:
            collection.replace_one({ "_id": model.id }, doc, upsert = True)
        # Done
        return model.id

    @classmethod
    def replace(cls, collection, model, autoCreate = False, **ctx):
        """Replace a model
        Returns:
            The model id
        """
        # Validate model & dump
        model.validate()
        doc = model.dump(DumpContext(datetime2str = False))
        # Replace mongodb
        rtn = collection.replace_one({ "_id": model.id }, doc, upsert = autoCreate)
        if rtn.matched_count == 0 and rtn.modified_count == 0 and rtn.upserted_id is None:
            raise ModelNotFoundError
        # Done
        return model.id

    @classmethod
    def updateOne(cls, collection, id, updates, **ctx):
        """Update a model
        Returns:
            True / False
        """
        return collection.update_one({ "_id": id }, cls.getUpdatesByUpdates(updates)).matched_count == 1

    @classmethod
    def updates(cls, collection, ids, updates, **ctx):
        """Update models
        Returns:
            The number of models that is updated
        """
        if not ids:
            # Instead of delete all datas, we raise an exception in order to avoid potential misoperation risk
            raise InvalidParameterError(reason = "Require ids")
        if isinstance(ids, (tuple, list)):
            # Update multiple ids
            query = { "_id": { "$in": ids } }
        else:
            # Update a single id
            query = { "_id": ids }
        # Update
        return collection.update_many(query, cls.getUpdatesByUpdates(updates)).matched_count

    @classmethod
    def updateByQuery(cls, collection, query, updates, **ctx):
        """Update by query
        Returns:
            The number of models that is updated
        """
        if not query:
            # Instead of delete all datas, we raise an exception in order to avoid potential misoperation risk
            raise InvalidParameterError(reason = "Require query")
        # Update by query
        return collection.update_many(cls.getQueryByCondition(query), cls.getUpdatesByUpdates(updates)).modified_count

    @classmethod
    def deleteOne(cls, collection, id, **ctx):
        """Delete a model
        Returns:
            True / False
        """
        return collection.delete_one({ "_id": id }).deleted_count == 1

    @classmethod
    def deletes(cls, collection, ids, **ctx):
        """Delete models
        Returns:
            The number of models that is deleted
        """
        if not ids:
            # Instead of delete all datas, we raise an exception in order to avoid potential misoperation risk
            raise InvalidParameterError(reason = "Require ids")
        # Delete by query
        if isinstance(ids, (list, tuple)):
            query = { "_id": { "$in": ids }}
        else:
            query = { "_id": ids }
        # Delete it
        return collection.delete_many(query).deleted_count

    @classmethod
    def deleteByQuery(cls, collection, query, **ctx):
        """Delete by query
        Returns:
            The number of models that is deleted
        """
        if not query:
            # Instead of delete all datas, we raise an exception in order to avoid potential misoperation risk
            raise InvalidParameterError(reason = "Require query")
        # Delete by query
        return collection.delete_many(cls.getQueryByCondition(query)).deleted_count

    @classmethod
    def counts(cls, collection, ids, **ctx):
        """Count by ids
        Returns:
            The number of found models
        """
        if ids is None:
            query = {}
        elif isinstance(ids, (list, tuple)):
            query = { "_id": { "$in": ids } }
        else:
            query = { "_id": ids }
        # Count
        return collection.count(query)

    @classmethod
    def countByQuery(cls, collection, query, **ctx):
        """Count by query
        Returns:
            The number of found models
        """
        return collection.count(cls.getQueryByCondition(query))

class StaticMongodbCollectionContext(object):
    """The static mongodb context
    """
    class SimpleContextWrapper(object):
        """The simple context wrapper
        """
        def __init__(self, collection):
            """Create a new SimpleContextWrapper
            """
            self.collection = collection

        def __enter__(self):
            """Enter context
            """
            return self.collection

        def __exit__(self, exctype, excval, exctb):
            """Exit context
            """
            pass

    def __init__(self, collection):
        """Create a new StaticMongodbCollectionContext
        """
        self._collection = collection

    def collection(self, ctx):
        """Get the mongodb collection
        """
        return self.SimpleContextWrapper(self._collection)

class MongodbDataService(DataServiceInterface):
    """The mongodb data service
    """
    def __init__(self, modelCls, mongodbContext):
        """Create a new MongodbDataService
        """
        self.modelCls = modelCls
        self.mongodbContext = mongodbContext

    def exist(self, id, **ctx):
        """Check if a model with id exists
        Returns:
            True / False
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.exist(collection, self.modelCls, id, **ctx)

    def getOne(self, id, **ctx):
        """Get one model
        Returns:
            Model object or None
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.getOne(collection, self.modelCls, id, **ctx)

    def gets(self, ids = None, start = 0, size = 0, sorts = None, **ctx):
        """Get models
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.gets(collection, self.modelCls, ids, start, size, sorts, **ctx)

    def getByQuery(self, query, start = 0, size = 0, sorts = None, **ctx):
        """Get by query
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.getByQuery(collection, self.modelCls, query, start, size, sorts, **ctx)

    def create(self, model, overwrite = False, **ctx):
        """Create a model
        Returns:
            The model id
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.create(collection, model, overwrite, **ctx)

    def replace(self, model, autoCreate = False, **ctx):
        """Replace a model
        Returns:
            The model id
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.replace(collection, model, autoCreate, **ctx)

    def updateOne(self, id, updates, **ctx):
        """Update a model
        Returns:
            True / False
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.updateOne(collection, id, updates, **ctx)

    def updates(self, ids, updates, **ctx):
        """Update models
        Returns:
            The number of models that is updated
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.updates(collection, ids, updates, **ctx)

    def updateByQuery(self, query, updates, **ctx):
        """Update by query
        Returns:
            The number of models that is updated
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.updateByQuery(collection, query, updates, **ctx)

    def deleteOne(self, id, **ctx):
        """Delete a model
        Returns:
            True / False
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.deleteOne(collection, id, **ctx)

    def deletes(self, ids, **ctx):
        """Delete models
        Returns:
            The number of models that is deleted
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.deletes(collection, ids, **ctx)

    def deleteByQuery(self, query, **ctx):
        """Delete by query
        Returns:
            The number of models that is deleted
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.deleteByQuery(collection, query, **ctx)

    def counts(self, ids, **ctx):
        """Count by ids
        Returns:
            The number of found models
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.counts(collection, ids, **ctx)

    def countByQuery(self, query, **ctx):
        """Count by query
        Returns:
            The number of found models
        """
        with self.mongodbContext.collection(ctx) as collection:
            return MongodbDataStorage.countByQuery(collection, query, **ctx)
