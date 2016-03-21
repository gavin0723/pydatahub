# encoding=utf8

""" The repository
    Author: lipixun
    Created Time : 五  3/11 19:13:35 2016

    File Name: repository.py
    Description:

        About the updates:

        Like JSON patch RFC: https://tools.ietf.org/html/rfc6902 definitions, we're defining a patch rules like that:

        push        Push value to an array
        { 'push': { 'key': 'path', 'position': '1', 'value': value } }

        pushs       Push values to an array
        { 'pushs': { 'key': 'path', 'position': '1', 'values': values } }

        pop         Pop value from head or tail to an array
        { 'pop': { 'key': 'path', 'head': true / false } }

        set         Set a value
        { 'set': { 'key': 'path', 'value': value } }

        clear       Clear a value
        { 'clear': { 'key': 'path' } }

"""

from datahub.feature import FeatureMetaClass, feature
from datahub.model import DataModel, IDDataModel, StringType, IntegerType, BooleanType, ListType, ModelType, DictType, AnyType
from datahub.errors import FeatureNotSupportedError, BadValueError, DuplicatedKeyError, ModelNotFoundError

from datahub.sorts import SortRule
from datahub.conditions import ConditionType

from result import UpdatesResult, DeletesResult

class Repository(object):
    """The repository interface
    """
    __metaclass__ = FeatureMetaClass

    def __init__(self, modelClass, sorts):
        """The model type
        """
        self.modelClass = modelClass
        self.sorts = sorts

    @feature(name = 'store.exist', params = dict(id = StringType()))
    def existByID(self, id):
        """Exist by id
        Parameters:
            id                              The id
        Returns:
            True / False
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.exists', params = dict(ids = ListType(StringType())))
    def existsByID(self, ids):
        """Exists by id
        Parameters:
            ids                             A list of id
        Returns:
            A list of exist ids
        """
        raise FeatureNotSupportedError

    @feature(name = 'query.exists', params = dict(condition = ConditionType()))
    def existsByQuery(self, condition):
        """Exists by query
        Parameters:
            condition                       The condition
        Returns:
            A list of exist ids
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.get', params = dict(id = StringType()))
    def getByID(self, id):
        """Get by id
        Parameters:
            id                              The id
        Returns:
            The model object, None will be returned if not found
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.gets', params = dict(ids = ListType(StringType()), sorts = ListType(ModelType(SortRule))))
    def getsByID(self, ids, sorts = None):
        """Get a couple of models by id
        Parameters:
            ids                             A list of id
        Returns:
            Yield of model
        """
        raise FeatureNotSupportedError

    @feature(name = 'query.gets',
        params = dict(condition = ConditionType(), sorts = ListType(ModelType(SortRule)), start = IntegerType(), size = IntegerType())
        )
    def getsByQuery(self, condition, sorts = None, start = 0, size = 10):
        """Gets by query
        Parameters:
            condition                       The condition
        Returns:
            Yield of model
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.getall', params = dict(sorts = ListType(ModelType(SortRule)), start = IntegerType(), size = IntegerType()))
    def gets(self, start = 0, size = 10, sorts = None):
        """Get all models
        Returns:
            Yield of model
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.create', params = dict(model = ModelType(IDDataModel), overwrite = BooleanType(), configs = DictType(AnyType())))
    def create(self, model, overwrite = False, configs = None):
        """Create a new model
        Parameters:
            model                           The model object
        Returns:
            The model object which is created
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.replace', params = dict(model = ModelType(IDDataModel), configs = DictType(AnyType())))
    def replace(self, model, configs = None):
        """Replace a model by id
        Parameters:
            model                           The model object
        Returns:
            The model object which is replaced, None will be returned if not replaced
        Errors:
            - ModelNotFoundError will be raised if the model not found
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.update', params = dict(id = StringType(), updates = DictType(AnyType()), configs = DictType(AnyType())))
    def updateByID(self, id, updates, configs = None):
        """Update a model by id
        Parameters:
            id                              The model id
            updates                         The json updates
        Returns:
            The updated model if updated, None will be returned if not updated
        Errors:
            - ModelNotFoundError will be raised if the model not found
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.updates', params = dict(ids = ListType(StringType()), updates = DictType(AnyType()), configs = DictType(AnyType())))
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
        raise FeatureNotSupportedError

    @feature(name = 'query.updates', params = dict(condition = ConditionType(), updates = DictType(AnyType()), configs = DictType(AnyType())))
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
        raise FeatureNotSupportedError

    @feature(name = 'store.delete', params = dict(id = StringType(), configs = DictType(AnyType())))
    def deleteByID(self, id):
        """Delete a model
        Parameters:
            id                                  The id
        Returns:
            - The deleted model
        Errors:
            - ModelNotFoundError will be raised if the model not found
        NOTE:
            In order to get the removed model, the document is fetched before removed
            If you could ignore the returned model, please set config:
                fastRemove = True
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.deletes', params = dict(ids = ListType(StringType()), configs = DictType(AnyType())))
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
        raise FeatureNotSupportedError

    @feature(name = 'query.deletes', params = dict(condition = ConditionType(), configs = DictType(AnyType())))
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
        raise FeatureNotSupportedError

    @feature(name = 'store.countall')
    def count(self):
        """Count all
        Returns:
            The number
        """
        raise FeatureNotSupportedError

    @feature(name = 'store.count', params = dict(ids = ListType(StringType())))
    def countByID(self, ids):
        """Count the model numbers by id
        Parameters:
            ids                             A list of id
        Returns:
            The number
        """
        raise FeatureNotSupportedError

    @feature(name = 'query.count', params = dict(condition = ConditionType()))
    def countByQuery(self, condition):
        """Count the model numbers by condition
        Parameters:
            condition                       The condition
        Returns:
            The number
        """
        raise FeatureNotSupportedError

    @classmethod
    def isImplement(cls, name):
        """Check if the feature is implemented
        """
        if not name in cls.FEATURE_METHODS:
            raise ValueError('Unknown feature name [%s]' % name)
        return getattr(cls, cls.FEATURE_METHODS[name]).isImplemented

FEATURE_METHODS = {
    'store.exist'           : 'existByID',
    'store.exists'          : 'existsByID',
    'query.exists'          : 'existsByQuery',
    'store.get'             : 'getByID',
    'store.gets'            : 'getsByID',
    'store.getall'          : 'gets',
    'query.gets'            : 'getsByQuery',
    'store.create'          : 'create',
    'store.replace'         : 'replace',
    'store.update'          : 'updateByID',
    'store.updates'         : 'updatesByID',
    'query.updates'         : 'updatesByQuery',
    'store.delete'          : 'deleteByID',
    'store.deletes'         : 'deletesByID',
    'query.deletes'         : 'deletesByQuery',
    'store.count'           : 'countByID',
    'query.count'           : 'countByQuery'
}
