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

from spec import *
from model import DataModel, ModelType, IntegerType, ListType
from errors import FeatureNotSupportedError

class Repository(object):
    """The repository interface
    """
    def __init__(self, cls, sorts = None):
        """The model type
        """
        # Check cls, must be a sub class of DataModel
        if not issubclass(cls, DataModel):
            raise TypeError('cls must be a subclass of DataModel')
        # Set attributes
        self.cls = cls
        self.sorts = sorts

    def exist(self, id = None, configs = None):
        """Exist
        Parameters:
            id                              The id or a list / tuple of id or None
        Returns:
            True / False
        """
        raise FeatureNotSupportedError(FEATURE_STORE_EXIST)

    def existByQuery(self, query, configs = None):
        """Exist by query
        Parameters:
            query                           The condition
        Returns:
            True / False
        """
        raise FeatureNotSupportedError(FEATURE_QUERY_EXIST)

    def getOne(self, id, configs = None):
        """Get one by id
        Returns:
            Model object
        """
        raise FeatureNotSupportedError(FEATURE_STORE_GET)

    def get(self, id = None, start = 0, size = 0, sorts = None, configs = None):
        """Get
        Parameters:
            id                              The id or list / tuple of id
        Returns:
            Yield of model object
        """
        raise FeatureNotSupportedError(FEATURE_STORE_GET)

    def getByQuery(self, query, sorts = None, start = 0, size = 0, configs = None):
        """Gets by query
        Parameters:
            query                           The condition
        Returns:
            Yield of model
        """
        raise FeatureNotSupportedError(FEATURE_QUERY_GET)

    def create(self, model, configs = None):
        """Create a new model
        Parameters:
            model                           The model object
            configs                         A dict of configs
        Returns:
            The model object which is created
        Configs:
            overwrite                       Overwrite the model if exists, false by default
        """
        raise FeatureNotSupportedError(FEATURE_STORE_CREATE)

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
        raise FeatureNotSupportedError(FEATURE_STORE_REPLACE)

    def update(self, id, updates, configs = None):
        """Update model
        Parameters:
            id                              The model id or a list / tuple of ids
            updates                         A list of UpdateAction
            configs                         A dict of configs
        Returns:
            The count of matched models
        """
        raise FeatureNotSupportedError(FEATURE_STORE_UPDATE)

    def updatesByQuery(self, query, updates, configs = None):
        """Update a couple of models by query
        Parameters:
            query                           The condition
            updates                         A list of UpdateAction
            configs                         A dict of configs
        Returns:
            The count of matched models
        """
        raise FeatureNotSupportedError(FEATURE_QUERY_UPDATE)

    def delete(self, id, configs = None):
        """Delete model
        Parameters:
            id                              The id, a single id or a list / tuple of id
            configs                         A dict of configs
        Returns:
            The count of deleted models
        """
        raise FeatureNotSupportedError(FEATURE_STORE_DELETE)

    def deleteByQuery(self, query, configs = None):
        """Delete a couple of models by query
        Parameters:
            query                           The condition
            configs                         A dict of configs
        Returns:
            The count of deleted models
        """
        raise FeatureNotSupportedError(FEATURE_QUERY_DELETE)

    def count(self, id = None, configs = None):
        """Count models
        Parameters:
            id                              The id or a list / tuple of id or None
            configs                         A dict of configs
        Returns:
            The count of the counting models
        """
        raise FeatureNotSupportedError(FEATURE_STORE_COUNT)

    def countByQuery(self, query, configs = None):
        """Count the model numbers by condition
        Parameters:
            condition                       The condition
            configs                         A dict of configs
        Returns:
            The number
        """
        raise FeatureNotSupportedError(FEATURE_QUERY_COUNT)

    def watch(self, query = None, configs = None):
        """Watch the models
        Parameters:
            query                           The query
        Returns:
            (A list of models, the watcher object)
        """
        raise FeatureNotSupportedError(FEATURE_WATCH)

    def support(self, name):
        """Check if the feature is supported
        """
        return False
