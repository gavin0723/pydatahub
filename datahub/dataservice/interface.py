# encoding=utf8

""" The interface of data service
    Author: lipixun
    Created Time : 二  8/16 11:03:59 2016

    File Name: interface.py
    Description:

"""

from datahub.errors import FeatureNotSupportedError

class DataServiceInterface(object):
    """The data service interface
    """
    def exist(self, id, **ctx):
        """Check if a model with id exists
        Returns:
            True / False
        """
        raise FeatureNotSupportedError

    def getOne(self, id, **ctx):
        """Get one model
        Returns:
            Model object or None
        """
        raise FeatureNotSupportedError

    def gets(self, ids = None, start = 0, size = 0, sorts = None, **ctx):
        """Get models
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        raise FeatureNotSupportedError

    def getByQuery(self, query, start = 0, size = 0, sorts = None, **ctx):
        """Get by query
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        raise FeatureNotSupportedError

    def create(self, model, overwrite = False, **ctx):
        """Create a model
        Returns:
            The model id
        """
        raise FeatureNotSupportedError

    def replace(self, model, autoCreate = False, **ctx):
        """Replace a model
        Returns:
            The model id
        """
        raise FeatureNotSupportedError

    def updateOne(self, id, updates, **ctx):
        """Update a model
        Returns:
            True / False
        """
        raise FeatureNotSupportedError

    def updates(self, ids, updates, **ctx):
        """Update models
        Returns:
            The number of models that is updated
        """
        raise FeatureNotSupportedError

    def updateByQuery(self, query, updates, **ctx):
        """Update by query
        Returns:
            The number of models that is updated
        """
        raise FeatureNotSupportedError

    def deleteOne(self, id, **ctx):
        """Delete a model
        Returns:
            True / False
        """
        raise FeatureNotSupportedError

    def deletes(self, ids, **ctx):
        """Delete models
        Returns:
            The number of models that is deleted
        """
        raise FeatureNotSupportedError

    def deleteByQuery(self, query, **ctx):
        """Delete by query
        Returns:
            The number of models that is deleted
        """
        raise FeatureNotSupportedError

    def counts(self, ids, **ctx):
        """Count by ids
        Returns:
            The number of found models
        """
        raise FeatureNotSupportedError

    def countByQuery(self, query, **ctx):
        """Count by query
        Returns:
            The number of found models
        """
        raise FeatureNotSupportedError
