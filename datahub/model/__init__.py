# encoding=utf8

""" The data model
    Author: lipixun
    Created Time : 六  3/12 23:26:03 2016

    File Name: __init__.py
    Description:

"""

from uuid import uuid4

from spec import *
from models import DataModel
from _types import DataType, StringType, IntegerType, FloatType, BooleanType, DatetimeType, DateType, TimeType, TimeDeltaType, \
    ListType, SetType, DictType, ModelType, DynamicModelType, AnyType

class IDDataModel(DataModel):
    """The data model with _id and id pre-defined
    """
    _id = StringType(required = True, default = lambda: str(uuid4()), doc = 'The identity string')

    @property
    def id(self):
        """Get model id
        """
        return self._id

    @id.setter
    def id(self, value):
        """Set the model id
        """
        self._id = value

def metadata(**kwargs):
    """The decorate method to set metadata to data model
    """
    def decorator(cls):
        """The decorator class
        """
        cls.setMetadata(ModelMetadata(**kwargs))
        return cls
    return decorator
