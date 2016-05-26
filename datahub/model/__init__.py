# encoding=utf8

""" The data model
    Author: lipixun
    Created Time : 六  3/12 23:26:03 2016

    File Name: __init__.py
    Description:

"""

from spec import nullValue, ModelMetadata, ModelIndex, DumpContext
from models import DataModel, IDDataModel
from _types import DataType, StringType, IntegerType, FloatType, BooleanType, DatetimeType, DateType, TimeType, TimeDeltaType, \
    ListType, SetType, DictType, ModelType, DynamicModelType, AnyType
from resource import ResourceMetadata, Resource, ResourceWatchChangeSet

def metadata(**kwargs):
    """The decorate method to set metadata to data model
    """
    def decorator(cls):
        """The decorator class
        """
        cls.setMetadata(ModelMetadata(**kwargs))
        return cls
    return decorator

__all__ = [
    'nullValue', 'ModelMetadata', 'ModelIndex', 'DumpContext',
    'DataModel', 'IDDataModel',
    'DataType', 'StringType', 'IntegerType', 'FloatType', 'BooleanType', 'DatetimeType', 'DateType', 'TimeType', 'TimeDeltaType',
    'ListType', 'SetType', 'DictType', 'ModelType', 'DynamicModelType', 'AnyType',
    'ResourceMetadata', 'Resource', 'ResourceWatchChangeSet',
    'metadata'
    ]
