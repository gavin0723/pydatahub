# encoding=utf8

""" The data model
    Author: lipixun
    Created Time : 六  3/12 23:26:03 2016

    File Name: __init__.py
    Description:

"""

from spec import nullValue, ModelMetadata, DumpContext, IndexAttr, ExpireAttr
from models import DataModel, IDDataModel
from _types import DataType, StringType, IntegerType, FloatType, BooleanType, DatetimeType, DateType, TimeType, TimeDeltaType, \
    ListType, SetType, DictType, ModelType, DynamicModelType, AnyType

def metadata(**kwargs):
    """The decorate method to set metadata to data model
    """
    def decorator(cls):
        """The decorator class
        """
        cls.setMetadata(ModelMetadata(**kwargs))
        return cls
    return decorator

def metaattr(attr):
    """The decorator method to set metadata attribute to data model
    """
    def decorator(cls):
        """The decorator class
        """
        metadata = cls.getMetadata()
        if not metadata:
            raise ValueError('Metadata not found')
        metadata.attrs.append(attr)
        return cls
    return decorator

__all__ = [
    'nullValue', 'ModelMetadata', 'DumpContext', 'IndexAttr', 'ExpireAttr',
    'DataModel', 'IDDataModel',
    'DataType', 'StringType', 'IntegerType', 'FloatType', 'BooleanType', 'DatetimeType', 'DateType', 'TimeType', 'TimeDeltaType',
    'ListType', 'SetType', 'DictType', 'ModelType', 'DynamicModelType', 'AnyType',
    'metadata', 'metaattr',
    ]
