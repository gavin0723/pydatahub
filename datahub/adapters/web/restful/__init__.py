# encoding=utf8

""" The restful web adapter
    Author: lipixun
    Created Time : 二  3/15 22:52:42 2016

    File Name: __init__.py
    Description:

"""

from resource import ResourceService, KeyValueAttributeParameterMapper, ResourceLocation
from connector import Connection, ResourceConnector

__all__ = [ 'ResourceService', 'KeyValueAttributeParameterMapper', 'ResourceLocation', 'Connection', 'ResourceConnector' ]
