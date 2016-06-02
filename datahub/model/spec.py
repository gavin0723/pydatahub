# encoding=utf8

""" The spec
    Author: lipixun
    Created Time : 一  3/14 23:08:34 2016

    File Name: spec.py
    Description:

"""

from sets import Set
from collections import namedtuple

FILEDS_NAME         = '_datahub_datamodel_fields'
METADATA_NAME       = '_datahub_datamodel_metadata'
STORE_NAME          = '_datahub_datamodel_store'

UNKNOWN_FIELD_IGNORE    = 'ignore'
UNKNOWN_FIELD_ERROR     = 'error'

class NullValue(object):
    """A null value
    """
    def __eq__(self, other):
        """==
        """
        return isinstance(other, NullValue)

    def __ne__(self, other):
        """!=
        """
        return not isinstance(other, NullValue)

nullValue = NullValue()

class ModelMetadata(object):
    """The model metadata
    Attributes:
        namespace                           The model namespace
        strict                              Whether the model is strict
        attrs                               The metadata attributes
        none                                Return None if the field is not assigned
    """
    def __init__(self, namespace = None, strict = False, attrs = None, none = True):
        """Create a new ModelMetadata
        """
        self.none = none
        self.attrs = attrs or []
        self.strict = strict
        self.namespace = namespace

    def getAttrs(self, name):
        """Get attributes by name
        """
        return filter(lambda x: x.NAME == name, self.attrs)

    @staticmethod
    def getDefault():
        """Get default metadata
        """
        global DEFAULT_MODEL_METADATA
        return DEFAULT_MODEL_METADATA

    @staticmethod
    def setDefault(metadata):
        """Set default metadata
        """
        global DEFAULT_MODEL_METADATA
        DEFAULT_MODEL_METADATA = metadata

# This is the default model metadata which will be used by any models that doesnt' define a metadata
DEFAULT_MODEL_METADATA = ModelMetadata()

class DumpContext(object):
    """The dump context
    """
    def __init__(self, dumpNone = False, datetime2str = True, datetimeFormat = None, date2str = True, dateFormat = None, time2str = True, timeFormat = None):
        """Create a new DumpContext
        """
        self.dumpNone = dumpNone
        self.datetime2str = datetime2str
        self.datetimeFormat = datetimeFormat
        self.date2str = date2str
        self.dateFormat = dateFormat
        self.time2str = time2str
        self.timeFormat = timeFormat

    @staticmethod
    def getDefault():
        """Get default metadata
        """
        global DEFAULT_DUMP_CONTEXT
        return DEFAULT_DUMP_CONTEXT

    @staticmethod
    def setDefault(context):
        """Set default dump context
        """
        global DEFAULT_DUMP_CONTEXT
        DEFAULT_DUMP_CONTEXT = context

DEFAULT_DUMP_CONTEXT = DumpContext()

# The metadata attributes

class IndexAttr(object):
    """The index attribute
    """
    NAME = 'index'

    def __init__(self, keys, unique = False, sparse = False):
        """Create a new ModelIndex
        """
        self.keys = keys
        self.unique = unique
        self.sparse = sparse

class ExpireAttr(object):
    """The expire attr
    """
    NAME = 'expire'

    def __init__(self, key, expires = None):
        """Create a new ExpireAttr
        Parameters:
            key                                 The key
            expires                             The expire seconds, the model will be expired in this time period
        """
        self.key = key
        self.expires = expires or 0
