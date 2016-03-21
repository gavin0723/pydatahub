# encoding=utf8

""" The spec
    Author: lipixun
    Created Time : 一  3/14 23:08:34 2016

    File Name: spec.py
    Description:

"""

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
        return False

    def __ne__(self, other):
        """!=
        """
        return True

nullValue = NullValue()

class ModelMetadata(object):
    """The model metadata

    :param strict:
        If the data model is strict or not. Model will be validated after each change of the model (including the initialization).
    :param autoInitialize:
        The model will assign all default values to the fields which haven't given any data when creating the model if true.
    """
    def __init__(self, namespace = None, strict = False, none4Unassigned = True, unknownField = None, continueOnError = True, autoInitialize = True, indices = None):
        """Create a new ModelMetadata
        """
        self.namespace = namespace
        self.strict = strict
        self.none4Unassigned = none4Unassigned
        self.unknownField = unknownField or UNKNOWN_FIELD_ERROR
        if self.unknownField != UNKNOWN_FIELD_IGNORE and self.unknownField != UNKNOWN_FIELD_ERROR:
            raise ValueError('Unknown value of parameter "unknownField"')
        self.continueOnError = continueOnError
        self.autoInitialize = autoInitialize
        self.indices = indices

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

class ModelIndex(object):
    """The model index
    """
    def __init__(self, keys, unique = False, sparse = False):
        """Create a new ModelIndex
        """
        self.keys = keys
        self.unique = unique
        self.sparse = sparse

LoadContext = namedtuple('LoadContext', 'key,raw,model')

EMPTY_LOAD_CONTEXT = LoadContext(None, None, None)

ValidateContext = namedtuple('ValidateContext', 'continueOnError')

DEFAULT_VALIDATE_CONTEXT = ValidateContext(False)

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
