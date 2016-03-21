# encoding=utf8

""" The data models
    Author: lipixun
    Created Time : 六  3/12 23:26:11 2016

    File Name: models.py
    Description:

"""

from datahub.errors import DataModelError, NestedDataModelError, UnknownFieldError, MissingRequiredFieldError

from spec import *
from _types import DataType

class DataModelMetaClass(type):
    """The data model meta class
    This class will set the following meta attributes of data model:
        _datahub_datamodel_attrs            The attributes
    """
    def __new__(cls, name, bases, attrs):
        """Create a new DataModel object
        """
        fields = {}
        for base in bases:
            if hasattr(base, FILEDS_NAME):
                fields.update(getattr(base, FILEDS_NAME))
        for key, field in attrs.iteritems():
            if isinstance(field, DataType):
                # The name of the field to the key name (Aka. the attribute name) if not specified
                if not field.name:
                    field.name = key
                fields[key] = field
        # Add fields to attrs
        attrs[FILEDS_NAME] = fields
        # Super
        return type.__new__(cls, name, bases, attrs)

class DataModel(object):
    """The data model base class
    """
    __metaclass__ = DataModelMetaClass

    def __init__(self, raw = None, **kwargs):
        """Create a new DataModel
        """
        rawValues = raw or {}
        rawValues.update(kwargs)
        # Initialize the stores
        setattr(self, STORE_NAME, {})
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Get metadata
        metadata = getattr(type(self), METADATA_NAME) if hasattr(type(self), METADATA_NAME) else ModelMetadata.getDefault()
        # Load values
        nestedError = None
        for key, value in rawValues.iteritems():
            if not key in fields:
                # Key not found
                if metadata.unknownField == UNKNOWN_FIELD_IGNORE:
                    continue
                else:
                    # Set error
                    if not nestedError:
                        nestedError = NestedDataModelError({}, rawValues)
                    nestedError.errors[key] = UnknownFieldError(key, rawValues)
                    # Check continue or not
                    if not metadata.continueOnError:
                        break
            else:
                # Load the value
                try:
                    self.__setvalue__(key, fields[key].load(value, LoadContext(key, rawValues, self), ValidateContext(metadata.continueOnError)))
                except Exception as error:
                    # Set error
                    if not nestedError:
                        nestedError = NestedDataModelError({}, rawValues)
                    nestedError.errors[key] = error
                    if not metadata.continueOnError:
                        break
        # Check error
        if nestedError:
            raise nestedError
        # Check required
        if metadata.strict:
            self.validateRequiredFields()
        if metadata.autoInitialize:
            self.initialize()

    def __existvalue__(self, key):
        """Get if exist a raw value
        """
        return key in getattr(self, STORE_NAME)

    def __getvalue__(self, key):
        """Get the raw value
        """
        return getattr(self, STORE_NAME)[key]

    def __setvalue__(self, key, value):
        """Set the raw value
        """
        getattr(self, STORE_NAME)[key] = value

    def __eq__(self, that):
        """Equals
        """
        # Compare type
        if type(self) != type(that):
            return False
        # Compare fields
        fields = getattr(type(self), FILEDS_NAME)
        for name, field in fields.iteritems():
            found1, found2, v1, v2 = False, False, None, None
            if self.__existvalue__(name):
                v1 = self.__getvalue__(name)
                if not field.isEmpty(v1):
                    found1 = True
            if that.__existvalue__(name):
                v2 = that.__getvalue__(name)
                if not field.isEmpty(v2):
                    found2 = True
            if found1 != found2:
                return False
            if found1 == True and not field.equals(v1, v2):
                return False
        # Done
        return True

    def __ne__(self, that):
        """Not equals
        """
        return not self.__eq__(that)

    def initialize(self):
        """Initialize all fields
        """
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Get metadata
        metadata = getattr(type(self), METADATA_NAME) if hasattr(type(self), METADATA_NAME) else ModelMetadata.getDefault()
        # Check all fields which has a default value but not assigned by any data
        for name, field in fields.iteritems():
            if not self.__existvalue__(name) and field.hasDefault():
                self.__setvalue__(name, field.load(field.getDefault(), LoadContext(name, None, self), ValidateContext(metadata.continueOnError)))

    def query(self, path):
        """Query value by path
        Parameters:
            path                    A string path
        Returns:
            Yield of found value
        """
        index = path.find('.')
        if index == -1:
            name = path
            nextPath = None
        else:
            name = path[: index]
            nextPath = path[index + 1: ]
        # Get the attribute
        try:
            value = getattr(self, name)
        except AttributeError:
            # Not found
            return
        # Yield return
        if not nextPath:
            yield value
        else:
            # Get the type
            try:
                _type = getattr(type(self), name)
            except AttributeError:
                # Type not found, may not be a model type
                return
            # Continue quering the value via type
            for v in _type.query(value, nextPath):
                yield v

    def match(self, condition):
        """Check if this model match the condition
        Parameters:
            condition               The condition object
        Returns:
            True / False
        """
        return condition.check(self)

    def update(self, updates):
        """Update this model by the update actions
        """
        raise NotImplementedError

    def dump(self, context = None):
        """Dump this model
        """
        if not context:
            context = DEFAULT_DUMP_CONTEXT
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Validate the required fields and validate the field
        rawValues = {}
        for name, field in fields.iteritems():
            if self.__existvalue__(name):
                value = field.dump(self.__getvalue__(name), context)
                if not field.isEmpty(value) or field.dumpEmpty:
                    rawValues[name] = value
            elif field.dumpEmpty:
                rawValues[name] = field.emptyDump()
        # Done
        return rawValues

    def clone(self):
        """Clone this data model
        """
        raise NotImplementedError

    def validateRequiredFields(self, fixByDefault = True):
        """Validate the required fields
        """
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Get metadata
        metadata = getattr(type(self), METADATA_NAME) if hasattr(type(self), METADATA_NAME) else ModelMetadata.getDefault()
        # Check required
        for name, field in fields.iteritems():
            if field.required and (not self.__existvalue__(name) or field.isEmpty(self.__getvalue__(name))):
                # Check default
                if fixByDefault and field.hasDefault():
                    self.__setvalue__(name, field.load(field.getDefault(), LoadContext(name, None, self), ValidateContext(metadata.continueOnError)))
                else:
                    raise MissingRequiredFieldError(name)

    def validate(self, context = None):
        """Validate the model
        """
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Get metadata
        metadata = getattr(type(self), METADATA_NAME) if hasattr(type(self), METADATA_NAME) else ModelMetadata.getDefault()
        # Check required
        self.validateRequiredFields()
        # Validate the fields
        if not context:
            context = ValidateContext(metadata.continueOnError)
        for name, field in fields.iteritems():
            if self.__existvalue__(name):
                value = self.__getvalue__(name)
                if not field.isEmpty(value):
                    field.validate(value, context)

    @classmethod
    def getMetadata(cls):
        """Get meta
        """
        if hasattr(cls, METADATA_NAME):
            return getattr(cls, METADATA_NAME)

    @classmethod
    def setMetadata(cls, metadata):
        """Set metadata
        """
        setattr(cls, METADATA_NAME, metadata)
