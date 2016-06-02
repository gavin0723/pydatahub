# encoding=utf8

""" The data models
    Author: lipixun
    Created Time : 六  3/12 23:26:11 2016

    File Name: models.py
    Description:

"""

import os

from uuid import uuid4
from binascii import b2a_hex

from datahub.errors import DataModelError, CompoundDataModelError, NestedDataModelError, UnknownFieldError, \
    MissingRequiredFieldError, FieldNotDumpError, QueryNotMatchError

from spec import *
from _types import DataType, StringType, FloatType, DatetimeType, DictType, ModelType

class DataModelMetaClass(type):
    """The data model meta class
    This class will set the following meta attributes of data model:
        _datahub_datamodel_fields           The field definitions
        _datahub_datamodel_metadata         The metadata definition
        _datahub_datamodel_store            The stored values
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

    def __init__(self, __raw__ = None, __continueOnError__ = False, **kwargs):
        """Create a new DataModel
        """
        container = __raw__ or {}
        container.update(kwargs)
        # Initialize the stores
        setattr(self, STORE_NAME, {})
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Get metadata
        metadata = getattr(type(self), METADATA_NAME) if hasattr(type(self), METADATA_NAME) else ModelMetadata.getDefault()
        # All errors
        errors = []
        # Load values
        for key, value in container.iteritems():
            if not key in fields:
                # Key not found
                error = UnknownFieldError(key)
                if not __continueOnError__:
                    raise error
                # Add error
                errors.append(error)
            else:
                # Load the value
                try:
                    self.__setvalue__(key, fields[key].load(value, self, container))
                except Exception as error:
                    # Set error
                    if not __continueOnError__:
                        raise
                    # Add error
                    errors.append(error)
        # Set default
        for name, field in fields.iteritems():
            if not self.__existvalue__(name) and field.hasDefault():
                try:
                    self.__setvalue__(name, field.load(field.getDefault(), self, None))
                except Exception as error:
                    # Set error
                    if not __continueOnError__:
                        raise error
                    # Add error
                    errors.append(error)
        # Check required
        if metadata.strict:
            try:
                self._validateRequiredFields(fields, metadata)
            except Exception as error:
                # Set error
                if not __continueOnError__:
                    raise error
                # Add error
                errors.append(error)
        # Check error
        if errors:
            if len(errors) == 1:
                raise errors[0]
            else:
                raise CompoundDataModelError(errors, 'Failed to initialize model (%s)' % type(self).__name__)

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

    def __getitem__(self, key):
        """Get item
        """
        if not key in getattr(self, STORE_NAME):
            raise KeyError(key)
        # Get
        return getattr(self, key)

    def __setitem__(self, key, value):
        """Set item
        """
        if not key in getattr(self, STORE_NAME):
            raise KeyError(key)
        # Set
        setattr(self, key, value)

    def __delitem__(self, key):
        """Delete item
        """
        if not key in getattr(self, STORE_NAME):
            raise KeyError(key)
        # Delete
        delattr(self, key)

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

    def _validateRequiredFields(self, fields = None, metadata = None):
        """Validate the required fields
        """
        # Get all fields
        fields = fields or getattr(type(self), FILEDS_NAME)
        # Get metadata
        metadata = metadata or (getattr(type(self), METADATA_NAME) if hasattr(type(self), METADATA_NAME) else ModelMetadata.getDefault())
        # Check required
        for name, field in fields.iteritems():
            if field.required and (not self.__existvalue__(name) or field.isEmpty(self.__getvalue__(name))):
                raise MissingRequiredFieldError(name)

    def validate(self, attr = True, required = True, continueOnError = False):
        """Validate this model
        Parameters:
            attr                            Validate the attribute or not
            required                        Validate the required constraint or not
            continueOnError                 Continue validation on error
        Returns:
            Nothing
        """
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Get metadata
        metadata = getattr(type(self), METADATA_NAME) if hasattr(type(self), METADATA_NAME) else ModelMetadata.getDefault()
        # Validation errors
        errors = []
        # Check required
        try:
            if required:
                self._validateRequiredFields(fields, metadata)
        except Exception as error:
            # Check error
            if not continueOnError:
                raise
            # Found error
            errors.append(error)
        # Check the fields
        if attr:
            for name, field in fields.iteritems():
                if self.__existvalue__(name):
                    try:
                        field.validate(self.__getvalue__(name), required, continueOnError)
                    except Exception as error:
                        # Check error
                        if not continueOnError:
                            raise
                        # Found error
                        errors.append(NestedDataModelError(name, error))
        # Check errors
        if errors:
            if len(errors) == 1:
                raise errors[0]
            else:
                raise CompoundDataModelError(errors, 'Failed to validate model (%s)' % type(self).__name__)

    def queryMySelf(self, path):
        """Query myself
        Returns:
            (type, name, value, NextPath)
        Error:
            QueryNotMatchError
        """
        # Parse the path
        index = path.find('.')
        if index == -1:
            name = path
            nextPath = None
        else:
            name = path[: index]
            nextPath = path[index + 1: ]
        # Get the fields
        fields = getattr(type(self), FILEDS_NAME)
        if not name in fields:
            raise QueryNotMatchError(name, nextPath)
        t = fields[name]
        # Get the value
        if not self.__existvalue__(name):
            raise QueryNotMatchError(name, nextPath, t)
        # Return
        return t, name, self.__getvalue__(name), nextPath

    def query(self, path):
        """Query value by path
        Parameters:
            path                    A string path
        Returns:
            value
        """
        try:
            t, name, value, nextPath = self.queryMySelf(path)
            if not nextPath:
                yield value
            else:
                for v in t.query(value, nextPath):
                    yield v
        except QueryNotMatchError:
            pass

    def match(self, query):
        """Check if this model match the query
        Parameters:
            query                   The condition object
        Returns:
            True / False
        """
        return query.check(self)

    def dump(self, context = None):
        """Dump this model
        """
        context = context or DEFAULT_DUMP_CONTEXT
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        # Validate the required fields and validate the field
        container = {}
        for name, field in fields.iteritems():
            if self.__existvalue__(name):
                try:
                    value = field.dump(self.__getvalue__(name), self, container, context)
                except FieldNotDumpError:
                    continue
                # Set value
                container[name] = value
        # Done
        return container

    def clone(self):
        """Clone this data model
        """
        # Get all fields
        fields = getattr(type(self), FILEDS_NAME)
        clonedFields = {}
        for k, t in fields.iteritems():
            if self.__existvalue__(k):
                clonedFields[k] = t.clone(self.__getvalue__(k))
        # Create new one
        return type(self)(clonedFields)

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

    @classmethod
    def load(cls, raw, continueOnError = False):
        """Load from raw object
        """
        return cls(raw, continueOnError)

def randomID(length = 32):
    """Get a random id
    """
    if length < 16:
        raise ValueError('Length cannot be smaller than 16')
    return b2a_hex(os.urandom(length - 16) + uuid4().bytes)

class IDDataModel(DataModel):
    """The data model with _id and id pre-defined
    """
    _id = StringType(required = True, default = randomID, doc = 'The identity string')

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
