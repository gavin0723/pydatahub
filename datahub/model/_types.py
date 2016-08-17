# encoding=utf8

""" The data types
    Author: lipixun
    Created Time : 六  3/12 23:26:23 2016

    File Name: _types.py
    Description:

"""

from sets import Set
from copy import deepcopy
from datetime import datetime, date, time, timedelta

import arrow

from datahub.errors import FieldNotDumpError, DataModelError, CompoundDataModelError, NestedDataModelError, \
    MissingRequiredFieldError, TypeValidationError, ValueConversionError, ChoiceValidationError, UnqueryableValueError, QueryNotMatchError

from spec import *

class DataType(object):
    """The base data type
    """
    def __init__(self,
        name = None,
        required = False,
        default = nullValue,
        choices = None,
        doc = None,
        # The method parameters to overwrite the default one
        loader = None,
        dumper = None,
        validator = None,
        # The control options
        dumpWhenEmpty = False,
        ):
        """Create a new DataType
        The methods:
            - loader                The method: (t, value, model, container)
            - dumper                The method: (t, value, model, container)
            - validator             The method: (t, value, required, continueOnError)
        """
        self.name = name
        self.required = required
        self.default = default
        self.choices = choices
        self.doc = doc
        # The methods
        self._loader = loader
        self._dumper = dumper
        self._validator = validator
        # THe control options
        self.dumpWhenEmpty = dumpWhenEmpty

    def __loadvalue__(self, value, model, container):
        """Load the value (The default loader method)
        Parameters:
            value                   The value of this field (type)
            model                   The model of this field (type)
            container               The container of the value (AKA, the raw data for the model)
        Returns:
            The loaded value
        """
        return value

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        Parameters:
            value                   The value of this field (type)
            model                   The model of this field (type)
            container               The container of the value (AKA, the raw data current dumped)
            context                 The DumpContext object
        Returns:
            The dumped value
        """
        return value

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        pass

    def __get__(self, instance, cls):
        """Get the value of this data type
        """
        if instance:
            # Get the value
            if not self.exists(instance):
                metadata = instance.getMetadata() or DEFAULT_MODEL_METADATA
                if metadata.none:
                    return None
                else:
                    raise AttributeError
            # Return
            return getattr(instance, STORE_NAME)[self.name]
        else:
            # Get the type
            return self

    def __set__(self, instance, value):
        """Set the value of this data type
        """
        metadata = instance.getMetadata() or DEFAULT_MODEL_METADATA
        # Load the value
        value = self.load(value, instance, None)
        # Validate or not
        if metadata.strict:
            self.validate(value)
        # Good, set the value
        getattr(instance, STORE_NAME)[self.name] = value

    def __delete__(self, instance):
        """Delete the value of this type
        """
        metadata = instance.getMetadata() or DEFAULT_MODEL_METADATA
        if metadata.strict and self.required:
            raise MissingRequiredFieldError(self.name)
        # Pop the value
        getattr(instance, STORE_NAME).pop(self.name, None)

    def exists(self, model):
        """Check if the value is exists
        """
        return self.name in getattr(model, STORE_NAME)

    def hasDefault(self):
        """Get if this type has a default value
        """
        return not isinstance(self.default, NullValue)

    def getDefault(self):
        """Get the default value
        """
        if not self.hasDefault():
            raise ValueError('No default value')
        return self.default() if callable(self.default) else self.default

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None

    def load(self, value, model, container):
        """Load the value of this type
        """
        # Load the value
        if self._loader:
            value = self._loader(self, value, model, container)
        else:
            value = self.__loadvalue__(value, model, container)
        # Validate the loaded value
        self.validate(value, required = False)
        # Done
        return value

    def dump(self, value, model, container, context):
        """Dump this type of the model
        """
        # Check empty
        if not self.dumpWhenEmpty and self.isEmpty(value):
            raise FieldNotDumpError
        # Dump the value
        if self._dumper:
            return self._dumper(self, value, model, container, context)
        else:
            return self.__dumpvalue__(value, model, container, context)

    def validate(self, value, required = True, continueOnError = False):
        """Validate this type
        """
        errors = []
        # Validate choices
        if not self.isEmpty(value) and self.choices and not value in self.choices:
            error = ChoiceValidationError(value, self.choices)
            if not continueOnError:
                raise error
            # Add error
            errors.append(error)
        # Run validator
        try:
            if self._validator:
                self._validator(self, value, required, continueOnError)
            else:
                self.__validatevalue__(value, required, continueOnError)
        except Exception as error:
            if not continueOnError:
                raise
            errors.append(error)
        # Check error
        if errors:
            if len(errors) == 1:
                raise errors[0]
            else:
                raise CompoundDataModelError(errors)

    def query(self, value, path, allowNotFound = False):
        """Query the value by path
        Returns:
            Yield the value
        """
        # No value by default
        return
        yield

    def equals(self, v1, v2):
        """Compare value equals
        """
        return v1 == v2

    def clone(self, value):
        """Clone the value
        """
        return value

    def loader(self, method):
        """Set the loader method
        """
        self._loader = method

    def dumper(self, method):
        """Set the dump method
        """
        self._dumper = method

    def validator(self, method):
        """Set the validation method
        """
        self._validator = method

class StringType(DataType):
    """The string type
    """
    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, basestring):
            raise TypeValidationError(basestring, type(value), value)
        # Super
        super(StringType, self).__validatevalue__(value, required, continueOnError)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or value == ''

class IntegerType(DataType):
    """The integer type
    """
    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, (int, long)):
            return value
        elif isinstance(value, basestring) and value.isdigit():
            return long(value)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), (int, long), value)

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, (int, long)):
            raise TypeValidationError((int, long), type(value), value)
        # Super
        super(IntegerType, self).__validatevalue__(value, required, continueOnError)

class FloatType(DataType):
    """The float type
    """
    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, float):
            return value
        elif isinstance(value, (basestring, int, long)):
            try:
                return float(value)
            except:
                raise ValueConversionError(type(value), float, value)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), float, value)

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, float):
            raise TypeValidationError(float, type(value), value)
        # Super
        super(FloatType, self).__validatevalue__(value, required, continueOnError)

class BooleanType(DataType):
    """The boolean type
    """
    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, bool):
            return value
        elif isinstance(value, basestring):
            value = value.lower().strip()
            if value == '1' or value == 'true' or value == 'yes' or value == 'on':
                return True
            elif value == '0' or value == 'false' or value == 'no' or value == 'off':
                return False
            else:
                raise ValueConversionError(type(value), bool, value)
        elif isinstance(value, (int, long)):
            if value == 1:
                return True
            elif value == 0:
                return False
            else:
                raise ValueConversionError(type(value), bool, value)
        elif isinstance(value, float):
            if value >= 0.9999999 and value <= 1.0000001:
                return True
            elif value >= -0.00000001 and value <= 0.00000001:
                return False
            else:
                raise ValueConversionError(type(value), bool, value)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), bool, value)

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, bool):
            raise TypeValidationError(bool, type(value), value)
        # Super
        super(BooleanType, self).__validatevalue__(value, required, continueOnError)

class DatetimeType(DataType):
    """The datetime type
    """
    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return datetime(date.year, date.month, date.day, 0, 0, 0)
        elif isinstance(value, basestring):
            dt = arrow.get(value).datetime
            # NOTE: Here, we just remove the tzinfo
            return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)
        elif isinstance(value, (int, long, float)):
            return datetime.fromtimestamp(int(value))
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), datetime, value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        if context.datetime2str:
            # Convert time to string
            if context.datetimeFormat:
                return value.strftime(context.datetimeFormat)
            else:
                return value.isoformat()
        else:
            # Just return
            return value

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, datetime):
            raise TypeValidationError(datetime, type(value), value)
        # Super
        super(DatetimeType, self).__validatevalue__(value, required, continueOnError)

class DateType(DataType):
    """The date type
    """
    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, date):
            return value
        elif isinstance(value, basestring):
            return arrow.get(value).date()
        elif isinstance(value, (int, long, float)):
            return date.fromtimestamp(int(value)).date()
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), date, value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        if context.date2str:
            # Convert time to string
            if context.dateFormat:
                return value.strftime(context.dateFormat)
            else:
                return value.isoformat()
        else:
            # Just return
            return value

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, date):
            raise TypeValidationError(date, type(value), value)
        # Super
        super(DateType, self).__validatevalue__(value, required, continueOnError)

class TimeType(DataType):
    """The time type
    """
    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, time):
            return value
        elif isinstance(value, basestring):
            return datetime.strptime(value, '%H:%M:%S.%f').time()
        elif isinstance(value, (int, long, float)):
            return date.fromtimestamp(int(value)).time()
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), date, value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        if context.time2str:
            # Convert time to string
            return value.strftime(context.timeFormat or '%H:%M:%S.%f')
        else:
            # Just return
            return value

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, time):
            raise TypeValidationError(time, type(value), value)
        # Super
        super(TimeType, self).__validatevalue__(value, required, continueOnError)

class TimeDeltaType(DataType):
    """The time delta type
    """
    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, timedelta):
            return value
        elif isinstance(value, basestring):
            try:
                return timedelta(seconds = float(value))
            except:
                raise ValueConversionError(type(value), timedelta, value)
        elif isinstance(value, (int, long, float)):
            return timedelta(seconds = float(value))
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), timedelta, value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        return value.total_seconds()

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, timedelta):
            raise TypeValidationError(timedelta, type(value), value)
        # Super
        super(TimeDeltaType, self).__validatevalue__(value, required, continueOnError)

class ListType(DataType):
    """The list type
    """
    def __init__(self, itemType, *args, **kwargs):
        """Create a new ListType
        """
        self.itemType = itemType
        # Super
        super(ListType, self).__init__(*args, **kwargs)

    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, (list, tuple)):
            items = []
            # Load all items
            for i in range(0, len(value)):
                v = value[i]
                try:
                    items.append(self.itemType.load(v, model, container))
                except Exception as error:
                    raise NestedDataModelError(str(i), error)
            # Done
            return items
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), (list, tuple), value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        dumpedValue = []
        for v in value:
            try:
                dumpedValue.append(self.itemType.dump(v, model, container, context))
            except FieldNotDumpError:
                pass
        return dumpedValue

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        if not value is None and not isinstance(value, list):
            raise TypeValidationError(list, type(value), value)
        # Check item
        if not self.isEmpty(value):
            errors = []
            for i in range(0, len(value)):
                v = value[i]
                try:
                    self.itemType.validate(v, required, continueOnError)
                except Exception as error:
                    if not continueOnError:
                        raise
                    # Add error
                    errors.append(NestedDataModelError(str(i), error))
            # Check errors
            if errors:
                if len(errors) == 1:
                    raise errors[0]
                else:
                    raise CompoundDataModelError(errors)
        # Super
        super(ListType, self).__validatevalue__(value, required, continueOnError)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or len(value) == 0

    def query(self, value, path):
        """Query the value by path
        """
        if not self.isEmpty(value):
            for item in value:
                for v in self.itemType.query(item, path):
                    yield v

    def clone(self, value):
        """Clone the value
        """
        if self.isEmpty(value):
            return value
        else:
            clonedValue = []
            for item in value:
                clonedValue.append(self.itemType.clone(item))
            return clonedValue

class SetType(DataType):
    """The set type
    """
    def __init__(self, itemType, *args, **kwargs):
        """Create a new ListType
        """
        self.itemType = itemType
        # Super
        super(SetType, self).__init__(args, kwargs)

    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, (list, tuple)):
            items = []
            # Load all items
            for i in range(0, len(value)):
                v = value[i]
                try:
                    items.append(self.itemType.load(v, model, container))
                except Exception as error:
                    raise NestedDataModelError(str(i), error)
            # Done
            return Set(items)
        elif isinstance(value, Set):
            items = []
            # Load all items
            for v in value:
                try:
                    items.append(self.itemType.load(v, model, container))
                except Exception as error:
                    raise NestedDataModelError(None, error)
            # Done
            return Set(items)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), (list, tuple, Set), value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        dumpedValue = []
        for v in value:
            try:
                dumpedValue.append(self.itemType.dump(v, model, container, context))
            except FieldNotDumpError:
                pass
        return dumpedValue

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        if not value is None and not isinstance(value, Set):
            raise TypeValidationError(Set, type(value), value)
        # Check items
        if not self.isEmpty(value):
            errors = []
            for v in value:
                try:
                    self.itemType.validate(v, required, continueOnError)
                except Exception as error:
                    if not continueOnError:
                        raise
                    # Add error
                    errors.append(error)
            # Check errors
            if errors:
                if len(errors) == 1:
                    raise errors[0]
                else:
                    raise CompoundDataModelError(errors)
        # Super
        super(SetType, self).__validatevalue__(value, required, continueOnError)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or len(value) == 0

    def query(self, value, path):
        """Query the value by path
        """
        if not self.isEmpty(value):
            for item in value:
                for part in self.itemType.query(item, path):
                    yield part

    def clone(self, value):
        """Clone the value
        """
        if self.isEmpty(value):
            return value
        else:
            clonedValue = Set()
            for item in value:
                clonedValue.add(self.itemType.clone(item))
            return clonedValue

class DictType(DataType):
    """The dict type
    """
    def __init__(self, itemType, *args, **kwargs):
        """Create a new ListType
        """
        self.itemType = itemType
        # Super
        super(DictType, self).__init__(*args, **kwargs)

    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, dict):
            items = {}
            for k, v in value.iteritems():
                try:
                    if not isinstance(k, basestring):
                        raise TypeValidationError(basestring, type(k), k)
                    items[k] = self.itemType.load(v, model, container)
                except Exception as error:
                    raise NestedDataModelError(k, error)
            # Done
            return items
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), dict, value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        if not self.isEmpty(value):
            dumpedValue = {}
            for k, v in value.iteritems():
                try:
                    dumpedValue[k] = self.itemType.dump(v, model, container, context)
                except FieldNotDumpError:
                    pass
            return dumpedValue
        else:
            return value

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        if not value is None and not isinstance(value, dict):
            raise TypeValidationError(dict, type(value), value)
        # Check items
        if not self.isEmpty(value):
            errors = []
            for k, v in value.iteritems():
                try:
                    if not isinstance(k, basestring):
                        raise TypeValidationError(basestring, type(k), k)
                    self.itemType.validate(v, required, continueOnError)
                except Exception as error:
                    error = NestedDataModelError(k, error)
                    if not continueOnError:
                        raise error
                    # Add error
                    errors.append(error)
            # Check errors
            if errors:
                if len(errors) == 1:
                    raise errors[0]
                else:
                    raise CompoundDataModelError(errors)
        # Super
        super(DictType, self).__validatevalue__(value, required, continueOnError)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or len(value) == 0

    def queryMySelf(self, value, path):
        """Query myself
        Returns:
            name, value, nextPath
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
        # Get the value
        if not name in value:
            return QueryNotMatchError(name, nextPath)
        # Done
        return name, value[name], nextPath

    def query(self, value, path):
        """Query the value by path
        """
        if not self.isEmpty(value):
            # Query myself
            try:
                name, nextValue, nextPath = self.queryMySelf(value, path)
                # Yield return
                if not nextPath:
                    yield nextValue
                else:
                    for v in self.itemType.query(nextValue, nextPath):
                        yield v
            except QueryNotMatchError:
                pass

    def clone(self, value):
        """Clone the value
        """
        if self.isEmpty(value):
            return value
        else:
            clonedValue = {}
            for k, item in value.iteritems():
                clonedValue[k] = self.itemType.clone(item)
            return clonedValue

class ModelType(DataType):
    """The model type
    """
    def __init__(self, cls, *args, **kwargs):
        """Create a new ModelType
        """
        from models import DataModel
        # Check
        if not issubclass(cls, DataModel):
            raise TypeError('cls must be a subclass of DataModel')
        # Set attributes
        self.cls = cls
        # Super
        super(ModelType, self).__init__(*args, **kwargs)

    def __loadmodel__(self, cls, value, model, container):
        """Load the model
        """
        return cls.load(value)

    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, self.cls):
            return value
        elif isinstance(value, dict):
            return self.__loadmodel__(self.cls, value, model, container)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), (self.cls, dict), value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        if not self.isEmpty(value):
            return value.dump(context)

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        if not self.isEmpty(value):
            # Check value type
            if not isinstance(value, self.cls):
                raise TypeValidationError(self.cls, type(value), value)
            # Continue validate
            value.validate(required = required, continueOnError = continueOnError)
        # Super
        super(ModelType, self).__validatevalue__(value, required, continueOnError)

    def query(self, value, path):
        """Query the value by path
        """
        if value:
            for v in value.query(path):
                yield v

    def clone(self, value):
        """Clone the value
        """
        if self.isEmpty(value):
            return value
        else:
            return value.clone()

class DynamicModelType(DataType):
    """The dynamic model type
    """
    def __init__(self, clsSelector, *args, **kwargs):
        """Create a new DynamicModelType
        """
        self.clsSelector = clsSelector
        # Super
        super(DynamicModelType, self).__init__(*args, **kwargs)

    def __loadmodel__(self, cls, value, model, container):
        """Load the model
        """
        return cls.load(value)

    def __loadvalue__(self, value, model, container):
        """Load the value
        Returns:
            The loaded value
        """
        from models import DataModel
        if isinstance(value, dict):
            # Get model class
            cls = self.clsSelector(value, model, container)
            # Load the model
            return self.__loadmodel__(cls, value, model, container)
        elif isinstance(value, DataModel):
            # Return the model
            return value
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), dict, value)

    def __dumpvalue__(self, value, model, container, context):
        """Dump the value
        """
        if not self.isEmpty(value):
            return value.dump(context)

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        if not self.isEmpty(value):
            # Continue validate
            value.validate(required = required, continueOnError = continueOnError)
        # Super
        super(DynamicModelType, self).__validatevalue__(value, required, continueOnError)

    def query(self, value, path):
        """Query the value by path
        """
        if value:
            for v in value.query(path):
                yield v

    def clone(self, value):
        """Clone the value
        """
        if self.isEmpty(value):
            return value
        else:
            return value.clone()

class AnyType(DataType):
    """The any type of value type
    """
    def __init__(self, types = None, *args, **kwargs):
        """Create a new AnyType
        """
        self.types = types
        # Super
        super(AnyType, self).__init__(*args, **kwargs)

    def __validatevalue__(self, value, required, continueOnError):
        """Validate the value
        """
        if self.types:
            for t in self.types:
                if isinstance(value, t):
                    break
            else:
                raise TypeValidationError(self.types, type(value), value)
        # Super
        super(AnyType, self).__validatevalue__(value, required, continueOnError)

    def query(self, value, path):
        """Query the value by path
        Returns:
            Yield the value
        """
        if not self.isEmpty(value):
            # Check the value
            if isinstance(value, list):
                # Extend the values
                for item in value:
                    for v in self.query(item, path):
                        yield v
            elif isinstance(value, dict):
                # Get the values by name
                index = path.find('.')
                if index == -1:
                    name = path
                    nextPath = None
                else:
                    name = path[: index]
                    nextPath = path[index + 1: ]
                # Get value
                if not name in value:
                    return
                nextValue = value[name]
                # Yield return
                if not nextPath:
                    yield nextValue
                else:
                    for v in self.query(nextValue, nextPath):
                        yield v
            else:
                raise UnqueryableValueError

    def clone(self, value):
        """Clone the value
        """
        if self.isEmpty(value):
            return value
        else:
            return deepcopy(value)
