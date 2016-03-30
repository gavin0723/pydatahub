# encoding=utf8

""" The data types
    Author: lipixun
    Created Time : 六  3/12 23:26:23 2016

    File Name: _types.py
    Description:

"""

from sets import Set
from datetime import datetime, date, time, timedelta

import arrow

from datahub.errors import DataModelError, NestedDataModelError, MissingRequiredFieldError, TypeValidationError, ValueConversionError, ChoiceValidationError

from spec import *

class DataType(object):
    """The base data type
    """
    def __init__(self,
        name = None,
        required = False,
        default = nullValue,
        loader = None,
        dumper = None,
        validator = None,
        choices = None,
        dumpEmpty = False,
        doc = None
        ):
        """Create a new DataType
        """
        self.name = name
        self.required = required
        self.default = default
        self._loader = loader
        self._dumper = dumper
        self._validator = validator
        self.choices = choices
        self.dumpEmpty = dumpEmpty
        self.doc = doc

    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value (The default loader method)
        Returns:
            The loaded value
        """
        return value

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        return value

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Run choices validation
        if not self.isEmpty(value) and self.choices and not value in self.choices:
            raise ChoiceValidationError(value, self.choices)
        # Run custom validator
        if self._validator:
            self._validator(value, context)

    def __get__(self, instance, cls):
        """Get the value of this data type
        """
        if instance:
            # Get the value
            if not self.exists(instance):
                # Value not exists, try to set default
                if not self.hasDefault():
                    metadata = instance.getMetadata() or DEFAULT_MODEL_METADATA
                    if metadata.none4Unassigned:
                        return
                    else:
                        raise AttributeError
                # Initialize
                value = self.getDefault()
                self.__validatevalue__(value, None)
                getattr(instance, STORE_NAME)[self.name] = value
                return value
            # Return
            return getattr(instance, STORE_NAME)[self.name]
        else:
            # Get the type
            return self

    def __set__(self, instance, value):
        """Set the value of this data type
        """
        metadata = instance.getMetadata() or DEFAULT_MODEL_METADATA
        if metadata.strict:
            self.__validatevalue__(value)
        # Good, set the value
        getattr(instance, STORE_NAME)[self.name] = value

    def __delete__(self, instance):
        """Delete the value of this type
        """
        metadata = instance.getMetadata() or DEFAULT_MODEL_METADATA
        if metadata.strict and self.required:
            raise MissingRequiredFieldError(self.name)
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

    def load(self, value, loadContext = None, validateContext = None):
        """Load the value of this type
        """
        if not loadContext:
            loadContext = EMPTY_LOAD_CONTEXT
        if not validateContext:
            validateContext = DEFAULT_VALIDATE_CONTEXT
        # Load the value
        if self._loader:
            value = self._loader(value, loadContext, validateContext)
        else:
            value = self.__loadvalue__(value, loadContext, validateContext)
        # Validate the loaded value
        self.__validatevalue__(value, validateContext)
        # Done
        return value

    def dump(self, value, context):
        """Dump this type of the model
        """
        if self._dumper:
            return self._dumper(self, value, context)
        else:
            return self.__dumpvalue__(value, context)

    def emptyDump(self):
        """Dump the empty value
        """
        return None

    def validate(self, value, context):
        """Validate this type
        """
        self.__validatevalue__(value, context)

    def query(self, value, path):
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
    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, basestring):
            raise TypeValidationError(basestring, type(value), value)
        # Super
        super(StringType, self).__validatevalue__(value, context)

class IntegerType(DataType):
    """The integer type
    """
    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, (int, long)):
            return value
        elif isinstance(value, basestring) and value.isdigit():
            return int(value)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), (int, long), value, loadContext.raw)

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, (int, long)):
            raise TypeValidationError((int, long), type(value), value)
        # Super
        super(IntegerType, self).__validatevalue__(value, context)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or value == ''

class FloatType(DataType):
    """The float type
    """
    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, float):
            return value
        elif isinstance(value, basestring):
            try:
                return float(value)
            except:
                raise ValueConversionError(type(value), float, value, loadContext.raw)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), float, value, loadContext.raw)

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, float):
            raise TypeValidationError(float, type(value), value)
        # Super
        super(FloatType, self).__validatevalue__(value, context)

class BooleanType(DataType):
    """The boolean type
    """
    def __loadvalue__(self, value, loadContext, validateContext):
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
                raise ValueConversionError(type(value), bool, value, loadContext.raw)
        elif isinstance(value, (int, long)):
            if value == 1:
                return True
            elif value == 0:
                return False
            else:
                raise ValueConversionError(type(value), bool, value, loadContext.raw)
        elif isinstance(value, float):
            if value >= 0.9999999 and value <= 1.0000001:
                return True
            elif value >= -0.00000001 and value <= 0.00000001:
                return false
            else:
                raise ValueConversionError(type(value), bool, value, loadContext.raw)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), bool, value, loadContext.raw)

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, bool):
            raise TypeValidationError(bool, type(value), value)
        # Super
        super(BooleanType, self).__validatevalue__(value, context)

class DatetimeType(DataType):
    """The datetime type
    """
    def __loadvalue__(self, value, loadContext, validateContext):
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
            raise ValueConversionError(type(value), datetime, value, loadContext.raw)

    def __dumpvalue__(self, value, context):
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

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not isinstance(value, datetime):
            raise TypeValidationError(datetime, type(value), value)
        # Super
        super(DatetimeType, self).__validatevalue__(value, context)

class DateType(DataType):
    """The date type
    """
    def __loadvalue__(self, value, loadContext, validateContext):
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
            raise ValueConversionError(type(value), date, value, loadContext.raw)

    def __dumpvalue__(self, value, context):
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

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, date):
            raise TypeValidationError(date, type(value), value)
        # Super
        super(DateType, self).__validatevalue__(value, context)

class TimeType(DataType):
    """The time type
    """
    def __loadvalue__(self, value, loadContext, validateContext):
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
            raise ValueConversionError(type(value), date, value, loadContext.raw)

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        if context.time2str:
            # Convert time to string
            return value.strftime(context.timeFormat or '%H:%M:%S.%f')
        else:
            # Just return
            return value

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, time):
            raise TypeValidationError(time, type(value), value)
        # Super
        super(TimeType, self).__validatevalue__(value, context)

class TimeDeltaType(DataType):
    """The time delta type
    """
    def __loadvalue__(self, value, loadContext, validateContext):
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
                raise ValueConversionError(type(value), timedelta, value, loadContext.raw)
        elif isinstance(value, (int, long, float)):
            return timedelta(seconds = float(value))
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), timedelta, value, loadContext.raw)

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        return value.total_seconds()

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, timedelta):
            raise TypeValidationError(timedelta, type(value), value)
        # Super
        super(TimeDeltaType, self).__validatevalue__(value, context)

class ListType(DataType):
    """The list type
    """
    def __init__(self,
        itemType,
        name = None,
        required = False,
        default = nullValue,
        loader = None,
        dumper = None,
        validator = None,
        choices = None,
        dumpEmpty = False,
        doc = None
        ):
        """Create a new ListType
        """
        self.itemType = itemType
        super(ListType, self).__init__(name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, (list, tuple)):
            items = []
            nestedError = None
            for i in range(0, len(value)):
                v = value[i]
                try:
                    items.append(self.itemType.load(v, LoadContext(loadContext.key, value, loadContext.model), validateContext))
                except Exception as error:
                    if not nestedError:
                        nestedError = NestedDataModelError({}, value)
                    nestedError.errors['_item[%s]' % i] = error
                    if not validateContext.continueOnError:
                        break
            if nestedError:
                raise nestedError
            return items
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), (list, tuple), value, loadContext.raw)

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        return [ self.itemType.dump(x, context) for x in value ]

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, list):
            raise TypeValidationError(list, type(value), value)
        # Check items
        if not self.isEmpty(value):
            nestedError = None
            for i in range(0, len(value)):
                v = value[i]
                try:
                    self.itemType.validate(v, context)
                except Exception as error:
                    if not nestedError:
                        nestedError = NestedDataModelError({})
                    nestedError.errors['_item[%s]' % i] = error
                    if not context.continueOnError:
                        break
            if nestedError:
                raise nestedError
        # Super
        super(ListType, self).__validatevalue__(value, context)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or len(value) == 0

    def emptyDump(self, value):
        """Dump the empty value
        """
        return []

    def query(self, value, path):
        """Query the value by path
        Returns:
            Yield the value
        """
        for item in value:
            for v in self.itemType.query(item, path):
                yield v

class SetType(DataType):
    """The set type
    """
    def __init__(self,
        itemType,
        name = None,
        required = False,
        default = nullValue,
        loader = None,
        dumper = None,
        validator = None,
        choices = None,
        dumpEmpty = False,
        doc = None
        ):
        """Create a new ListType
        """
        self.itemType = itemType
        super(SetType, self).__init__(name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, (list, tuple, Set)):
            items = []
            nestedError = None
            for v in value:
                try:
                    items.append(self.itemType.load(v, LoadContext(loadContext.key, value, loadContext.model), validateContext))
                except Exception as error:
                    if not nestedError:
                        nestedError = NestedDataModelError({}, value)
                    nestedError.errors[str(v)] = error
                    if not validateContext.continueOnError:
                        break
            if nestedError:
                raise nestedError
            # Done
            return Set(items)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), (list, tuple, Set), value, loadContext.raw)

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        return [ self.itemType.dump(x, context) for x in value ]

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, Set):
            raise TypeValidationError(Set, type(value), value)
        # Check items
        if not self.isEmpty(value):
            nestedError = None
            for v in value:
                try:
                    self.itemType.validate(v, context)
                except Exception as error:
                    if not nestedError:
                        nestedError = NestedDataModelError({})
                    nestedError.errors[str(v)] = error
                    if not context.continueOnError:
                        break
            if nestedError:
                raise nestedError
        # Super
        super(SetType, self).__validatevalue__(value, context)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or len(value) == 0

    def emptyDump(self, value):
        """Dump the empty value
        """
        return []

    def query(self, value, path):
        """Query the value by path
        Returns:
            Yield the value
        """
        for item in value:
            for v in self.itemType.query(item, path):
                yield v

class DictType(DataType):
    """The dict type
    """
    def __init__(self,
        itemType,
        name = None,
        required = False,
        default = nullValue,
        loader = None,
        dumper = None,
        validator = None,
        choices = None,
        dumpEmpty = False,
        doc = None
        ):
        """Create a new ListType
        """
        self.itemType = itemType
        super(DictType, self).__init__(name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, dict):
            items = {}
            nestedError = None
            for k, v in value.iteritems():
                try:
                    if not isinstance(k, basestring):
                        raise TypeValidationError(basestring, type(v), v, value)
                    items[k] = self.itemType.load(v, LoadContext(loadContext.key, value, loadContext.model), validateContext)
                except Exception as error:
                    if not nestedError:
                        nestedError = NestedDataModelError({}, value)
                    nestedError.errors[k] = error
                    if not validateContext.continueOnError:
                        break
            if nestedError:
                raise nestedError
            # Done
            return items
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), dict, value, loadContext.raw)

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        return dict(map(lambda (k, v): (k, self.itemType.dump(v, context)), value.iteritems()))

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Check the value type
        if not self.isEmpty(value) and not isinstance(value, dict):
            raise TypeValidationError(dict, type(value), value)
        # Check items
        if not self.isEmpty(value):
            nestedError = None
            for k, v in value.iteritems():
                try:
                    if not isinstance(k, basestring):
                        raise TypeValidationError(basestring, type(v), v, value)
                    self.itemType.validate(v, context)
                except Exception as error:
                    if not nestedError:
                        nestedError = NestedDataModelError({})
                    nestedError.errors[k] = error
                    if not context.continueOnError:
                        break
            if nestedError:
                raise nestedError
        # Super
        super(DictType, self).__validatevalue__(value, context)

    def isEmpty(self, value):
        """Tell if the value is empty
        """
        return value is None or len(value) == 0

    def emptyDump(self, value):
        """Dump the empty value
        """
        return {}

    def query(self, value, path):
        """Query the value by path
        Returns:
            Yield the value
        """
        index = path.find('.')
        if index == -1:
            name = path
            nextPath = None
        else:
            name = path[: index]
            nextPath = path[index + 1: ]
        # Get the value
        if not name in value:
            return
        nextValue = value[name]
        # Continue quering or not
        if not nextPath:
            yield nextValue
        else:
            for v in self.itemType.query(nextValue, nextPath):
                yield v

class ModelType(DataType):
    """The model type
    """
    def __init__(self,
        modelClass,
        name = None,
        required = False,
        default = nullValue,
        loader = None,
        dumper = None,
        validator = None,
        choices = None,
        dumpEmpty = False,
        doc = None
        ):
        """Create a new ModelType
        """
        self.modelClass = modelClass
        super(ModelType, self).__init__(name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __loadmodel__(self, modelClass, value, loadContext):
        """Load the model
        """
        return modelClass(value)

    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value
        Returns:
            The loaded value
        """
        if self.isEmpty(value):
            pass
        elif isinstance(value, self.modelClass):
            return value
        elif isinstance(value, dict):
            try:
                return self.__loadmodel__(self.modelClass, value, loadContext)
            except Exception as error:
                raise NestedDataModelError({ self.modelClass.__name__: error }, value)
        else:
            raise ValueConversionError(type(value), (self.modelClass, dict), value, loadContext.raw)

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        if not self.isEmpty(value):
            return value.dump(context)

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        try:
            if not self.isEmpty(value):
                self.modelClass.validate(value, context)
        except Exception as error:
            raise NestedDataModelError({ self.modelClass.__name__: error }, value)
        # Super
        super(ModelType, self).__validatevalue__(value, context)

    def query(self, value, path):
        """Query the value by path
        Returns:
            Yield the value
        """
        return value.query(path)

class DynamicModelType(DataType):
    """The dynamic model type
    """
    def __init__(self,
        modelClassSelector = None,
        name = None,
        required = False,
        default = nullValue,
        loader = None,
        dumper = None,
        validator = None,
        choices = None,
        dumpEmpty = None,
        doc = None
        ):
        """Create a new DynamicModelType
        """
        self._modelClassSelector = modelClassSelector
        super(DynamicModelType, self).__init__(name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __loadvalue__(self, value, loadContext, validateContext):
        """Load the value
        Returns:
            The loaded value
        """
        if isinstance(value, dict):
            modelClass = self._modelClassSelector(value, loadContext)
            return modelClass(value)
        elif not self.isEmpty(value):
            raise ValueConversionError(type(value), dict, value, loadContext.raw)

    def __dumpvalue__(self, value, context):
        """Dump the value
        """
        return value.dump(context)

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        # Super
        super(DynamicModelType, self).__validatevalue__(value, context)

    def modelClassSelector(self, method):
        """Set the model class selector
        """
        self._modelClassSelector = method

    def query(self, value, path):
        """Query the value by path
        Returns:
            Yield the value
        """
        return value.query(path)

class AnyType(DataType):
    """The any type of value type
    """
    def __init__(self,
        types = None,
        name = None,
        required = False,
        default = nullValue,
        loader = None,
        dumper = None,
        validator = None,
        choices = None,
        dumpEmpty = False,
        doc = None
        ):
        """Create a new AnyType
        """
        self.types = types
        super(AnyType, self).__init__(name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __validatevalue__(self, value, context):
        """Validate the value
        """
        if self.types:
            for _type in self.types:
                if isinstance(value, _type):
                    break
            else:
                raise TypeValidationError(self.types, type(value), value)
        # Super
        super(AnyType, self).__validatevalue__(value, context)

    def query(self, value, path):
        """Query the value by path
        Returns:
            Yield the value
        """
        if not path:
            yield value
            return
        # Check the value
        if isinstance(value, (list, Set)):
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
            value = value[name]
            if not nextPath:
                yield value
            else:
                for v in self.query(value, nextPath):
                    yield v
