# encoding=utf8

""" The errors
    Author: lipixun
    Created Time : 二  3/15 16:31:59 2016

    File Name: errors.py
    Description:

"""

class DataHubError(Exception):
    """The data hub error
    """
    def __repr__(self):
        """Repr
        """
        return '%s(%s)' % (type(self).__name__, str(self))

class FeatureNotSupportedError(DataHubError):
    """The feature is not supported
    """
    def __init__(self, feature = None):
        """Create a new FeatureNotSupportedError
        """
        self.feature = feature

    def __str__(self):
        """Convert to string
        """
        return str(self.feature) if self.feature else 'N/A'

class InvalidParameterError(DataHubError):
    """The parameter is invalid
    """
    def __init__(self, reason = None):
        """Create a new InvalidParameterError
        """
        self.reason = reason

    def __str__(self):
        """Convert to string
        """
        return str(self.reason or '')

class BadValueError(DataHubError):
    """Bad value error
    """
    def __init__(self, reason):
        """Create a new BadValueError
        """
        self.reason = reason

    def __str__(self):
        """To string
        """
        return 'Reason [%s]' % self.reason

class FeatureNotEnabledError(DataHubError):
    """The feature is not enabled
    """
    def __init__(self, name):
        """Create a new FeatureNotEnabledError
        """
        self.name = name

    def __str__(self):
        """Convert to string
        """
        return str(self.name or '')

class DuplicatedKeyError(DataHubError):
    """The duplicated key error
    """
    def __init__(self, message, key = None):
        """Create a new DuplicatedKeyError
        """
        self.key = key
        self.message = message

    def __str__(self):
        """Convert to string
        """
        return 'Key [%s] Message [%s]' % (self.key, self.message)

class ModelNotFoundError(DataHubError):
    """The model not found error
    """
    pass

class FieldNotDumpError(DataHubError):
    """The field is not dumped
    """
    pass

class WatchTimeoutError(DataHubError):
    """The watch is timed out
    """

class WatchResetError(DataHubError):
    """The watch is reset (Some untracable changes happened)
    """

class QueryNotMatchError(DataHubError):
    """The query is not matched
    """
    def __init__(self, name, nextPath, type = None):
        """Create a new QueryNotMatchError
        """
        self.name = name
        self.nextPath = nextPath
        self.type = type

    def __str__(self):
        """Convert to string
        """
        return 'Name [%s] Next [%s] Type [%s]' % (self.name, self.nextPath, type(self.type).__name__ if self.type else 'N/A')

class UnqueryableValueError(DataHubError):
    """Indicates the value is not queryable
    """

class DataModelError(DataHubError):
    """The data model error
    """
    pass

class CompoundDataModelError(DataModelError):
    """The compound data model error
    """
    def __init__(self, errors, message = None):
        """Create new CompoundDataModelError
        """
        self.errors = errors
        self.message = message

    def __str__(self):
        """To string
        """
        return 'Compound Message [%s] Errors [%s]' % (self.message or 'N/A', ','.join([ repr(x) for x in self.errors ]))

class NestedDataModelError(DataModelError):
    """The nested validation error
    """
    def __init__(self, key, error, message = None):
        """Create new NestedDataModelError
        """
        self.key = key
        self.error = error
        self.message = message

    def __str__(self):
        """To string
        """
        return 'Nested Error Key [%s] Message [%s] Inner Error [%s]' % (self.key, self.message or 'N/A', repr(self.error))

class ValueConversionError(DataModelError):
    """Value conversion error
    """
    def __init__(self, sourceType, targetType, value):
        """Create a new ValueConversionError
        """
        self.sourceType = sourceType
        self.targetType = targetType
        self.value = value

    def __str__(self):
        """To string
        """
        return 'Source Type [%s] Target Type [%s] Value [%s]' % (
            self.sourceType,
            self.targetType,
            self.value,
            )

class UnknownFieldError(DataModelError):
    """The unknown field error
    """
    def __init__(self, key):
        """Create a new UnknownFieldError
        """
        self.key = key
        super(UnknownFieldError, self).__init__()

    def __str__(self):
        """Convert to string
        """
        return 'Unknown field [%s]' % self.key

class ValidationError(DataModelError):
    """The validation error
    """

class MissingRequiredFieldError(ValidationError):
    """Missing required field error
    """
    def __init__(self, field):
        """Create a new MissingRequiredFieldError
        """
        self.field = field
        # Super
        super(MissingRequiredFieldError, self).__init__()

    def __str__(self):
        """Convert to string
        """
        return 'Missing field [%s]' % self.field

class TypeValidationError(ValidationError):
    """The type validation error
    """
    def __init__(self, expectedType, actualType, value):
        """Create a new TypeValidationError
        """
        self.expectedType = expectedType
        self.actualType = actualType
        self.value = value
        # Super
        super(TypeValidationError, self).__init__()

    def __str__(self):
        """Convert to string
        """
        return 'Expect type [%s] actual type [%s] value [%s]' % (self.expectedType, self.actualType, self.value)

class ChoiceValidationError(ValidationError):
    """The choice validation error
    """
    def __init__(self, value, choices):
        """Create a new ChoiceValidationError
        """
        self.value = value
        self.choices = choices
        # Super
        super(ChoiceValidationError, self).__init__()

    def __str__(self):
        """Convert to string
        """
        return 'Value [%s] not in choices %s' % (self.value, self.choices)
