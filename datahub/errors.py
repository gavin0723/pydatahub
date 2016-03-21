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
    def __init__(reason = None):
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
    def __init__(self, reason, context = None):
        """Create a new BadValueError
        """
        self.reason = reason
        self.context = context

    def __str__(self):
        """To string
        """
        return 'Reason [%s] Context [%s]' % (self.reason, self.context)

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
    def __init__(self, message):
        """Create a new DuplicatedKeyError
        """
        self.message = message

class ModelNotFoundError(DataHubError):
    """The model not found error
    """
    pass

class DataModelError(DataHubError):
    """The data model error
    """
    pass

class NestedDataModelError(DataModelError):
    """The nested validation error
    """
    def __init__(self, errors, context = None):
        """Create new NestedDataModelError
        """
        self.errors = errors
        self.context = context

    def __str__(self):
        """To string
        """
        return 'Nested Errors: [%s] Context [%s]' % (self.errors, self.context)

class ValueConversionError(DataModelError):
    """Value conversion error
    """
    def __init__(self, sourceType, targetType, value, context = None):
        """Create a new ValueConversionError
        """
        self.sourceType = sourceType
        self.targetType = targetType
        self.value = value
        self.context = context

    def __str__(self):
        """To string
        """
        return 'Source Type [%s] Target Type [%s] Value [%s] Context [%s]' % (
            self.sourceType,
            self.targetType,
            self.value,
            self.context
            )

class UnknownFieldError(DataModelError):
    """The unknown field error
    """
    def __init__(self, key, context):
        """Create a new UnknownFieldError
        """
        self.key = key
        super(UnknownFieldError, self).__init__(context)

    def __str__(self):
        """Convert to string
        """
        return 'Unknown field [%s] context [%s]' % (self.key, self.context)

class ValidationError(DataModelError):
    """The validation error
    """
    def __init__(self, context):
        """Create a new ValidationError

        :param context:
            The error value context, aka, the piece of data which has error
        """
        self.context = context

    def __str__(self):
        """To string
        """
        return 'Context: [%s]' % self.context

class MissingRequiredFieldError(ValidationError):
    """Missing required field error
    """
    def __init__(self, field, context = None):
        """Create a new MissingRequiredFieldError
        """
        self.field = field
        super(MissingRequiredFieldError, self).__init__(context)

    def __str__(self):
        """Convert to string
        """
        return 'Missing field [%s] context [%s]' % (self.field, self.context)

class TypeValidationError(ValidationError):
    """The type validation error
    """
    def __init__(self, expectedType, actualType, value, context = None):
        """Create a new TypeValidationError
        """
        self.expectedType = expectedType
        self.actualType = actualType
        self.value = value
        super(TypeValidationError, self).__init__(context)

    def __str__(self):
        """Convert to string
        """
        return 'Expect type [%s] actual type [%s] value [%s] context [%s]' % (
            self.expectedType,
            self.actualType,
            self.value,
            self.context
            )

class ChoiceValidationError(ValidationError):
    """The choice validation error
    """
    def __init__(self, value, choices, context = None):
        """Create a new ChoiceValidationError
        """
        self.value = value
        self.choices = choices
        super(ChoiceValidationError, self).__init__(context)

    def __str__(self):
        """Convert to string
        """
        return 'Value [%s] not in choices %s context [%s]' % (
            self.value,
            self.choices,
            self.context
            )
