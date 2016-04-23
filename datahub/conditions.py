# encoding=utf8

""" The conditions
    Author: lipixun
    Created Time : 二  3/15 17:01:11 2016

    File Name: conditions.py
    Description:

"""

from datahub.model import nullValue, DataModel, DataType, StringType, BooleanType, ListType, ModelType, AnyType
from datahub.errors import BadValueError

def loadCondition(value):
    """Load the condition
    """
    if len(value) != 1:
        raise BadValueError('Condition dict must have only one key and value')
    k, v = value.keys()[0], value.values()[0]
    if not k in CONDITIONS:
        raise BadValueError('Condition [%s] not found' % k)
    return CONDITIONS[k](value)

class ConditionType(ModelType):
    """The condition data type
    """
    def __init__(self,
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
        """Create a new ConditionType
        """
        super(ConditionType, self).__init__(Condition, name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __loadmodel__(self, modelClass, value, loadContext):
        """Load the model
        """
        return loadCondition(value)

class Condition(DataModel):
    """The condition
    """
    def __init__(self, raw = None, **kwargs):
        """Create a new Condition
        """
        if raw:
            if len(raw) != 1:
                raise ValueError('The length of the dict of condition must be 1')
            super(Condition, self).__init__(raw.values()[0], **kwargs)
        else:
            super(Condition, self).__init__(raw, **kwargs)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        raise NotImplementedError

    def dump(self, context = None):
        """Dump this condition
        """
        # Dump the values
        rawDumpValue = super(Condition, self).dump(context)
        # Wrap the raw dump value by name
        return { self.NAME: rawDumpValue }

class AndCondition(Condition):
    """And condition
    """
    NAME = 'and'

    conditions = ListType(ConditionType(), required = True, default = lambda: [])

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        for condition in self.conditions:
            if not condition.check(model):
                return False
        return True

class OrCondition(Condition):
    """Or condition
    """
    NAME = 'or'

    conditions = ListType(ConditionType(), required = True, default = lambda: [])

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        for condition in self.conditions:
            if condition.check(model):
                return True
        return False

class NotCondition(Condition):
    """Create a not condition
    """
    NAME = 'not'

    condition = ConditionType(required = True)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        return not self.condition.check(model)

class KeyValueCondition(Condition):
    """The key value condition
    """
    NAME = 'kv'

    key = StringType(required = True)
    value = AnyType(required = True)
    equals = BooleanType(required = True, default = True)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        if self.equals:
            # Equals
            for v in model.query(self.key):
                if v == self.value:
                    return True
        else:
            # Not equals
            #   - Has field not equals to the value
            #   - Field not exists
            hasValue = False
            for v in model.query(self.key):
                if v != self.value:
                    return True
                hasValue = True
            if not hasValue:
                return True
            # Done
        return False

class KeyValuesCondition(Condition):
    """The key values condition
    """
    NAME = 'kvs'

    key = StringType(required = True)
    values = ListType(AnyType(), required = True)
    includes = BooleanType(required = True, default = True)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        if self.includes:
            # Includes
            for v in model.query(self.key):
                if v in self.values:
                    return True
        else:
            # Not includes
            #   - Has field not in the values
            #   - Field not exists
            hasValue = False
            for v in model.query(self.key):
                if not v in self.values:
                    return True
                hasValue = True
            if not hasValue:
                return True
        return False

class ExistCondition(Condition):
    """The exist condition
    """
    NAME = 'exist'

    key = StringType(required = True)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        for v in model.query(self.key):
            return True
        return False

class NonExistCondition(Condition):
    """The non-exist condition
    """
    NAME = 'nonexist'

    key = StringType(required = True)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        for v in model.query(self.key):
            return False
        return True

class LargerCondition(Condition):
    """The larger condition
    """
    NAME = 'larger'

    key = StringType(required = True)
    value = AnyType(required = True)
    equals = BooleanType(required = True, default = False)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        for v in model.query(self.key):
            if self.equals:
                if v >= self.value:
                    return True
            else:
                if v > self.value:
                    return True
        return False

class SmallerCondition(Condition):
    """The smaller condition
    """
    NAME = 'smaller'

    key = StringType(required = True)
    value = AnyType(required = True)
    equals = BooleanType(required = True, default = False)

    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        for v in model.query(self.key):
            if self.equals:
                if v <= self.value:
                    return True
            else:
                if v < self.value:
                    return True
        return False

CONDITIONS = dict(map(lambda x: (x.NAME, x), (
    AndCondition,
    OrCondition,
    NotCondition,
    KeyValueCondition,
    KeyValuesCondition,
    ExistCondition,
    NonExistCondition,
    LargerCondition,
    SmallerCondition
    )))
