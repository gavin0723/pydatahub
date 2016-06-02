# encoding=utf8

""" The conditions
    Author: lipixun
    Created Time : 二  3/15 17:01:11 2016

    File Name: conditions.py
    Description:

"""

from datahub.model import nullValue, DataModel, DataType, StringType, BooleanType, ListType, ModelType, AnyType
from datahub.errors import BadValueError

class ConditionType(ModelType):
    """The condition data type
    NOTE:
        Actually this type is not necessary any more. Use ModelType is enough, but for forward compatible.
    """
    def __init__(self, *args, **kwargs):
        """Create a new ConditionType
        """
        super(ConditionType, self).__init__(Condition, *args, **kwargs)

class Condition(DataModel):
    """The condition
    """
    def check(self, model):
        """Check if the model satisfy the condition
        Returns:
            True / False
        """
        raise NotImplementedError

    def dump(self, context = None):
        """Dump this condition
        """
        return { self.NAME: super(Condition, self).dump(context) }

    @classmethod
    def load(cls, raw, continueOnError = False):
        """Load the condition object
        """
        if len(raw) != 1:
            raise BadValueError('Condition dict must have only one key and value')
        k, v = raw.keys()[0], raw.values()[0]
        if not k in CONDITIONS:
            raise BadValueError('Condition [%s] not found' % k)
        return CONDITIONS[k](v, __continueOnError__ = continueOnError)

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
            # Done
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
            # Done
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
            # Done
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
        # Done
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
        # Done
        return True

class GreaterCondition(Condition):
    """The greater condition
    """
    NAME = 'greater'

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
        # Done
        return False

class LesserCondition(Condition):
    """The lesser condition
    """
    NAME = 'lesser'

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
        # Done
        return False

CONDITIONS = dict(map(lambda x: (x.NAME, x), (
    AndCondition,
    OrCondition,
    NotCondition,
    KeyValueCondition,
    KeyValuesCondition,
    ExistCondition,
    NonExistCondition,
    GreaterCondition,
    LesserCondition
    )))
