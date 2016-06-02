# encoding=utf8

""" The updates
    Author: lipixun
    Created Time : 四  3/17 20:00:29 2016

    File Name: updates.py
    Description:

"""

from datahub.model import nullValue, DataModel, ModelType, StringType, BooleanType, IntegerType, AnyType, ListType
from datahub.errors import BadValueError

class UpdateActionType(ModelType):
    """The update action data type
    NOTE:
        Actually this type is not necessary any more. Use ModelType is enough, but for forward compatible.
    """
    def __init__(self, *args, **kwargs):
        """Create a new UpdateAction
        """
        super(UpdateAction, self).__init__(UpdateAction, *args, **kwargs)

class UpdateAction(DataModel):
    """The update action
    """
    key = StringType(required = True, doc = 'The update key')

    def dump(self, context = None):
        """Dump this condition
        """
        return { self.NAME: super(UpdateAction, self).dump(context) }

    @classmethod
    def load(cls, raw, continueOnError = False):
        """Load the condition object
        """
        if len(raw) != 1:
            raise BadValueError('Update action dict must have only one key and value')
        k, v = raw.keys()[0], raw.values()[0]
        if not k in ACTIONS:
            raise BadValueError('Update action [%s] not found' % k)
        return ACTIONS[k](v, __continueOnError__ = continueOnError)

class PushAction(UpdateAction):
    """The update action
    """
    NAME = 'push'
    # The position
    position = IntegerType(doc = 'The push position')
    # The value
    value = AnyType(required = True, doc = 'The push value')

class PushsAction(UpdateAction):
    """The push actions
    """
    NAME = 'pushs'
    # The position
    position = IntegerType(doc = 'The push position')
    # The value
    values = ListType(AnyType(), required = True, doc = 'The push values')

class PopAction(UpdateAction):
    """The pop action
    """
    NAME = 'pop'
    # The head
    head = BooleanType(required = True, default = True, doc = 'Pop from head or not')

class SetAction(UpdateAction):
    """Set action
    """
    NAME = 'set'
    # The value
    value = AnyType(required = True)

class ClearAction(UpdateAction):
    """Clear action
    """
    NAME = 'clear'

ACTIONS = {
    'push':         PushAction,
    'pushs':        PushsAction,
    'pop':          PopAction,
    'set':          SetAction,
    'clear':        ClearAction,
}
