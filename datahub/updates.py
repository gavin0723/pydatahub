# encoding=utf8

""" The updates
    Author: lipixun
    Created Time : 四  3/17 20:00:29 2016

    File Name: updates.py
    Description:

"""

from datahub.model import nullValue, DataModel, ModelType, StringType, BooleanType, IntegerType, AnyType, ListType
from datahub.errors import BadValueError

def loadUpdateAction(value):
    """Load the update action
    """
    if len(value) != 1:
        raise BadValueError('Update action dict must have only one key and value')
    k, v = value.keys()[0], value.values()[0]
    if not k in ACTIONS:
        raise BadValueError('Update action [%s] not found' % k)
    return ACTIONS[k](v)

class UpdateActionType(ModelType):
    """The update action data type
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
        """Create a new UpdateActionType
        """
        super(UpdateActionType, self).__init__(Condition, name, required, default, loader, dumper, validator, choices, dumpEmpty, doc)

    def __loadmodel__(self, modelClass, value, loadContext):
        """Load the model
        """
        return loadUpdateAction(value)

class UpdateAction(DataModel):
    """The update action
    """
    key = StringType(required = True, doc = 'The update key')

class PushAction(UpdateAction):
    """The update action
    """
    # The position
    position = IntegerType(doc = 'The push position')
    # The value
    value = AnyType(required = True, doc = 'The push value')

class PushsAction(UpdateAction):
    """The push actions
    """
    # The position
    position = IntegerType(doc = 'The push position')
    # The value
    values = ListType(AnyType(), required = True, doc = 'The push values')

class PopAction(UpdateAction):
    """The pop action
    """
    head = BooleanType(required = True, default = True, doc = 'Pop from head or not')

class SetAction(UpdateAction):
    """Set action
    """
    value = AnyType(required = True)

class ClearAction(UpdateAction):
    """Clear action
    """
    pass

ACTIONS = {
    'push':         PushAction,
    'pushs':        PushsAction,
    'pop':          PopAction,
    'set':          SetAction,
    'clear':        ClearAction,
}