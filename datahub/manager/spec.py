# encoding=utf8

""" The spec
    Author: lipixun
    Created Time : 二  3/15 20:50:04 2016

    File Name: spec.py
    Description:

"""

EVENT_CREATED           = 'created'
EVENT_REPLACED          = 'replaced'
EVENT_UPDATED           = 'updated'
EVENT_DELETED           = 'deleted'

class EventArgs(object):
    """The event arguments
    """
    def __init__(self, name, feature, models, affectCount, affectIDs):
        """Create a new EventArgs
        """
        self.name = name
        self.feature = feature
        self.models = models
        self.affectCount = affectCount
        self.affectIDs = affectIDs

WATCH_PRESERVED         = 'preserved'
WATCH_RESET             = 'reset'
WATCH_CREATED           = 'created'
WATCH_REPLACED          = 'replaced'
WATCH_UPDATED           = 'updated'
WATCH_DELETED           = 'deleted'
