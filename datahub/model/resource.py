# encoding=utf8

""" The data model for manager
    Author: lipixun
    Created Time : 三  3/16 16:00:04 2016

    File Name: model.py
    Description:

        NOTE:

            ALL models which intends to be managed by Manager must be derived from the following model classes:

            - Resource

"""

from models import DataModel, IDDataModel
from _types import StringType, FloatType, DatetimeType, DictType, ModelType

class ResourceMetadata(DataModel):
    """The metadata
    """
    # The create time
    createTime = DatetimeType(required = True, doc = 'The create time of the resource')
    # The timestamp
    timestamp = FloatType(required = True, doc = 'The resource change timestamp, including: create / replaced / updated / ...')
    # The expire time
    expireTime = DatetimeType(doc = 'The expire time')
    # The labels
    labels = DictType(StringType())

class Resource(IDDataModel):
    """The resource
    """
    # The metadata
    metadata = ModelType(ResourceMetadata, doc = 'The metadata of the resource')

class ResourceWatchChangeSet(DataModel):
    """The resource watch change set
    """
    # The changeset name
    name = StringType(required = True, doc = 'The changeset name (type)')
    # The changeset timestamp
    timestamp = FloatType(required = True, doc = 'The change timestamp')
    # The model id
    modelID = StringType(doc = 'The related model id')
    # The old model
    oldModel = ModelType(DataModel, doc = 'The old model. This field is set to the model before modification in replace, update action and the deleted model in delete action.')
    # The changeset related model
    newModel = ModelType(DataModel, doc = 'The new model. This field is set to the model after modification in replace, update action.')

    def __str__(self):
        """Convert to string
        """
        return 'Name [%s] TS [%s] ID [%s]' % (self.name, self.timestamp, self.modelID)
