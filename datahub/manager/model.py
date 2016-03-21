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

from datahub.model import DataModel, IDDataModel, StringType, FloatType, DatetimeType, DictType, ModelType

class Metadata(DataModel):
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
    metadata = ModelType(Metadata, required = True, doc = 'The metadata of the resource')

class ResourceWatchChangeSet(DataModel):
    """The resource watch change set
    """
    # The changeset name
    name = StringType(required = True, doc = 'The changeset name (type)')
    # The changeset timestamp
    timestamp = FloatType(required = True, doc = 'The change timestamp')
    # The model id
    modelID = StringType(doc = 'The related model id')
    # The changeset related model
    model = ModelType(DataModel, doc = 'The related model')
