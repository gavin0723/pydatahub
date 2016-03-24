# encoding=utf8

""" The repository result
    Author: lipixun
    Created Time : 六  3/19 23:47:30 2016

    File Name: result.py
    Description:

"""

from datahub.model import DataModel, ModelType, IntegerType, ListType

class ReplaceResult(DataModel):
    """The replace result
    """
    # The model before replaced
    before = ModelType(DataModel, doc = 'The model before replaced')
    # The model after replaced
    after = ModelType(DataModel, doc = 'The model before replaced')

class UpdateResult(DataModel):
    """The update result
    """
    # The model before updated
    before = ModelType(DataModel, doc = 'The model before updated')
    # The model after updated
    after = ModelType(DataModel, doc = 'The model before updated')

class UpdatesResult(DataModel):
    """The updates result
    """
    # The updated models
    #   When in fastUpdate model, this field will be empty or none
    updates = ListType(ModelType(UpdateResult))
    # The updated model count
    #   This field will always be set to the number of updated models
    count = IntegerType(required = True)

class DeletesResult(DataModel):
    """The deletes result
    """
    # The updated models
    #   When in fastDelete model, this field will be empty or none
    models = ListType(ModelType(DataModel))
    # The deleted model count
    #   This field will always be set to the number of updated models
    count = IntegerType(required = True)
