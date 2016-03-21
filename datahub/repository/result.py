# encoding=utf8

""" The repository result
    Author: lipixun
    Created Time : 六  3/19 23:47:30 2016

    File Name: result.py
    Description:

"""

from datahub.model import DataModel, ModelType, IntegerType, ListType

class UpdatesResult(DataModel):
    """The updates result
    """
    # The updated models
    #   When in fastUpdate model, this field will be empty or none
    models = ListType(ModelType(DataModel))
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
