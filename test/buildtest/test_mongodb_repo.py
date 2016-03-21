# encoding=utf8

""" The mongodb repository
    Author: lipixun
    Created Time : 五  3/18 10:34:45 2016

    File Name: repositoryMongodb.py
    Description:

"""

from uuid import uuid4

from datahub.updates import UpdateAction, PushAction, PushsAction, PopAction, SetAction, ClearAction
from datahub.conditions import KeyValueCondition, KeyValuesCondition, ExistCondition, NonExistCondition, LargerCondition, SmallerCondition, \
    AndCondition, OrCondition, NotCondition
from datahub.adapters.repository import MongodbRepository

from model import ATestModel, createBigModel
from utils import json

def test_mongodb_repository_basic():
    """Test the mongodb repository:
        - create
        - existByID
        - getByID
        - getByQuery
        - replace
        - update
        - deleteByID
    """
    repo = MongodbRepository(ATestModel, mongodb)
    modelID = str(uuid4())
    # Create the model
    model = createBigModel()
    model.id = modelID
    print 'Created model:'
    print json.dumps(model.dump(), indent = 4)
    # Insert into database
    assert not repo.create(model) is None
    assert repo.existByID(modelID)
    fetchedModel = repo.getByID(modelID)
    print 'Fetched model:'
    print json.dumps(fetchedModel.dump(), indent = 4)
    assert fetchedModel == model
    model.stringType = 'ANewString'
    assert repo.replace(model) == model
    assert repo.getByID(modelID) == model
    # Update
    assert repo.updateByID(modelID, [
        SetAction(key = 'stringType', value = 'UpdatedString1'),
        # NOTE: Set values to objects in array is not supported now for mongodb
        #SetAction(key = 'listType.stringType', value = 'UpdatedString2'),
        PushAction(key = 'listType', value = { 'stringType': 'PushedString' }),
        PushsAction(key = 'setType', values = [ 'setNewString1', 'setNewString2' ]),
        ClearAction(key = 'anyType'),
        # NOTE: Mongodb do not support update one field multiple times
        #PopAction(key = 'listType', head = True)
        ])
    # Update the model object
    # Query the model
    models = list(repo.getsByQuery(KeyValueCondition(key = 'stringType', value = 'UpdatedString1')))
    assert len(models) == 1
    models = list(repo.getsByQuery(KeyValueCondition(key = 'stringType', value = 'UpdatedString2', equals = False)))
    assert len(models) == 1
    models = list(repo.getsByQuery(KeyValuesCondition(key = 'stringType', values = [ 'UpdatedString1' ])))
    assert len(models) == 1
    models = list(repo.getsByQuery(KeyValuesCondition(key = 'stringType', values = [ 'UpdatedString2' ], includes = False)))
    assert len(models) == 1
    models = list(repo.getsByQuery(ExistCondition(key = 'stringType')))
    assert len(models) == 1
    models = list(repo.getsByQuery(NonExistCondition(key = 'stringType1')))
    assert len(models) == 1
    models = list(repo.getsByQuery(LargerCondition(key = 'intType', value = 0)))
    assert len(models) == 1
    models = list(repo.getsByQuery(LargerCondition(key = 'intType', value = 1)))
    assert len(models) == 0
    models = list(repo.getsByQuery(LargerCondition(key = 'intType', value = 1, equals = True)))
    assert len(models) == 1
    models = list(repo.getsByQuery(SmallerCondition(key = 'intType', value = 10)))
    assert len(models) == 1
    models = list(repo.getsByQuery(SmallerCondition(key = 'intType', value = 1)))
    assert len(models) == 0
    models = list(repo.getsByQuery(SmallerCondition(key = 'intType', value = 1, equals = True)))
    assert len(models) == 1
    models = list(repo.getsByQuery(AndCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedString1'),
        LargerCondition(key = 'intType', value = 1, equals = True)
        ])))
    assert len(models) == 1
    models = list(repo.getsByQuery(AndCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedString1'),
        LargerCondition(key = 'intType', value = 10, equals = True)
        ])))
    assert len(models) == 0
    models = list(repo.getsByQuery(OrCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedString1'),
        LargerCondition(key = 'intType', value = 10, equals = True)
        ])))
    assert len(models) == 1
    models = list(repo.getsByQuery(OrCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedStringX'),
        LargerCondition(key = 'intType', value = 10, equals = True)
        ])))
    assert len(models) == 0
    models = list(repo.getsByQuery(NotCondition(condition = KeyValueCondition(key = 'stringType', value = 'UpdatedString1'))))
    assert len(models) == 0
    # Delete the model
    assert repo.deleteByID(modelID)
    assert not repo.getByID(modelID)
