# encoding=utf8

""" The mongodb repository
    Author: lipixun
    Created Time : 五  3/18 10:34:45 2016

    File Name: repositoryMongodb.py
    Description:

"""

from uuid import uuid4

from datahub.updates import UpdateAction, PushAction, PushsAction, PopAction, SetAction, ClearAction
from datahub.conditions import KeyValueCondition, KeyValuesCondition, ExistCondition, NonExistCondition, GreaterCondition, LesserCondition, \
    AndCondition, OrCondition, NotCondition
from datahub.dataservice.mongodb import MongodbDataStorage

from model import ATestModel, createBigModel, ATestSubModel
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
    modelID = str(uuid4())
    # Create the model
    model = createBigModel()
    model.id = modelID
    print 'Created model:'
    print json.dumps(model.dump(), indent = 4)
    # Create the service
    mongodbDataService = MongodbDataStorage.collection(ATestModel, mongodb.test)
    # Exist
    assert not mongodbDataService.exist("ANotFoundID")
    # Insert into database
    mongodbDataService.create(model)
    assert mongodbDataService.exist(modelID)
    fetchedModel = mongodbDataService.getOne(modelID)
    print 'Fetched model:'
    print json.dumps(fetchedModel.dump(), indent = 4)
    assert fetchedModel == model
    fetchedModels = list(mongodbDataService.gets([ modelID ]))
    assert len(fetchedModels) == 1 and fetchedModels[0] == model
    model.stringType = 'ANewString'
    res = mongodbDataService.replace(model)
    assert mongodbDataService.getOne(modelID) == model
    # Update
    assert mongodbDataService.updateOne(modelID, [
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
    model.stringType = 'UpdatedString1'
    model.listType.append(ATestSubModel(stringType = 'PushedString'))
    model.setType.add('setNewString1')
    model.setType.add('setNewString2')
    del model.anyType
    # Query the model
    assert mongodbDataService.getOne(model.id) == model
    models = list(mongodbDataService.getByQuery(KeyValueCondition(key = 'stringType', value = 'UpdatedString1')))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(KeyValueCondition(key = 'stringType', value = 'UpdatedString2', equals = False)))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(KeyValuesCondition(key = 'stringType', values = [ 'UpdatedString1' ])))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(KeyValuesCondition(key = 'stringType', values = [ 'UpdatedString2' ], includes = False)))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(ExistCondition(key = 'stringType')))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(NonExistCondition(key = 'stringType1')))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(GreaterCondition(key = 'intType', value = 0)))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(GreaterCondition(key = 'intType', value = 1)))
    assert len(models) == 0
    models = list(mongodbDataService.getByQuery(GreaterCondition(key = 'intType', value = 1, equals = True)))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(LesserCondition(key = 'intType', value = 10)))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(LesserCondition(key = 'intType', value = 1)))
    assert len(models) == 0
    models = list(mongodbDataService.getByQuery(LesserCondition(key = 'intType', value = 1, equals = True)))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(AndCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedString1'),
        GreaterCondition(key = 'intType', value = 1, equals = True)
        ])))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(AndCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedString1'),
        GreaterCondition(key = 'intType', value = 10, equals = True)
        ])))
    assert len(models) == 0
    models = list(mongodbDataService.getByQuery(OrCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedString1'),
        GreaterCondition(key = 'intType', value = 10, equals = True)
        ])))
    assert len(models) == 1
    models = list(mongodbDataService.getByQuery(OrCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'UpdatedStringX'),
        GreaterCondition(key = 'intType', value = 10, equals = True)
        ])))
    assert len(models) == 0
    models = list(mongodbDataService.getByQuery(NotCondition(condition = KeyValueCondition(key = 'stringType', value = 'UpdatedString1'))))
    assert len(models) == 0
    # Delete the model
    assert mongodbDataService.deleteOne(modelID)
    assert not mongodbDataService.getOne(modelID)
