# encoding=utf8

""" The model test script
    Author: lipixun
    Created Time : 二  3/15 00:11:42 2016

    File Name: model.py
    Description:

"""

from sets import Set
from datetime import datetime, date, time, timedelta

from datahub.errors import MissingRequiredFieldError
from datahub.conditions import *

from model import ATestModel, ATestSubModel, createBigModel
from utils import json

def test_model_basic():
    """Test the model basic
    """
    model = createBigModel()
    now = model.datetimeType
    # Validate
    model.validate()
    # Delete the required value
    value = model.requiredType
    del model.requiredType
    try:
        model.validate()
        raise AssertionError
    except MissingRequiredFieldError:
        pass
    model.requiredType = value
    model.validate()
    # Assert get attributes
    assert model.stringType == 'astring'
    assert model.intType == 1
    assert model.floatType == 1.0
    assert model.boolType == True
    assert model.datetimeType == now
    assert model.dateType == now.date()
    assert model.timeType == now.time()
    assert model.listType == [ ATestSubModel(stringType = 'bstring') ]
    assert model.setType == Set([ '1234' ])
    assert model.dictType == { 'key': ATestSubModel(stringType = 'cstring') }
    assert model.modelType == ATestSubModel(stringType = 'dstring')
    assert model.dynamicModelType == ATestSubModel(stringType = 'estring')
    assert model.anyType == { 'key': [ { 'akey': 'value1' }, { 'akey': 'value2' }, { 'akey': 'value3' } ] }
    assert model.defaultType == 'defaultValue'
    assert model.defaultType2 == 'defaultValue2'
    assert model.requiredType == 'requiredValue'
    # Set new value
    model.defaultType = '_defaultValue'
    model.defaultType2 = '_defaultValue2'
    model.requiredType = '_requiredValue'
    assert model.defaultType == '_defaultValue'
    assert model.defaultType2 == '_defaultValue2'
    assert model.requiredType == '_requiredValue'
    # Dump
    dumpValue = model.dump()
    print json.dumps(dumpValue, ensure_ascii = False, indent = 4)
    # Load dump
    newModel = ATestModel(dumpValue)
    assert newModel.defaultType == '_defaultValue'
    assert newModel.defaultType2 == '_defaultValue2'
    assert newModel.requiredType == '_requiredValue'
    assert model == newModel
    # Check not equal
    newModel.defaultType = 'A new value'
    assert model != newModel
    # Delete
    del model.stringType
    assert model.stringType == None

def test_model_query():
    """Test the model query
    """
    model = createBigModel()
    model.validate()
    # Query
    res = list(model.query('stringType'))
    assert len(res) == 1 and res[0] == 'astring'
    res = list(model.query('dictType.key.stringType'))
    assert len(res) == 1 and res[0] == 'cstring'
    res = list(model.query('listType.stringType'))
    assert len(res) == 1 and res[0] == 'bstring'
    res = list(model.query('modelType.stringType'))
    assert len(res) == 1 and res[0] == 'dstring'
    res = list(model.query('dynamicModelType.stringType'))
    assert len(res) == 1 and res[0] == 'estring'
    res = list(model.query('anyType.key.akey'))
    assert len(res) == 3 and res == [ 'value1', 'value2', 'value3' ]

def test_model_condition():
    """The the model condition
    """
    model = createBigModel()
    model.validate()
    # Match
    assert model.match(KeyValueCondition(key = 'intType', value = 1))
    assert model.match(KeyValuesCondition(key = 'stringType', values = [ 'astring', 'aaaaa' ]))
    assert not model.match(KeyValuesCondition(key = 'stringType', values = [ 'aaaa' ]))
    assert model.match(NotCondition(condition = KeyValuesCondition(key = 'stringType', values = [ 'aaaa' ])))
    assert model.match(AndCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'astring'),
        KeyValueCondition(key = 'modelType.stringType', value = 'dstring')
        ]))
    assert model.match(AndCondition(conditions = [
        KeyValueCondition(key = 'stringType', value = 'astring'),
        KeyValueCondition(key = 'modelType.stringType', value = 'dstring'),
        OrCondition(conditions = [
            NonExistCondition(key = 'asas')
            ])
        ]))
    assert model.match(ExistCondition(key = 'anyType.key.akey'))
    assert model.match(KeyValueCondition(key = 'anyType.key.akey', value = 'value1'))
    assert model.match(GreaterCondition(key = 'floatType', value = 1.0, equals = True))
    assert model.match(GreaterCondition(key = 'floatType', value = 0.9))
    assert model.match(LesserCondition(key = 'floatType', value = 1.0, equals = True))
    assert model.match(LesserCondition(key = 'floatType', value = 1.1))
