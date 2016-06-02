# encoding=utf8

""" The model object
    Author: lipixun
    Created Time : 五  3/18 10:35:57 2016

    File Name: model.py
    Description:

"""

from sets import Set
from datetime import datetime, date, time, timedelta

from datahub.model import metadata, IDDataModel, DataModel, StringType, IntegerType, FloatType, BooleanType, DatetimeType, DateType, TimeType, TimeDeltaType, \
    ListType, SetType, DictType, ModelType, DynamicModelType, AnyType

class ATestSubModel(DataModel):
    """The sub model
    """
    stringType = StringType()

@metadata(namespace = 'testmodel.a')
class ATestModel(IDDataModel):
    """The test model
    """
    def selectModel(value, model, container):
        """Select the model
        """
        return ATestSubModel

    stringType = StringType()
    intType = IntegerType()
    floatType = FloatType()
    boolType = BooleanType()
    datetimeType = DatetimeType()
    dateType = DateType()
    timeType = TimeType()
    timedeltaType = TimeDeltaType()
    listType = ListType(ModelType(ATestSubModel))
    setType = SetType(StringType())
    dictType = DictType(ModelType(ATestSubModel))
    modelType = ModelType(ATestSubModel)
    dynamicModelType = DynamicModelType(selectModel)
    anyType = AnyType()
    defaultType = StringType(required = True, default = 'defaultValue')
    requiredType = StringType(required = True)
    defaultType2 = StringType(default = 'defaultValue2')

def createBigModel():
    """Create a new big model
    """
    now = datetime.now()
    # NOTE: We have datetime precision problem in mongodb
    now = datetime(year = now.year, month = now.month, day = now.day, hour = now.hour, minute = now.minute, second = now.second)
    model = ATestModel()
    model.stringType = 'astring'
    model.intType = 1
    model.floatType = 1.0
    model.boolType = True
    model.datetimeType = now
    model.dateType = now.date()
    model.timeType = now.time()
    model.timedeltaType = timedelta(seconds = 10)
    model.listType = [ ATestSubModel(stringType = 'bstring') ]
    model.setType = Set([ '1234' ])
    model.dictType = { 'key': ATestSubModel(stringType = 'cstring') }
    model.modelType = ATestSubModel(stringType = 'dstring')
    model.dynamicModelType = ATestSubModel(stringType = 'estring')
    model.anyType = { 'key': [ { 'akey': 'value1' }, { 'akey': 'value2' }, { 'akey': 'value3' } ] }
    model.requiredType = 'requiredValue'
    # Done
    return model

@metadata(namespace = 'testresource')
class TestResource(IDDataModel):
    """The test model
    """
    # The name
    name = StringType(required = True)
    # The author
    author = StringType()
    # The articals
    articals = ListType(StringType())
