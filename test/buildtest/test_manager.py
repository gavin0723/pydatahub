# encoding=utf8

""" The manager
    Author: lipixun
    Created Time : 一  3/21 16:51:38 2016

    File Name: test_manager.py
    Description:

"""

from uuid import uuid4
from time import sleep
from datetime import datetime
from threading import Thread

from datahub.manager import DataManager, WATCH_PRESERVED, WATCH_CREATED, WATCH_REPLACED, WATCH_UPDATED, WATCH_DELETED
from datahub.adapters.repository import MongodbRepository
from datahub.sorts import SortRule
from datahub.updates import SetAction, PushAction, PopAction
from datahub.conditions import KeyValueCondition, KeyValuesCondition

from model import TestResource

def test_basic():
    """The basic test
    """
    manager = DataManager(MongodbRepository(TestResource, mongodb))
    # Init
    now = datetime.now()
    _id = str(uuid4())
    # Create a resource
    resource = manager.create(TestResource(_id = _id, name = 'test'))
    assert not resource.metadata is None and resource.metadata.createTime > now
    # Count
    assert manager.count() == 1
    assert manager.countByID([ _id ]) == 1
    assert manager.countByQuery(KeyValueCondition(key = '_id', value = _id)) == 1
    # Check a resource
    assert manager.existByID(_id)
    assert manager.existsByID([ _id ])
    assert manager.existsByQuery(KeyValueCondition(key = '_id', value = _id))
    # Get a resource
    resource = manager.getByID(_id)
    assert resource and resource.id == _id and resource.name == 'test'
    resources = list(manager.getsByID([ _id ]))
    assert resources and len(resources) == 1 and resources[0].id == _id and resources[0].name == 'test'
    resources = list(manager.getsByQuery(KeyValueCondition(key = '_id', value = _id)))
    assert resources and len(resources) == 1 and resources[0].id == _id and resources[0].name == 'test'
    resources = list(manager.gets(sorts = [ SortRule(key = 'name') ]))
    assert resources and len(resources) == 1 and resources[0].id == _id and resources[0].name == 'test'
    # Replace the resource
    res = manager.replace(TestResource(_id = _id, name = 'newname'))
    assert res.after.id == _id and res.after.name == 'newname'
    # Update the resource
    manager.updateByID(_id, [ SetAction(key = 'name', value = 'newname2'), SetAction(key = 'articals', value = []) ])
    assert manager.getByID(_id).name == 'newname2'
    assert manager.updatesByID([ _id ], [ PushAction(key = 'articals', value = 'artical1') ]).count == 1
    assert manager.getByID(_id).articals[0] == 'artical1'
    assert manager.updatesByQuery(KeyValueCondition(key = '_id', value = _id), [ PopAction(key = 'articals') ]).count == 1
    assert not manager.getByID(_id).articals
    # Delete the resource
    resource = manager.deleteByID(_id)
    assert resource and resource.id == _id and resource.name == 'newname2'
    assert not manager.existByID(_id)
    assert manager.create(TestResource(_id = _id, name = 'test')).id == _id
    assert manager.deletesByID([ _id ]).count == 1
    assert not manager.existByID(_id)
    assert manager.create(TestResource(_id = _id, name = 'test')).id == _id
    assert manager.deletesByQuery(KeyValueCondition(key = '_id', value = _id)).count == 1
    assert not manager.existByID(_id)

def test_watch():
    """Test watch
    """
    manager = DataManager(MongodbRepository(TestResource, mongodb))
    def ops():
        """The manager ops thread
        """
        sleep(2)        # In order to let the main thread run first, sleep 2s
        manager.create(TestResource(name = 'test1', author = 'Jim'))                    # 1 change set
        manager.create(TestResource(name = 'test2', author = 'Jim'))                    # 1 change set
        manager.create(TestResource(_id = '1', name = 'test3', author = 'Alice'))       # 1 change set
        manager.replace(TestResource(_id = '1', name = 'test31', author = 'Alice'))     # 1 change set
        manager.updateByID('1', [ SetAction(key = 'name', value = 'test32') ])          # 1 change set
        manager.updatesByID([ '1' ], [ SetAction(key = 'name', value = 'test33') ])     # 1 change set
        manager.updatesByQuery(KeyValueCondition(key = 'author', value = 'Jim'), [ SetAction(key = 'name', value = 'updates') ])
                                                                                        # 3 change sets (Update three models)
        manager.create(TestResource(_id = '2', name = 'test4', author = 'John'))        # 1 change set
        manager.create(TestResource(_id = '3', name = 'test5', author = 'Frank'))       # 1 change set
        manager.create(TestResource(_id = '4', name = 'test6', author = 'Jack'))        # 1 change set
        manager.deleteByID('1')                                                         # 1 change set
        manager.deletesByID([ '1', '2', '3', '4' ])                                     # 3 change sets
        manager.deletesByQuery(KeyValueCondition(key = 'author', value = 'Jim'))        # 3 change sets (Delete three models)
    # Init
    manager.create(TestResource(_id = '0', name = 'test0', author = 'Jim'))             # 1 change set
    # Create the ops thread
    t1 = Thread(target = ops)
    # Start the thread
    t1.start()
    # Start watching
    changeSets = []
    count = 0
    for changeSet in manager.watch():
        print changeSet
        changeSets.append(changeSet)
        count += 1
        if count >= 20:
            break
    assert len(changeSets) == 20
    assert changeSets[0].name == WATCH_PRESERVED and changeSets[0].modelID == '0'
    assert changeSets[1].name == WATCH_CREATED and changeSets[1].newModel.name == 'test1'
    assert changeSets[2].name == WATCH_CREATED and changeSets[2].newModel.name == 'test2'
    assert changeSets[3].name == WATCH_CREATED and changeSets[3].newModel.name == 'test3'
    assert changeSets[4].name == WATCH_REPLACED and changeSets[4].newModel.name == 'test31'
    #assert changeSets[5].name == WATCH_UPDATED and changeSets[5].model.name == 'test32'
    #assert changeSets[6].name == WATCH_UPDATED and changeSets[6].model.name == 'test33'
    #assert changeSets[7].name == WATCH_UPDATED and changeSets[7].model.name == 'updates'
    #assert changeSets[8].name == WATCH_UPDATED and changeSets[8].model.name == 'updates'
    #assert changeSets[9].name == WATCH_UPDATED and changeSets[9].model.name == 'updates'
    assert changeSets[10].name == WATCH_CREATED and changeSets[10].modelID == '2'
    assert changeSets[11].name == WATCH_CREATED and changeSets[11].modelID == '3'
    assert changeSets[12].name == WATCH_CREATED and changeSets[12].modelID == '4'
    assert changeSets[13].name == WATCH_DELETED and changeSets[13].oldModel.name == 'test33'
    assert changeSets[14].name == WATCH_DELETED and changeSets[14].oldModel.name == 'test4'
    assert changeSets[15].name == WATCH_DELETED and changeSets[15].oldModel.name == 'test5'
    assert changeSets[16].name == WATCH_DELETED and changeSets[16].oldModel.name == 'test6'
    assert changeSets[17].name == WATCH_DELETED and changeSets[17].oldModel.name == 'updates'
    assert changeSets[18].name == WATCH_DELETED and changeSets[18].oldModel.name == 'updates'
    assert changeSets[19].name == WATCH_DELETED and changeSets[19].oldModel.name == 'updates'
    t1.join()
    # Condition watch
    # Init
    manager.create(TestResource(_id = '0', name = 'test0', author = 'Jim'))             # 1 change set
    # Create the ops thread
    t2 = Thread(target = ops)
    # Start the thread
    t2.start()
    # Start watching
    changeSets = []
    count = 0
    for changeSet in manager.watch(KeyValuesCondition(key = 'author', values = [ 'Jim', 'Alice', 'Jack' ])):
        # Jim has 9 change sets
        # Alice has 5 change sets
        # Jack has 2 change sets
        print changeSet
        changeSets.append(changeSet)
        count += 1
        if count >= 16:
            break
    assert len(changeSets) == 16
    assert changeSets[0].name == WATCH_PRESERVED and changeSets[0].modelID == '0'
    assert changeSets[1].name == WATCH_CREATED and changeSets[1].newModel.name == 'test1'
    assert changeSets[2].name == WATCH_CREATED and changeSets[2].newModel.name == 'test2'
    assert changeSets[3].name == WATCH_CREATED and changeSets[3].newModel.name == 'test3'
    assert changeSets[4].name == WATCH_REPLACED and changeSets[4].newModel.name == 'test31'
    #assert changeSets[5].name == WATCH_UPDATED and changeSets[5].model.name == 'test32'
    #assert changeSets[6].name == WATCH_UPDATED and changeSets[6].model.name == 'test33'
    #assert changeSets[7].name == WATCH_UPDATED and changeSets[7].model.name == 'updates'
    #assert changeSets[8].name == WATCH_UPDATED and changeSets[8].model.name == 'updates'
    #assert changeSets[9].name == WATCH_UPDATED and changeSets[9].model.name == 'updates'
    assert changeSets[10].name == WATCH_CREATED and changeSets[10].modelID == '4'
    assert changeSets[11].name == WATCH_DELETED and changeSets[11].oldModel.name == 'test33'
    assert changeSets[12].name == WATCH_DELETED and changeSets[12].oldModel.name == 'test6'
    assert changeSets[13].name == WATCH_DELETED and changeSets[13].oldModel.name == 'updates'
    assert changeSets[14].name == WATCH_DELETED and changeSets[14].oldModel.name == 'updates'
    assert changeSets[15].name == WATCH_DELETED and changeSets[15].oldModel.name == 'updates'
    t2.join()
