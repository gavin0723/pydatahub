# encoding=utf8

""" Test web restful service
    Author: lipixun
    Created Time : 二  5/31 16:33:38 2016

    File Name: test_web_restful.py
    Description:

"""

import mime

from webtest import TestApp

from unifiedrpc import Server, CONFIG_RESPONSE_MIMETYPE, CONFIG_RESPONSE_CONTENT_CONTAINER
from unifiedrpc.adapters.web import WebAdapter
from unifiedrpc.content.container import APIContentContainer

from datahub.utils import json
_json = json
from datahub.errors import ModelNotFoundError, DuplicatedKeyError
from datahub.updates import UpdateAction, PushAction, PushsAction, PopAction, SetAction, ClearAction
from datahub.conditions import KeyValueCondition, KeyValuesCondition, ExistCondition, NonExistCondition, GreaterCondition, LesserCondition, \
    AndCondition, OrCondition, NotCondition
from datahub.adapters.repository import MongodbRepository
from datahub.adapters.web.restful import ResourceService, ResourceLocation, Connection, ResourceConnector

from model import ATestModel, createBigModel, ATestSubModel

def test_web_restful_basic():
    """Test web restful
    """
    # Create adapter and test application
    adapter = WebAdapter()
    webTestApp = TestApp(adapter)
    # Create web app
    server = Server([ ResourceService(MongodbRepository(ATestModel, mongodb), [ ResourceLocation('/') ]) ], [ adapter ], {
        CONFIG_RESPONSE_MIMETYPE: mime.APPLICATION_JSON,
        CONFIG_RESPONSE_CONTENT_CONTAINER: APIContentContainer,
        })
    server.start()
    # Run test on webTestApp
    # Empty
    assert webTestApp.head('/', expect_errors = True).status_int == 404
    assert webTestApp.head('/1', expect_errors = True).status_int == 404
    assert json.loads(webTestApp.get('/').body)['value'] == []
    assert webTestApp.get('/1', expect_errors = True).status_int == 404
    assert json.loads(webTestApp.patch_json('/1', params = { 'updates': [ SetAction(key = 'stringType', value = 'test0').dump() ] }).body)['value'] == 0
    assert json.loads(webTestApp.delete('/1').body)['value'] == 0
    # Create
    model = createBigModel()
    webTestApp.post_json('/', params = { 'model': model.dump() })
    # Exist
    webTestApp.head('/')
    webTestApp.head('/%s' % model.id)
    models = json.loads(webTestApp.get('/').body)['value']
    # Get
    assert models and len(models) == 1 and ATestModel.load(models[0]) == model
    assert ATestModel.load(json.loads(webTestApp.get('/%s' % model.id).body)['value']) == model
    models = json.loads(webTestApp.post_json('/_query', params = { 'query': KeyValueCondition(key = '_id', value = model.id).dump() }).body)['value']
    assert models and len(models) == 1 and ATestModel.load(models[0]) == model
    # Update
    assert json.loads(webTestApp.patch_json(
        '/%s' % model.id,
        params = { 'updates': [ x.dump() for x in (SetAction(key = 'stringType', value = 'test0'), SetAction(key = 'intType', value = 10)) ] }
        ).body)['value'] == 1
    fetchedModel = ATestModel.load(json.loads(webTestApp.get('/%s' % model.id).body)['value'])
    assert fetchedModel.stringType == 'test0' and fetchedModel.intType == 10
    assert json.loads(webTestApp.patch_json(
        '/',
        params = {
            'query': KeyValueCondition(key = '_id', value = model.id).dump(),
            'updates': [ x.dump() for x in (SetAction(key = 'stringType', value = 'test1'), SetAction(key = 'intType', value = 11)) ]
            }
        ).body)['value'] == 1
    fetchedModel = ATestModel.load(json.loads(webTestApp.get('/%s' % model.id).body)['value'])
    assert fetchedModel.stringType == 'test1' and fetchedModel.intType == 11
    # Replace
    fetchedModel.intType = 101010
    json.loads(webTestApp.put_json('/', params = { 'model': fetchedModel.dump() }).body)['value']
    fetchedModel = ATestModel.load(json.loads(webTestApp.get('/%s' % model.id).body)['value'])
    assert fetchedModel.intType == 101010
    # Delete
    assert json.loads(webTestApp.delete('/%s' % model.id).body)['value'] == 1
    # Create a new one, test delete by query
    webTestApp.post_json('/', params = { 'model': model.dump() })
    assert json.loads(webTestApp.delete_json('/', params = { 'query': KeyValueCondition(key = '_id', value = model.id).dump() }).body)['value'] == 1

class WebTestConnection(Connection):
    """The web test connection
    """
    def __init__(self, app):
        """Create a new WebTestConnection
        """
        self.app = app

    def request(self, method, path, json = None, **kwargs):
        """Send a request
        Parameters:
            method                                  The request method
            path                                    The request path
            json                                    The json object to send as payload
        Returns:
            requests.Response object
        """
        path = path or '/'
        # Do json serialize if json
        if json and 'data' in kwargs:
            raise ValueError('Cannot set json and data at the same time')
        # Set body
        if json:
            body = _json.dumps(json, ensure_ascii = False).encode('utf8')
            if kwargs.get('headers'):
                kwargs['headers']['Content-Type'] = 'application/json; charset=utf-8'
            else:
                kwargs['headers'] = { 'Content-Type': 'application/json; charset=utf-8' }
        elif 'data' in kwargs:
            body = kwargs.pop('data')
        else:
            body = None
        kwargs['body'] = body
        # Send
        rsp = self.app.request(path, method = method, expect_errors = True, **kwargs)
        rsp.content = rsp.body
        rsp.raise_for_status = lambda: self.raise_for_status(rsp)
        return rsp

    @classmethod
    def raise_for_status(cls, rsp):
        """Raise for status
        """
        raise AssertionError('Http status code [%s]' % rsp.status_int)

def test_web_restful_basic_connector():
    """Test web restful
    """
    # Create adapter and test application
    adapter = WebAdapter()
    webTestApp = TestApp(adapter)
    # Create web app
    server = Server([ ResourceService(MongodbRepository(ATestModel, mongodb), [ ResourceLocation('/') ]) ], [ adapter ], {
        CONFIG_RESPONSE_MIMETYPE: mime.APPLICATION_JSON,
        CONFIG_RESPONSE_CONTENT_CONTAINER: APIContentContainer,
        })
    server.start()
    # Create connector
    connector = ResourceConnector(ATestModel, WebTestConnection(webTestApp))
    # Start test
    # Empty
    assert not connector.exist('/', id = '1')
    assert not connector.exist('/', query = KeyValueCondition(key = '_id', value = '1'))
    assert connector.get('/', id = '1') is None
    assert len(connector.get('/', query = KeyValueCondition(key = '_id', value = '1'))) == 0
    try:
        connector.replace('/', createBigModel())
        raise AssertionError
    except ModelNotFoundError:
        pass
    assert connector.update('/', [ SetAction(key = 'intType', value = 0) ], id = '1') == 0
    assert connector.update('/', [ SetAction(key = 'intType', value = 0) ], query = KeyValueCondition(key = '_id', value = '1')) == 0
    assert connector.delete('/', id = '1') == 0
    assert connector.delete('/', query = KeyValueCondition(key = '_id', value = '1')) == 0
    assert connector.count('/') == 0
    assert connector.count('/', id = '1') == 0
    assert connector.count('/', query = KeyValueCondition(key = '_id', value = '1')) == 0
    # Create
    model = createBigModel()
    connector.create('/', model)
    assert connector.exist('/', id = model.id)
    assert connector.exist('/', query = KeyValueCondition(key = '_id', value = model.id))
    assert connector.count('/') == 1
    assert connector.count('/', id = model.id) == 1
    assert connector.count('/', query = KeyValueCondition(key = '_id', value = model.id)) == 1
    assert connector.get('/', id = model.id) == model
    models = connector.get('/', query = KeyValueCondition(key = '_id', value = model.id))
    assert len(models) == 1 and models[0] == model
    model.intType = 100
    connector.replace('/', model)
    assert connector.get('/', id = model.id).intType == 100
    assert connector.update('/', [ SetAction(key = 'intType', value = 1000) ], id = model.id) == 1
    assert connector.get('/', id = model.id).intType == 1000
    assert connector.update('/', [ SetAction(key = 'intType', value = 10000) ], query = KeyValueCondition(key = '_id', value = model.id)) == 1
    assert connector.get('/', id = model.id).intType == 10000
    # Delete
    assert connector.delete('/', id = model.id) == 1
    # Create & Delete
    model = createBigModel()
    connector.create('/', model)
    try:
        connector.create('/', model)
        raise AssertionError
    except DuplicatedKeyError:
        pass
    connector.create('/', model, configs = { 'overwrite': True })
    connector.create('/', createBigModel())
    connector.create('/', createBigModel())
    assert connector.count('/') == 3
    assert connector.count('/', id = model.id) == 1
    assert connector.count('/', query = KeyValueCondition(key = '_id', value = model.id)) == 1
    connector.replace('/', createBigModel(), configs = { 'autoCreate': True })
    # Delete
    assert connector.delete('/', query = KeyValueCondition(key = '_id', value = model.id)) == 1
    assert connector.count('/') == 3
