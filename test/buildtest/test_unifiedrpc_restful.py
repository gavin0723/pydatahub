# encoding=utf8

""" Test web restful service
    Author: lipixun
    Created Time : 二  5/31 16:33:38 2016

    File Name: test_web_restful.py
    Description:

"""

import mime

from urllib import urlencode

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
from datahub.dataservice.mongodb import MongodbDataStorage
from datahub.dataservice.unifiedrpc.client import RestfulWebClient
from datahub.dataservice.unifiedrpc.service import RestfulWebService

from model import ATestModel, createBigModel, ATestSubModel

def test_unifiedrpc_restful_webservice():
    """Test unifiedrpc restful web service
    """
    # Create adapter and test application
    adapter = WebAdapter()
    webTestApp = TestApp(adapter)
    # Create web app
    server = Server([ RestfulWebService(ATestModel, MongodbDataStorage.collection(ATestModel, mongodb.testunifiedrpcrestful)) ], [ adapter ], {
        CONFIG_RESPONSE_MIMETYPE: mime.APPLICATION_JSON,
        CONFIG_RESPONSE_CONTENT_CONTAINER: APIContentContainer,
        })
    server.start()
    # Run test on webTestApp
    # Empty
    assert webTestApp.head("/1", expect_errors = True).status_int == 404
    assert webTestApp.get("/1", expect_errors = True).status_int == 404
    assert webTestApp.patch_json("/1", params = { "updates": [ SetAction(key = "stringType", value = "test0").dump() ] }, expect_errors = True).status_int == 404
    assert webTestApp.delete("/1", expect_errors = True).status_int == 404
    assert json.loads(webTestApp.post("/_counts").body)["value"] == 0
    # Create
    model = createBigModel()
    webTestApp.post_json("/", params = { "model": model.dump() })
    # Count
    assert json.loads(webTestApp.post("/_counts").body)["value"] == 1
    # Exist
    webTestApp.head("/%s" % model.id)
    # Get
    assert ATestModel.load(json.loads(webTestApp.get("/%s" % model.id).body)["value"]) == model
    # Gets
    models = [ ATestModel.load(x) for x in json.loads(webTestApp.post_json("/_gets", params = { "ids": [ model.id ] }).body)["value"] ]
    assert len(models) == 1 and models[0] == model
    # Get by query
    models = [ ATestModel.load(x) for x in json.loads(webTestApp.post_json("/_getbyquery", params = { "query": KeyValueCondition(key = "_id", value = model.id).dump() }).body)["value"] ]
    assert len(models) == 1 and models[0] == model
    # Update one
    assert json.loads(webTestApp.patch_json(
        "/%s" % model.id,
        params = { "updates": [ x.dump() for x in (SetAction(key = "stringType", value = "test0"), SetAction(key = "intType", value = 10)) ] }
        ).body)["value"] == True
    fetchedModel = ATestModel.load(json.loads(webTestApp.get("/%s" % model.id).body)["value"])
    assert fetchedModel.stringType == "test0" and fetchedModel.intType == 10
    # Update ids
    assert json.loads(webTestApp.patch_json(
        "/_updates",
        params = {
            "ids": [ model.id ],
            "updates": [ x.dump() for x in (SetAction(key = "stringType", value = "test1"), SetAction(key = "intType", value = 11)) ]
            }
        ).body)["value"] == 1
    fetchedModel = ATestModel.load(json.loads(webTestApp.get("/%s" % model.id).body)["value"])
    assert fetchedModel.stringType == "test1" and fetchedModel.intType == 11
    # Update query
    assert json.loads(webTestApp.patch_json(
        "/_updatebyquery",
        params = {
            "query": KeyValueCondition(key = "_id", value = model.id).dump(),
            "updates": [ x.dump() for x in (SetAction(key = "stringType", value = "test2"), SetAction(key = "intType", value = 12)) ]
            }
        ).body)["value"] == 1
    fetchedModel = ATestModel.load(json.loads(webTestApp.get("/%s" % model.id).body)["value"])
    assert fetchedModel.stringType == "test2" and fetchedModel.intType == 12
    # Replace
    fetchedModel.intType = 101010
    json.loads(webTestApp.put_json("/", params = { "model": fetchedModel.dump() }).body)["value"]
    fetchedModel = ATestModel.load(json.loads(webTestApp.get("/%s" % model.id).body)["value"])
    assert fetchedModel.intType == 101010
    # Count by query
    assert json.loads(webTestApp.post_json("/_countbyquery", params = { "query": KeyValueCondition(key = "intType", value = 101010).dump() }).body)["value"] == 1
    # Delete
    assert json.loads(webTestApp.delete("/%s" % model.id).body)["value"] == True
    # Create a new one, test delete by query
    webTestApp.post_json("/", params = { "model": model.dump() })
    assert json.loads(webTestApp.post_json("/_deletebyquery", params = { "query": KeyValueCondition(key = "_id", value = model.id).dump() }).body)["value"] == 1
    assert json.loads(webTestApp.post_json("/_countbyquery", params = { "query": KeyValueCondition(key = "intType", value = 101010).dump() }).body)["value"] == 0

class WebTestSession(object):
    """The web test connection
    """
    def __init__(self, app):
        """Create a new WebTestSession
        """
        self.app = app

    def head(self, path, **kwargs):
        """Head
        """
        return self.request("HEAD", path, **kwargs)

    def get(self, path, **kwargs):
        """Get
        """
        return self.request("GET", path, **kwargs)

    def post(self, path, **kwargs):
        """Post
        """
        return self.request("POST", path, **kwargs)

    def put(self, path, **kwargs):
        """Post
        """
        return self.request("PUT", path, **kwargs)

    def delete(self, path, **kwargs):
        """Delete
        """
        return self.request("DELETE", path, **kwargs)

    def patch(self, path, **kwargs):
        """Patch
        """
        return self.request("PATCH", path, **kwargs)

    def request(self, method, path, **kwargs):
        """Send a request
        Parameters:
            method                                  The request method
            path                                    The request path
            json                                    The json object to send as payload
        Returns:
            requests.Response object
        """
        path = path or "/"
        if "data" in kwargs:
            kwargs["body"] = kwargs.pop("data")
        if "params" in kwargs:
            params = kwargs.pop("params")
            if params:
                path = "%s?%s" % (path, urlencode(params, doseq = True))
        # Send
        rsp = self.app.request(path, method = method, expect_errors = True, **kwargs)
        rsp.content = rsp.body
        rsp.raise_for_status = lambda: self.raise_for_status(rsp)
        return rsp

    @classmethod
    def raise_for_status(cls, rsp):
        """Raise for status
        """
        raise AssertionError("Http status code [%s]" % rsp.status_int)

def test_unifiedrpc_restful_webclient():
    """Test unifiedrpc restful web client
    """
    # Create adapter and test application
    adapter = WebAdapter()
    webTestApp = TestApp(adapter)
    # Create web app
    server = Server([ RestfulWebService(ATestModel, MongodbDataStorage.collection(ATestModel, mongodb.testunifiedrpcrestful)) ], [ adapter ], {
        CONFIG_RESPONSE_MIMETYPE: mime.APPLICATION_JSON,
        CONFIG_RESPONSE_CONTENT_CONTAINER: APIContentContainer,
        })
    server.start()
    # Create client
    client = RestfulWebClient("", ATestModel, WebTestSession(webTestApp))
    # Start test
    # Empty
    assert not client.exist("1")
    assert client.getOne("1") is None
    assert not client.getByQuery(KeyValueCondition(key = "_id", value = "1"))
    try:
        client.replace(createBigModel())
        raise AssertionError
    except ModelNotFoundError:
        pass
    assert client.updateOne("1", [ SetAction(key = "intType", value = 0) ]) == False
    assert client.updates([ "1" ], [ SetAction(key = "intType", value = 0) ]) == 0
    assert client.updateByQuery(KeyValueCondition(key = "_id", value = "1"), [ SetAction(key = "intType", value = 0) ]) == 0
    assert client.deleteOne("1") == False
    assert client.deletes([ "1" ]) == 0
    assert client.deleteByQuery(KeyValueCondition(key = "_id", value = "1")) == 0
    assert client.counts([ "1" ]) == 0
    assert client.countByQuery(KeyValueCondition(key = "_id", value = "1")) == 0
    # Create one and test
    model = createBigModel()
    assert client.create(model) == model.id
    assert client.exist(model.id)
    assert client.counts() == 1
    assert client.counts([ model.id ]) == 1
    assert client.countByQuery(KeyValueCondition(key = "_id", value = model.id)) == 1
    assert client.getOne(model.id) == model
    models = client.gets([ model.id ])
    assert models and len(models) == 1 and models[0] == model
    models = client.getByQuery(KeyValueCondition(key = "_id", value = model.id))
    assert models and len(models) == 1 and models[0] == model
    # Replace one and test
    model.intType = 100
    assert client.replace(model) == model.id
    assert client.getOne(model.id).intType == 100
    assert client.updateOne(model.id, [ SetAction(key = "intType", value = 1000) ]) == 1
    assert client.getOne(model.id).intType == 1000
    assert client.updates([ model.id ], [ SetAction(key = "intType", value = 10000) ]) == 1
    assert client.getOne(model.id).intType == 10000
    assert client.updateByQuery(KeyValueCondition(key = "_id", value = model.id), [ SetAction(key = "intType", value = 100000) ]) == 1
    assert client.getOne(model.id).intType == 100000
    # Delete
    assert client.deleteOne(model.id) == 1
    assert client.counts() == 0
    # Create & Delete
    model = createBigModel()
    assert client.create(model) == model.id
    # TODO: Support transparent error
    #try:
    #    client.create(model)
    #    raise AssertionError
    #except DuplicatedKeyError:
    #    pass
    assert client.create(model, overwrite = True) == model.id
    client.create(createBigModel())
    client.create(createBigModel())
    assert client.counts() == 3
    assert client.counts([ model.id ]) == 1
    assert client.countByQuery(KeyValueCondition(key = "_id", value = model.id)) == 1
    client.replace(createBigModel(), autoCreate = True)
    assert client.counts() == 4
    # Delete
    assert client.deletes([ model.id ]) == 1
    assert client.counts() == 3
    assert client.deleteByQuery(KeyValueCondition(key = "intType", value = 1)) == 3
    assert client.counts() == 0
