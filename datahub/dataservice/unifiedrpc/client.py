# encoding=utf8

""" The client of service
    Author: lipixun
    Created Time : 二  8/16 10:23:03 2016

    File Name: client.py
    Description:

        TODO: Support transparent error

"""

from urllib import quote_plus

import requests

from datahub.utils import json
from datahub.errors import ModelNotFoundError
from datahub.dataservice.interface import DataServiceInterface

class RestfulWebClient(DataServiceInterface):
    """The restful web client
    """
    def __init__(self, uri, modelCls, session = None):
        """Create a new RestfulWebClient
        """
        self.uri = uri if not uri.endswith("/") else uri[: -1]
        self.modelCls = modelCls
        self.session = session or requests.Session()

    def handleErrorResponse(self, rsp):
        """Handle error response
        """
        rsp.raise_for_status()

    def exist(self, id, **ctx):
        """Check if a model with id exists
        Returns:
            True / False
        """
        rsp = self.session.head(self.uri + "/%s" % quote_plus(id))
        if rsp.status_code == 200:
            return True
        elif rsp.status_code == 404:
            return False
        else:
            self.handleErrorResponse(rsp)

    def getOne(self, id, **ctx):
        """Get one model
        Returns:
            Model object or None
        """
        rsp = self.session.get(self.uri + "/%s" % quote_plus(id))
        if rsp.status_code == 200:
            return self.modelCls.load(json.loads(rsp.content)["value"])
        elif rsp.status_code == 404:
            return None
        else:
            self.handleErrorResponse(rsp)

    def gets(self, ids = None, start = 0, size = 0, sorts = None, **ctx):
        """Get models
        Returns:
            A list of model objects
        """
        data = {}
        if ids:
            data["ids"] = ids
        if start:
            data["start"] = start
        if size:
            data["size"] = size
        if sorts:
            data["sorts"] = [ x.dump() for x in sorts ]
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.post(self.uri + "/_gets", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return [ self.modelCls.load(x) for x in json.loads(rsp.content)["value"] ]
        else:
            self.handleErrorResponse(rsp)

    def getByQuery(self, query, start = 0, size = 0, sorts = None, **ctx):
        """Get by query
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        data = {}
        if query:
            data["query"] = query.dump()
        if start:
            data["start"] = start
        if size:
            data["size"] = size
        if sorts:
            data["sorts"] = [ x.dump() for x in sorts ]
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.post(self.uri + "/_getbyquery", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return [ self.modelCls.load(x) for x in json.loads(rsp.content)["value"] ]
        else:
            self.handleErrorResponse(rsp)

    def create(self, model, overwrite = False, **ctx):
        """Create a model
        Returns:
            The model id
        """
        model.validate()
        data = { "model": model.dump() }
        params = None
        if overwrite:
            params = { "overwrite": overwrite }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.post(self.uri or "/", params = params, headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        else:
            self.handleErrorResponse(rsp)

    def replace(self, model, autoCreate = False, **ctx):
        """Replace a model
        Returns:
            The model id
        """
        model.validate()
        data = { "model": model.dump() }
        params = None
        if autoCreate:
            params = { "autoCreate": autoCreate }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.put(self.uri or "/", params = params, headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        elif rsp.status_code == 404:
            raise ModelNotFoundError
        else:
            self.handleErrorResponse(rsp)

    def updateOne(self, id, updates, **ctx):
        """Update a model
        Returns:
            True / False
        """
        if not updates:
            raise ValueError("Require updates")
        updates = [ x.dump() for x in updates ]
        data = { "updates": updates }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.patch(self.uri + "/%s" % quote_plus(id), headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        elif rsp.status_code == 404:
            return False
        else:
            self.handleErrorResponse(rsp)

    def updates(self, ids, updates, **ctx):
        """Update models
        Returns:
            The number of models that is updated
        """
        if not ids:
            raise ValueError("Require ids")
        if not updates:
            raise ValueError("Require updates")
        updates = [ x.dump() for x in updates ]
        data = { "ids": ids, "updates": updates }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.patch(self.uri + "/_updates", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        else:
            self.handleErrorResponse(rsp)

    def updateByQuery(self, query, updates, **ctx):
        """Update by query
        Returns:
            The number of models that is updated
        """
        if not query:
            raise ValueError("Require query")
        if not updates:
            raise ValueError("Require updates")
        updates = [ x.dump() for x in updates ]
        data = { "query": query.dump(), "updates": updates }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.patch(self.uri + "/_updatebyquery", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        else:
            self.handleErrorResponse(rsp)

    def deleteOne(self, id, **ctx):
        """Delete a model
        Returns:
            True / False
        """
        rsp = self.session.delete(self.uri + "/%s" % quote_plus(id))
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        elif rsp.status_code == 404:
            return False
        else:
            self.handleErrorResponse(rsp)

    def deletes(self, ids, **ctx):
        """Delete models
        Returns:
            The number of models that is deleted
        """
        if not ids:
            raise ValueError("Require ids")
        data = { "ids": ids }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.post(self.uri + "/_deletes", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        else:
            self.handleErrorResponse(rsp)

    def deleteByQuery(self, query, **ctx):
        """Delete by query
        Returns:
            The number of models that is deleted
        """
        if not query:
            raise ValueError("Require query")
        data = { "query": query.dump() }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.post(self.uri + "/_deletebyquery", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        else:
            self.handleErrorResponse(rsp)

    def counts(self, ids = None, **ctx):
        """Count by ids
        Returns:
            The number of found models
        """
        if ids:
            data = { "ids": ids }
        else:
            data = {}
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.post(self.uri + "/_counts", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        else:
            self.handleErrorResponse(rsp)

    def countByQuery(self, query, **ctx):
        """Count by query
        Returns:
            The number of found models
        """
        if not query:
            raise ValueError("Require query")
        data = { "query": query.dump() }
        body = json.dumps(data, ensure_ascii = False).encode("utf8")
        rsp = self.session.post(self.uri + "/_countbyquery", headers = { "Content-Type": "application/json", "Content-Length": str(len(body)) }, data = body)
        if rsp.status_code == 200:
            return json.loads(rsp.content)["value"]
        else:
            self.handleErrorResponse(rsp)
