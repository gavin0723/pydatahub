# encoding=utf8

""" The datahub data service based on unifiedrpc framework
    Author: lipixun
    Created Time : 二  8/16 10:22:31 2016

    File Name: service.py
    Description:

        TODO: Support transparent error

"""

from unifiedrpc import Service, endpoint, context
from unifiedrpc.errors import BadRequestError, NotFoundError
from unifiedrpc.helpers import paramtype, requiredata, container, mimetype
from unifiedrpc.paramtypes import boolean
from unifiedrpc.adapters.web import head, get, post, put, patch, delete

from datahub.sorts import SortRule
from datahub.errors import DuplicatedKeyError, ModelNotFoundError
from datahub.updates import UpdateAction
from datahub.conditions import Condition

class RestfulWebService(Service):
    """The restful web service
    """
    def __init__(self, modelCls, underlying, factory = None, name = None, endpoints = None, configs = None, stage = None):
        """Create a new RestfulWebService
        """
        self.modelCls = modelCls
        self.underlying = underlying
        self.factory = factory or RestfulEndpointFactory()
        # Create endpoints
        if not endpoints:
            endpoints = {}
        endpoints["__exist"] = self.factory.create("exist", self.exist)
        endpoints["__getOne"] = self.factory.create("getOne", self.getOne)
        endpoints["__list"] = self.factory.create("list", self.list)
        endpoints["__gets"] = self.factory.create("gets", self.gets)
        endpoints["__getByQuery"] = self.factory.create("getByQuery", self.getByQuery)
        endpoints["__create"] = self.factory.create("create", self.create)
        endpoints["__replace"] = self.factory.create("replace", self.replace)
        endpoints["__updateOne"] = self.factory.create("updateOne", self.updateOne)
        endpoints["__updates"] = self.factory.create("updates", self.updates)
        endpoints["__updateByQuery"] = self.factory.create("updateByQuery", self.updateByQuery)
        endpoints["__deleteOne"] = self.factory.create("deleteOne", self.deleteOne)
        endpoints["__deletes"] = self.factory.create("deletes", self.deletes)
        endpoints["__deleteByQuery"] = self.factory.create("deleteByQuery", self.deleteByQuery)
        endpoints["__counts"] = self.factory.create("counts", self.counts)
        endpoints["__countByQuery"] = self.factory.create("countByQuery", self.countByQuery)
        # Super
        super(RestfulWebService, self).__init__(name, endpoints, configs, stage)

    def mapModelAfterGet(self, model, **ctx):
        """Map the model after get
        Returns:
            Model object, None means do not return this model
        """
        return model

    def mapModelBeforeCreate(self, model, **ctx):
        """Map the model before create
        Returns:
            Model object, None means do not create this model
        """
        return model

    def mapModelBeforeReplace(self, model, **ctx):
        """Map the model before replace
        Returns:
            Model object, None means do not replace this model
        """
        return model

    def exist(self, id, **ctx):
        """Check if a model with id exists
        Returns:
            True / False
        """
        if not self.underlying.exist(id, **ctx):
            raise NotFoundError

    def getOne(self, id, **ctx):
        """Get one model
        Returns:
            Model object or None
        """
        model = self.underlying.getOne(id, **ctx)
        if not model:
            raise NotFoundError
        model = self.mapModelAfterGet(model, **ctx)
        if not model:
            raise NotFoundError
        # Done
        return model.dump()

    def list(self, start = 0, size = 0, sorts = None, **ctx):
        """List models
        """
        sortRules = None
        if sorts:
            sortRules = []
            for s in sorts.split(","):
                s = s.strip()
                if s:
                    index = s.find(":")
                    if index == -1:
                        sortRules.append(SortRule(key = s))
                    else:
                        sortRules.append(SortRule(key = s[: index], ascending = s[index + 1: ].lower() == "ascending"))
        # Gets
        models = self.underlying.gets(None, start, size, sorts, **ctx)
        if models is None:
            return []
        else:
            return [ x.dump() for x in filter(lambda x: x, map(lambda x: self.mapModelAfterGet(x, **ctx), models)) ]

    def gets(self, **ctx):
        """Get models
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        body = context.request.content.data
        # Get parameters
        ids, start, size, sorts = body.get("ids"), body.get("start", 0), body.get("size", 0), body.get("sorts")
        # Decode
        try:
            start = int(start)
        except Exception as error:
            raise BadRequestError(reason = "Invalid parameter start, error: %s" % error)
        try:
            size = int(size)
        except Exception as error:
            raise BadRequestError(reason = "Invalid parameter size, error: %s" % error)
        try:
            if sorts:
                sorts = [ SortRule(x) for x in sorts ]
                for s in sorts:
                    s.validate()
        except Exception as error:
            raise BadRequestError(reason = "Invalid parameter sorts, error: %s" % error)
        # Gets
        models = self.underlying.gets(ids, start, size, sorts, **ctx)
        if models is None:
            return []
        else:
            return [ x.dump() for x in filter(lambda x: x, map(lambda x: self.mapModelAfterGet(x, **ctx), models)) ]

    def getByQuery(self, **ctx):
        """Get by query
        Returns:
            A list of model objects or empty list or None
            NOTE: Yield of models is also allowed
        """
        body = context.request.content.data
        # Get parameters
        query, start, size, sorts = body.get("query"), body.get("start", 0), body.get("size", 0), body.get("sorts")
        # Decode
        try:
            query = Condition.load(query)
        except Exception as error:
            raise BadRequestError(reason = "Invalid parameter query, error: %s" % error)
        try:
            start = int(start)
        except Exception as error:
            raise BadRequestError(reason = "Invalid parameter start, error: %s" % error)
        try:
            size = int(size)
        except Exception as error:
            raise BadRequestError(reason = "Invalid parameter size, error: %s" % error)
        try:
            if sorts:
                sorts = [ SortRule(x) for x in sorts ]
                for s in sorts:
                    s.validate()
        except Exception as error:
            raise BadRequestError(reason = "Invalid parameter sorts, error: %s" % error)
        # Gets
        models = self.underlying.getByQuery(query, start, size, sorts, **ctx)
        if models is None:
            return []
        else:
            return [ x.dump() for x in filter(lambda x: x, map(lambda x: self.mapModelAfterGet(x, **ctx), models)) ]

    def create(self, overwrite = False, **ctx):
        """Create a model
        Returns:
            The model id
        """
        body = context.request.content.data
        # Decode the model
        try:
            model = self.modelCls.load(body["model"])
        except Exception as error:
            raise BadRequestError(reason = "Invalid post model, error: %s" % error)
        model = self.mapModelBeforeCreate(model, **ctx)
        if not model:
            raise BadRequestError(reason = "Create is denied by model mapping")
        # Create
        return self.underlying.create(model, overwrite, **ctx)

    def replace(self, autoCreate = False, **ctx):
        """Replace a model
        Returns:
            The model id
        """
        body = context.request.content.data
        # Decode the model
        try:
            model = self.modelCls.load(body["model"])
        except Exception as error:
            raise BadRequestError(reason = "Invalid post model, error: %s" % error)
        # Check
        model = self.mapModelBeforeReplace(model, **ctx)
        if not model:
            raise BadRequestError(reason = "Replace is denied by model mapping")
        # Replace
        try:
            return self.underlying.replace(model, autoCreate, **ctx)
        except ModelNotFoundError:
            raise NotFoundError

    def updateOne(self, id, **ctx):
        """Update a model
        Returns:
            True / False
        """
        body = context.request.content.data
        # Decode the updates
        try:
            updates = body.get("updates")
            if not updates:
                updates = []
            else:
                updates = [ UpdateAction.load(x) for x in updates ]
        except Exception as error:
            raise BadRequestError(reason = "Invalid post data, error: %s" % error)
        # Create
        if not self.underlying.updateOne(id, updates, **ctx):
            raise NotFoundError
        return True

    def updates(self, **ctx):
        """Update models
        Returns:
            The number of models that is updated
        """
        body = context.request.content.data
        # Decode the updates
        try:
            ids = body.get("ids")
            updates = body.get("updates")
            if not updates:
                updates = []
            else:
                updates = [ UpdateAction.load(x) for x in updates ]
        except Exception as error:
            raise BadRequestError(reason = "Invalid post data, error: %s" % error)
        # Create
        return self.underlying.updates(ids, updates, **ctx)

    def updateByQuery(self, **ctx):
        """Update by query
        Returns:
            The number of models that is updated
        """
        body = context.request.content.data
        # Decode the updates
        try:
            query = body.get("query")
            if query:
                query = Condition.load(query)
            updates = body.get("updates")
            if not updates:
                updates = []
            else:
                updates = [ UpdateAction.load(x) for x in updates ]
        except Exception as error:
            raise BadRequestError(reason = "Invalid post data, error: %s" % error)
        # Create
        return self.underlying.updateByQuery(query, updates, **ctx)

    def deleteOne(self, id, **ctx):
        """Delete a model
        Returns:
            True / False
        """
        if not self.underlying.deleteOne(id, **ctx):
            raise NotFoundError
        return True

    def deletes(self, **ctx):
        """Delete models
        Returns:
            The number of models that is deleted
        """
        body = context.request.content.data
        # Decode the updates
        ids = body.get("ids")
        # Deletes
        return self.underlying.deletes(ids, **ctx)

    def deleteByQuery(self, **ctx):
        """Delete by query
        Returns:
            The number of models that is deleted
        """
        body = context.request.content.data
        # Decode the updates
        try:
            query = body.get("query")
            if query:
                query = Condition.load(query)
        except Exception as error:
            raise BadRequestError(reason = "Invalid post data, error: %s" % error)
        # Deletes
        return self.underlying.deleteByQuery(query, **ctx)

    def counts(self, **ctx):
        """Count by ids
        Returns:
            The number of found models
        """
        body = context.request.content.data
        # Decode the updates
        ids = body.get("ids") if body else None
        # Deletes
        return self.underlying.counts(ids, **ctx)

    def countByQuery(self, **ctx):
        """Count by query
        Returns:
            The number of found models
        """
        body = context.request.content.data
        # Decode the updates
        try:
            query = body.get("query")
            if query:
                query = Condition.load(query)
        except Exception as error:
            raise BadRequestError(reason = "Invalid post data, error: %s" % error)
        # Deletes
        return self.underlying.countByQuery(query, **ctx)

class RestfulEndpointFactory(object):
    """The restful endpoint factory
    """
    def __init__(self, prefix = ""):
        """Create a new RestfulEndpointFactory
        """
        self.prefix = prefix if not prefix.endswith("/") else prefix[: -1]

    def create(self, name, handler):
        """Create a new endpoint
        """
        if name == "exist":
            # Create a exist endpoint
            return head(path = self.prefix + "/<id>")(endpoint()(handler))
        elif name == "getOne":
            # Create a getOne endpoint
            return get(path = self.prefix + "/<id>")(endpoint()(handler))
        elif name == "list":
            # Create a list endpoint
            ep = get(path = self.prefix or "/")(endpoint()(handler))
            paramtype(start = int, size = int)(ep)
            return ep
        elif name == "gets":
            # Create a gets endpoint
            ep = post(path = self.prefix + "/_gets")(endpoint()(handler))
            requiredata()(ep)
            return ep
        elif name == "getByQuery":
            # Create a getByQuery endpoint
            ep = post(path = self.prefix + "/_getbyquery")(endpoint()(handler))
            requiredata()(ep)
            return ep
        elif name == "create":
            # Create a create endpoint
            ep = post(path = self.prefix or "/")(endpoint()(handler))
            paramtype(overwrite = boolean)(ep)
            requiredata()(ep)
            return ep
        elif name == "replace":
            # Create a replace endpoint
            ep = put(path = self.prefix or "/")(endpoint()(handler))
            paramtype(autoCreate = boolean)(ep)
            requiredata()(ep)
            return ep
        elif name == "updateOne":
            # Create update one endpoint
            ep = patch(path = self.prefix + "/<id>")(endpoint()(handler))
            requiredata()(ep)
            return ep
        elif name == "updates":
            # Create updates endpoint
            ep = patch(path = self.prefix + "/_updates")(endpoint()(handler))
            requiredata()(ep)
            return ep
        elif name == "updateByQuery":
            # Create update by query endpoint
            ep = patch(path = self.prefix + "/_updatebyquery")(endpoint()(handler))
            requiredata()(ep)
            return ep
        elif name == "deleteOne":
            # Create delete one endpoint
            return delete(path = self.prefix + "/<id>")(endpoint()(handler))
        elif name == "deletes":
            # Create deletes endpoint
            ep = post(path = self.prefix + "/_deletes")(endpoint()(handler))
            requiredata()(ep)
            return ep
        elif name == "deleteByQuery":
            # Create delete by query endpoint
            ep = post(path = self.prefix + "/_deletebyquery")(endpoint()(handler))
            requiredata()(ep)
            return ep
        elif name == "counts":
            # Create counts endpoint
            ep = post(path = self.prefix + "/_counts")(endpoint()(handler))
            return ep
        elif name == "countByQuery":
            # Create count by query endpoint
            ep = post(path = self.prefix + "/_countbyquery")(endpoint()(handler))
            requiredata()(ep)
            return ep
        else:
            raise ValueError("Unknown endpoint name [%s]" % name)
