# encoding=utf8

""" The resource service
    Author: lipixun
    Created Time : 二  3/15 22:53:43 2016

    File Name: resource.py
    Description:

"""

import logging

from unifiedrpc import context, endpoint, Service, Endpoint
from unifiedrpc.helpers import requiredata, paramtype, container, mimetype
from unifiedrpc.errors import BadRequestError, NotFoundError, InternalServerError
from unifiedrpc.adapters.web import head, get, post, put, patch, delete
from unifiedrpc.content.container import PlainContentContainer

from datahub.spec import *
from datahub.utils import json
from datahub.sorts import SortRule
from datahub.model import DataModel
from datahub.errors import DataHubError, ModelNotFoundError, WatchTimeoutError, WatchResetError, DuplicatedKeyError
from datahub.updates import UpdateAction, SetAction
from datahub.repository import Repository
from datahub.conditions import Condition, AndCondition, KeyValueCondition

CONFIG_WATCH_KEEP_ALIVE         = 10

class ResourceLocation(object):
    """The resource location
    Attributes:
        path                                    The resource path prefix
        features                                The enabled features for this resource location
        modelAttrParams                         The model attribute parameters, a dict key is parameter value is attribute path
    """
    def __init__(self, path, features = None, modelAttrParams = None, general = True):
        """Create a ResourceLocation
        """
        # Well form the path
        path = path if len(path) == 1 or not path.endswith('/') else path[: -1]
        if not path:
            path = '/'
        # Set attribtues
        self.path = path
        self.general = general
        self.features = features
        self.modelAttrParams = modelAttrParams

class ParamRepoSelector(object):
    """The repository selector based on parameter
    Attributes:
        param                                   The parameter name
        repositories                            The repository dict which key is string (The parameter value) value is Manager object
        default                                 The default repository when could not choose repository from parameter
    """
    def __init__(self, param, repositories, default = None):
        """Create a new ParamRepoSelector
        """
        self.param = param
        self.default = default
        self.repositories = repositories

    def __call__(self, params):
        """Pop repository from params
        """
        key = params.pop(self.param, None)
        if key in self.repositories:
            return self.repositories[key]
        else:
            return self.default

class ResourceService(Service):
    """The resource service
    """
    def __init__(self, repository, locations, name = None, configs = None):
        """Create a new ResourceService
        """
        self.configs = configs
        self.locations = locations
        self.repository = repository
        # Create the endpoints
        endpoints = {}
        for location in self.locations:
            endpoints.update(map(lambda (name, endpoint): ('%s:%s' % (location.path, name), endpoint), self.createEndpoints4Location(location)))
        # Super
        super(ResourceService, self).__init__(name, endpoints)

    def invoke(self, location, feature, target, params):
        """Handle resource request
        Parameters:
            location                            The ResourceLocation object
            feature                             The feature name
            target                              The calling target
            params                              The parameters
        Returns:
            The return result object
        """
        return target(**params)

    def popRepositoryFromParams(self, params):
        """Get repository
        Returns:
            params, repo
        """
        if isinstance(self.repository, Repository):
            # The repository
            return self.repository
        else:
            # Get the repository
            return self.repository(params)

    def popIDFromParamsOrBody(self, params, body, key = 'id'):
        """Get and pop id
        """
        # TODO: Identify a single id and a single id with blank ids (which indicates this is a multiple ids request)
        # Check key
        if key in params and key in body:
            raise BadRequestError(reason = 'Found id in both parameters and body')
        # Get id
        if key in params:
            id = params.pop(key, None)
            if id.find(',') != -1:
                # A list
                return filter(lambda x: x, map(lambda x: x.strip(), id.split(',')))
            else:
                # A string
                return id
        elif key in body:
            return body.pop(key, None)

    def popQueryFromBody(self, body, key = 'query'):
        """Get query from body
        """
        try:
            if body:
                query = body.pop(key, None)
                if query:
                    return Condition.load(query)
            # Done
        except DataHubError as error:
            raise BadRequestError(reason = 'Invalid query [%s]' % error)

    def popSortsFromBody(self, body, key = 'sorts'):
        """Get and remove sorts
        """
        sorts = body.pop(key, None)
        if sorts:
            if not isinstance(sorts, (list, tuple)):
                raise BadRequestError(reason = 'Invalid sorts')
            sortRules = []
            for sort in sorts:
                if not isinstance(sort, dict):
                    raise BadRequestError(reason = 'Invalid sorts')
                try:
                    sort = SortRule(sort)
                    sort.validate()
                except DataModelError as error:
                    raise BadRequestError(reason = 'Invalid sort. Error [%s]' % error)
                # Add this rule
                sortRules.append(sort)
            # Done
            return sortRules

    def popModelAttributeConditionsFromParams(self, location, params):
        """Get the query condition by value for this parameter
        Returns:
            A list of Condition object or None
        """
        if location.modelAttrParams:
            conditions = []
            for key, attrPath in location.modelAttrParams.iteritems():
                if key in params:
                    conditions.append(KeyValueCondition(key = attrPath, value = params.pop(key)))
            return conditions

    def popModelAttributeUpdateActionsFromParams(self, location, params):
        """The the value to model
        Returns:
            A list of UpdateAction object or None
        """
        if location.modelAttrParams:
            updateActions = []
            for key, attrPath in location.modelAttrParams.iteritems():
                if key in params:
                    updateActions.append(SetAction(key = attrPath, value = params.pop(key)))
            return updateActions

    def getEndpointHandler(self, location, method):
        """Get the endpoint handler
        """
        def handler(service, **kwargs):
            """The handler
            """
            return method(location, kwargs, context.request.content.data if context.request.content and context.request.content.data else {})
        # Done
        return handler

    def getLocationPath(self, location, path):
        """Get location path
        """
        if location.path.endswith('/'):
            return location.path + path[1: ]
        else:
            return location.path + path

    def createEndpoints4Location(self, location):
        """Create the endpoint for a feature
        Returns:
            Yield of (name, Endpoint) object
        """
        # The general feature
        if location.general:
            endpoint = Endpoint(self.getEndpointHandler(location, self.general))
            get(path = self.getLocationPath(location, '/_feature/<feature>'))(endpoint)
            post(path = self.getLocationPath(location, '/_feature/<feature>'))(endpoint)
            # Yield
            yield 'general', endpoint
        # The watch feature
        if not location.features or FEATURE_WATCH in location.features:
            endpoint = Endpoint(self.getEndpointHandler(location, self.watch))
            container(PlainContentContainer)(endpoint)
            mimetype('text/plain')(endpoint)
            post(path = self.getLocationPath(location, '/_watch'))(endpoint)
            # Yield
            yield 'watch', endpoint
        # The exist feature
        if not location.features or \
            FEATURE_STORE_EXIST in location.features or \
            FEATURE_QUERY_EXIST in location.features:
            # Create endpoint
            endpoint = Endpoint(self.getEndpointHandler(location, self.exist))
            head(path = self.getLocationPath(location, '/'))(endpoint)
            head(path = self.getLocationPath(location, '/<id>'))(endpoint)
            yield 'exist', endpoint
        # The get feature
        if not location.features or \
            FEATURE_STORE_GET in location.features or \
            FEATURE_QUERY_GET in location.features:
            endpoint = Endpoint(self.getEndpointHandler(location, self.get))
            get(path = self.getLocationPath(location, '/'))(endpoint)
            get(path = self.getLocationPath(location, '/<id>'))(endpoint)
            post(path = self.getLocationPath(location, '/_query'))(endpoint)
            yield 'get', endpoint
        # Get create feature
        if not location.features or FEATURE_STORE_CREATE in location.features:
            endpoint = Endpoint(self.getEndpointHandler(location, self.create))
            post(path = self.getLocationPath(location, '/'))(endpoint)
            yield 'create', endpoint
        # The replace feature
        if not location.features or FEATURE_STORE_REPLACE in location.features:
            endpoint = Endpoint(self.getEndpointHandler(location, self.replace))
            put(path = self.getLocationPath(location, '/'))(endpoint)
            yield 'replace', endpoint
        # The update feature
        if not location.features or \
            FEATURE_STORE_UPDATE in location.features or \
            FEATURE_QUERY_UPDATE in location.features:
            endpoint = Endpoint(self.getEndpointHandler(location, self.update))
            patch(path = self.getLocationPath(location, '/'))(endpoint)
            patch(path = self.getLocationPath(location, '/<id>'))(endpoint)
            yield 'update', endpoint
        # The delete feature
        if not location.features or \
            FEATURE_STORE_DELETE in location.features or \
            FEATURE_QUERY_DELETE in location.features:
            endpoint = Endpoint(self.getEndpointHandler(location, self.delete))
            delete(path = self.getLocationPath(location, '/'))(endpoint)
            delete(path = self.getLocationPath(location, '/<id>'))(endpoint)
            yield 'delete', endpoint
        # The count feature
        if not location.features or \
            FEATURE_STORE_COUNT in location.features or \
            FEATURE_QUERY_COUNT in location.features:
            endpoint = Endpoint(self.getEndpointHandler(location, self.count))
            get(path = self.getLocationPath(location, '/_count'))(endpoint)
            post(path = self.getLocationPath(location, '/_count'))(endpoint)
            get(path = self.getLocationPath(location, '/_count/<id>'))(endpoint)
            yield 'count', endpoint

    def general(self, location, params, body):
        """General entry
        """
        feature = params.pop('feature', None)
        if not feature:
            raise BadRequestError(reason = 'Require feature')
        if location.features and not feature in location.features:
            raise BadRequestError(reason = 'Unsupported feature [%s]' % feature)
        # Call feature handlers
        if feature == FEATURE_WATCH:
            return self.watch(location, params, body)
        elif feature in (FEATURE_STORE_EXIST, FEATURE_QUERY_EXIST):
            return self.exist(location, params, body)
        elif feature in (FEATURE_STORE_GET, FEATURE_QUERY_GET):
            return self.get(location, params, body)
        elif feature == FEATURE_STORE_CREATE:
            return self.create(location, params, body)
        elif feature == FEATURE_STORE_REPLACE:
            return self.replace(location, params, body)
        elif feature in (FEATURE_STORE_UPDATE, FEATURE_QUERY_UPDATE):
            return self.update(location, params, body)
        elif feature in (FEATURE_STORE_DELETE, FEATURE_QUERY_DELETE):
            return self.delete(location, params, body)
        elif feature in (FEATURE_STORE_COUNT, FEATURE_QUERY_COUNT):
            return self.count(location, params, body)
        else:
            # Unknown feature
            raise BadRequestError(reason = 'Unknown feature [%s]' % feature)

    def watch(self, location, params, body):
        """The watch entry
        """
        raise NotImplementedError

    def exist(self, location, params, body):
        """Exist entry
        """
        id, query = self.popIDFromParamsOrBody(params, body), self.popQueryFromBody(body)
        if id and query:
            raise BadRequestError(reason = 'Cannot both specify id and query')
        # Get repository
        repo = self.popRepositoryFromParams(params)
        if not repo:
            raise NotFoundError(reason = 'Repository not found')
        # Pop query from parameters
        queryFromParams = self.popModelAttributeConditionsFromParams(location, params)  # Returns a list of Condition object
        # Pop config
        configs = body.pop('configs', None)
        # Check params & body
        if params:
            raise BadRequestError(reason = 'Invalid parameter')
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Run
        if query or queryFromParams:
            # Use query
            if location.features and not FEATURE_QUERY_EXIST in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_QUERY_EXIST)
            # Build query
            query = [ query ] if query else []
            if queryFromParams:
                query.extend(queryFromParams)
            # Check id
            if id:
                if isinstance(id, basestring):
                    query.append(KeyValueCondition(key = '_id', value = id))
                else:
                    query.append(KeyValuesCondition(key = '_id', values = id))
            if len(query) == 1:
                query = query[0]
            else:
                query = AndCondition(conditions = query)
            # Call repository
            if not self.invoke(location, FEATURE_QUERY_EXIST, repo.existByQuery, dict(query = query, configs = configs)):
                raise NotFoundError
        else:
            if location.features and not FEATURE_STORE_EXIST in location.features:
                raise BadRequestError(reason = 'Unsupported fe]ature [%s]' % FEATURE_STORE_EXIST)
            # Call repository
            if not self.invoke(location, FEATURE_STORE_EXIST, repo.exist, dict(id = id, configs = configs)):
                raise NotFoundError

    def get(self, location, params, body):
        """Exist entry
        """
        id, query = self.popIDFromParamsOrBody(params, body), self.popQueryFromBody(body)
        if id and query:
            raise BadRequestError(reason = 'Cannot both specify id and query')
        # Get repository
        repo = self.popRepositoryFromParams(params)
        if not repo:
            raise NotFoundError(reason = 'Repository not found')
        # Pop start & size
        start0, size0 = params.pop('start', None), params.pop('size', None)
        start1, size1 = body.pop('start', None), body.pop('size', None)
        if not start0 is None and not start1 is None:
            raise BadRequestError(reason = 'Cannot specify start multiple times')
        if not size0 is None and not size1 is None:
            raise BadRequestError(reason = 'Cannot specify size multiple times')
        start = 0
        if not start0 is None:
            start = start0
        if not start1 is None:
            start = start1
        size = 0
        if not size0 is None:
            size = size0
        if not size1 is None:
            size = size1
        # Pop sorts
        sorts = self.popSortsFromBody(body)
        # Pop conditions
        queryFromParams = self.popModelAttributeConditionsFromParams(location, params)
        # Pop configs
        configs = body.pop('configs', None)
        # Check params & body
        if params:
            raise BadRequestError(reason = 'Invalid parameter')
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Run
        if query or queryFromParams:
            # Use query
            if location.features and not FEATURE_QUERY_GET in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_QUERY_GET)
            # Build query
            query = [ query ] if query else []
            if queryFromParams:
                query.extend(queryFromParams)
            if id:
                if isinstance(id, basestring):
                    query.append(KeyValueCondition(key = '_id', value = id))
                else:
                    query.append(KeyValuesCondition(key = '_id', values = id))
            if len(query) == 1:
                query = query[0]
            else:
                query = AndCondition(conditions = query)
            # Call repository
            models = self.invoke(location, FEATURE_QUERY_GET, repo.getByQuery, dict(query = query, sorts = sorts, start = start, size = size, configs = configs))
        else:
            # Use id
            if location.features and not FEATURE_STORE_GET in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_STORE_GET)
            # Call repository
            models = self.invoke(location, FEATURE_STORE_GET, repo.get, dict(id = id, configs = configs))
        # Return result
        if isinstance(id, basestring):
            # A single result
            models = list(models)
            if not models:
                raise NotFoundError
            elif len(models) != 1:
                raise InternalServerError('Multiple models found by id query')
            else:
                return models[0].dump()
        else:
            # Multiple result
            return [ x.dump() for x in models ]

    def create(self, location, params, body):
        """Create entry
        """
        # Get repository
        repo = self.popRepositoryFromParams(params)
        if not repo:
            raise NotFoundError(reason = 'Repository not found')
        # Check feature
        if location.features and not FEATURE_STORE_CREATE in location.features:
            raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_STORE_CREATE)
        model, configs = body.pop('model', None), body.pop('configs', None)
        if not model:
            raise BadRequestError(reason = 'Require model')
        # Create the model object
        try:
            model = repo.cls(model)
            # Get updates
            updateActions = self.popModelAttributeUpdateActionsFromParams(location, params)
            if updateActions:
                model.update(updateActions)
            # Validate the model
            model.validate()
        except DataModelError as error:
            raise BadRequestError(reason = 'Invalid model. Error [%s]' % error)
        # Check params & body
        if params:
            raise BadRequestError(reason = 'Invalid parameter')
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Call repository
        try:
            self.invoke(location, FEATURE_STORE_CREATE, repo.create, dict(model = model, configs = configs))
        except DuplicatedKeyError:
            raise BadRequestError(code = ERROR_DUPLICATED_KEY, reason = 'Duplicated key found')
        # Done

    def replace(self, location, params, body):
        """Replace entry
        """
        # Get repository
        repo = self.popRepositoryFromParams(params)
        if not repo:
            raise NotFoundError(reason = 'Repository not found')
        # Check feature
        if location.features and not FEATURE_STORE_REPLACE in location.features:
            raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_STORE_REPLACE)
        model, configs = body.pop('model', None), body.pop('configs', None)
        if not model:
            raise BadRequestError(reason = 'Require model')
        # Create the model object
        try:
            model = repo.cls(model)
            # Get updates
            updateActions = self.popModelAttributeUpdateActionsFromParams(location, params)
            if updateActions:
                model.update(updateActions)
            # Validate the model
            model.validate()
        except DataModelError as error:
            raise BadRequestError(reason = 'Invalid model. Error [%s]' % error)
        # Check params & body
        if params:
            raise BadRequestError(reason = 'Invalid parameter')
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Call repository
        try:
            self.invoke(location, FEATURE_STORE_REPLACE, repo.replace, dict(model = model, configs = configs))
        except ModelNotFoundError:
            raise NotFoundError
        # Done

    def update(self, location, params, body):
        """Update entry
        """
        id, query = self.popIDFromParamsOrBody(params, body), self.popQueryFromBody(body)
        if id and query:
            raise BadRequestError(reason = 'Cannot both specify id and query')
        if not id and not query:
            raise BadRequestError(reason = 'Require id or query')
        # Get repository
        repo = self.popRepositoryFromParams(params)
        if not repo:
            raise NotFoundError(reason = 'Repository not found')
        # Get update actions and configs
        updates, configs = body.pop('updates', None), body.pop('configs', None)
        if not updates:
            raise BadRequestError(reason = 'Require updates')
        try:
            updates = [ UpdateAction.load(x) for x in updates ]
        except:
            raise BadRequestError(reason = 'Invalid updates')
        # Pop query
        queryFromParams = self.popModelAttributeConditionsFromParams(location, params)
        # Check params & body
        if params:
            raise BadRequestError(reason = 'Invalid parameter')
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Call repository
        if query or queryFromParams:
            if location.features and not FEATURE_QUERY_UPDATE in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_QUERY_UPDATE)
            # Build query
            query = [ query ] if query else []
            if queryFromParams:
                query.extend(queryFromParams)
            if id:
                if isinstance(id, basestring):
                    query.append(KeyValueCondition(key = '_id', value = id))
                else:
                    query.append(KeyValuesCondition(key = '_id', values = id))
            if len(query) == 1:
                query = query[0]
            else:
                query = AndCondition(conditions = query)
            # Call repository
            return self.invoke(location, FEATURE_QUERY_UPDATE, repo.updateByQuery, dict(
                query = query,
                updates = updates,
                configs = configs
                ))
        else:
            if location.features and not FEATURE_STORE_UPDATE in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_STORE_UPDATE)
            # Update a single document
            return self.invoke(location, FEATURE_STORE_UPDATE, repo.update, dict(id = id, updates = updates, configs = configs))

    def delete(self, location, params, body):
        """Delete entry
        """
        id, query = self.popIDFromParamsOrBody(params, body), self.popQueryFromBody(body)
        if id and query:
            raise BadRequestError(reason = 'Cannot both specify id and query')
        if not id and not query:
            raise BadRequestError(reason = 'Require id or query')
        # Get repository
        repo = self.popRepositoryFromParams(params)
        if not repo:
            raise NotFoundError(reason = 'Repository not found')
        # Pop configs
        configs = body.pop('configs', None)
        # Pop conditions
        queryFromParams = self.popModelAttributeConditionsFromParams(location, params)
        # Check params & body
        if params:
            raise BadRequestError(reason = 'Invalid parameter')
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Run
        if query or queryFromParams:
            # Use query
            if location.features and not FEATURE_QUERY_DELETE in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_QUERY_DELETE)
            # Build query
            query = [ query ] if query else []
            if queryFromParams:
                query.extend(queryFromParams)
            if id:
                if isinstance(id, basestring):
                    query.append(KeyValueCondition(key = '_id', value = id))
                else:
                    query.append(KeyValuesCondition(key = '_id', values = id))
            if len(query) == 1:
                query = query[0]
            else:
                query = AndCondition(conditions = query)
            # Call repository
            return self.invoke(location, FEATURE_QUERY_DELETE, repo.deleteByQuery, dict(query = query, configs = configs))
        else:
            # Use id
            if location.features and not FEATURE_STORE_DELETE in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_STORE_DELETE)
            # Call repository
            return self.invoke(location, FEATURE_STORE_DELETE, repo.delete, dict(id = id, configs = configs))

    def count(self, location, params, body):
        """Count entry
        """
        id, query = self.popIDFromParamsOrBody(params, body), self.popQueryFromBody(body)
        if id and query:
            raise BadRequestError(reason = 'Cannot both specify id and query')
        # Get repository
        repo = self.popRepositoryFromParams(params)
        if not repo:
            raise NotFoundError(reason = 'Repository not found')
        # Pop conditions
        queryFromParams = self.popModelAttributeConditionsFromParams(location, params)
        # Pop configs
        configs = body.pop('configs', None)
        # Check params & body
        if params:
            raise BadRequestError(reason = 'Invalid parameter')
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Run
        if query or queryFromParams:
            # Use query
            if location.features and not FEATURE_QUERY_COUNT in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_QUERY_COUNT)
            # Build query
            query = [ query ] if query else []
            if queryFromParams:
                query.extend(queryFromParams)
            if id:
                if isinstance(id, basestring):
                    query.append(KeyValueCondition(key = '_id', value = id))
                else:
                    query.append(KeyValuesCondition(key = '_id', values = id))
            if len(query) == 1:
                query = query[0]
            else:
                query = AndCondition(conditions = query)
            # Call repository
            return self.invoke(location, FEATURE_QUERY_COUNT, repo.countByQuery, dict(query = query, configs = configs))
        else:
            # Count id or all
            if location.features and not FEATURE_STORE_COUNT in location.features:
                raise BadRequestError(reason = 'Unsupported feature [%s]' % FEATURE_STORE_COUNT)
            # Call repository
            return self.invoke(location, FEATURE_STORE_COUNT, repo.count, dict(id = id))
