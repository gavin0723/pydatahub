# encoding=utf8

""" The resource service
    Author: lipixun
    Created Time : 二  3/15 22:53:43 2016

    File Name: resource.py
    Description:

    Check the standard interface
    A map for special endpoints:
        - store.exist           HEAD            /<id>
        - store.exists          HEAD            /<id>               ID is splited by ,
        - store.get             GET             /<id>
        - store.gets            GET             /<id>               ID is splited by ,
        - store.gets            GET             /
        - store.create          POST            /
        - store.replace         PUT             /
        - store.update          PATCH           /<id>
        - store.updates         PATCH           /<id>               ID is splited by ,
        - store.delete          DELETE          /<id>
        - store.deletes         DELETE          /<id>               ID is splited by ,
    All feature has endpoint
        - *                     POST            /_feature/<feature name>

"""

import logging

from unifiedrpc import context, endpoint, Service, Endpoint
from unifiedrpc.helpers import requiredata, paramtype, container, mimetype
from unifiedrpc.errors import BadRequestError, NotFoundError
from unifiedrpc.adapters.web import head, get, post, put, patch, delete
from unifiedrpc.content.container import PlainContentContainer

from datahub.utils import json
from datahub.model import DataModel
from datahub.conditions import *
from datahub.updates import *
from datahub.sorts import *
from datahub.errors import DataHubError, ModelNotFoundError

class ResourceLocation(object):
    """The resource location
    Attributes:
        path                                    The resource path prefix
        params                                  The allowed parameters in this path
        features                                The enabled features for this resource location
    """
    def __init__(self, path, params = None, features = None, enableGeneralFeature = True, enableWatch = True):
        """Create a ResourceLocation
        """
        self.path = path if not path.endswith('/') else path[: -1]
        self.params = params
        self.features = features
        self.enableGeneralFeature = enableGeneralFeature
        self.enableWatch = enableWatch

class ParameterMapper(object):
    """The parameter mapper
    """
    def getCondition(self, value):
        """Get the query condition by value for this parameter
        """
        raise NotImplementedError

    def setModel(self, value):
        """The the value to model
        """
        raise NotImplementedError

class KeyValueAttributeParameterMapper(ParameterMapper):
    """The key value parameter attribute mapper
    Maps a parameter to a model attribute (By key)
    """
    def __init__(self, key):
        """Create a new KeyValueAttributeParameterMapper
        """
        self.key = key

    def getCondition(self, value):
        """Get the query condition by value for this parameter
        """
        return KeyValueCondition(key = self.key, value = value)

    def setModel(self, model, value):
        """The the value to model
        """
        model.update(SetAction(key = self.key, value = value))

class ResourceService(Service):
    """The resource service
    """
    def __init__(self, name, manager, locations, pipeline = None, serviceName = None):
        """Create a new ResourceService
        """
        self.name = name
        self.manager = manager
        self.locations = locations
        # Create the endpoints
        endpoints = {}
        for location in self.locations:
            endpoints.update(map(lambda (name, endpoint): ('%s:%s' % (location.path, name), endpoint), self.createEndpoints4Location(location)))
        # Super
        super(ResourceService, self).__init__(serviceName or name, endpoints)

    def __resourcerequest__(self, location, feature, params = None):
        """Handle resource request
        Parameters:
            location                            The ResourceLocation object
            feature                             The feature name
            params                              The parameters
        Returns:
            The return result object
        """
        # TODO:
        #   Add configuration pipeline support
        return self.manager.invokeFeature(feature, params)

    def watch(self):
        """Watch this resource
        """
        body = context.request.content.data
        if body:
            # Get condition from body
            query = body.pop('query', None)
            if query:
                try:
                    condition = loadCondition(query)
                except DataHubError as error:
                    raise BadRequestError(reason = 'Invalid query [%s]' % error)
            # Check body
            if body:
                raise BadRequestError(reason = 'Invalid body')
        else:
            condition = None
        # Start watch
        for changeSet in self.manager.watch(condition):
            yield '%s\n' % json.dumps(changeSet.dump(), ensure_ascii = False).encode('utf8')

    def createEndpoints4Location(self, location):
        """Create the endpoint for a feature
        Returns:
            Yield of (name, Endpoint obejct)
        """
        # The watch feature endpoint
        if location.enableWatch:
            yield '_watch', post(path = location.path + '/%s/_watch' % self.name)(
                mimetype('text/plain')(container(PlainContentContainer)(endpoint()(self.watch)))
                )
        # Create the handlers
        handlers = {}
        # The exist feature
        if not location.features or 'store.exist' in location.features or 'store.exists' in location.features:
            handler = StoreExistFeatureEndpointHandler(self, location)
            handlers['store.exist'] = handler
            handlers['store.exists'] = handler
            yield '_store.exist', head(path = location.path + '/%s/<id>' % self.name)(endpoint()(handler))
        if not location.features or 'query.exists' in location.features:
            handler = QueryExistsFeatureEndpointHandler(self, location)
            handlers['query.exists'] = handler
        # The get feature
        if not location.features or 'store.get' in location.features or 'store.gets' in location.features:
            handler = StoreGetFeatureEndpointHandler(self, location)
            handlers['store.get'] = handler
            handlers['store.gets'] = handler
            yield '_store.get', get(path = location.path + '/%s/<id>' % self.name)(endpoint()(handler))
        if not location.features or 'store.getall' in location.features:
            handler = StoreGetAllFeatureEndpointHandler(self, location)
            handlers['store.getall'] = handler
            yield '_store.getall', get(path = location.path + '/%s' % self.name)(endpoint()(handler))
        if not location.features or 'query.gets' in location.features:
            handler = QueryGetsFeatureEndpointHandler(self, location)
            handlers['query.gets'] = handler
        # The create feature
        if not location.features or 'store.create' in location.features:
            handler = StoreCreateFeatureEndpointHandler(self, location)
            handlers['store.create'] = handler
            yield '_store.create', post(path = location.path + '/%s' % self.name)(endpoint()(handler))
        # The replace feature
        if not location.features or 'store.replace' in location.features:
            handler = StoreReplaceFeatureEndpointHandler(self, location)
            handlers['store.replace'] = handler
            yield '_store.replace', put(path = location.path + '/%s' % self.name)(endpoint()(handler))
        # The update feature
        if not location.features or 'store.update' in location.features or 'store.updates' in location.features:
            handler = StoreUpdateFeatureEndpointHandler(self, location)
            handlers['store.update'] = handler
            handlers['store.updates'] = handler
            yield '_store.update', patch(path = location.path + '/%s/<id>' % self.name)(endpoint()(handler))
        if not location.features or 'query.updates' in location.features:
            handler = QueryUpdatesFeatureEndpointHandler(self, location)
            handlers['query.updates'] = handler
        # The delete feature
        if not location.features or 'store.delete' in location.features or 'store.deletes' in location.features:
            handler = StoreDeleteFeatureEndpointHandler(self, location)
            handlers['store.delete'] = handler
            handlers['store.deletes'] = handler
            yield '_store.delete', delete(path = location.path + '/%s/<id>' % self.name)(endpoint()(handler))
        if not location.features or 'query.deletes' in location.features:
            handler = QueryDeletesFeatureEndpointHandler(self, location)
            handlers['query.deletes'] = handler
        # The count feature
        if not location.features or 'store.count' in location.features:
            handler = StoreCountFeatureEndpointHandler(self, location)
            handlers['store.count'] = handler
        if not location.features or 'store.countall' in location.features:
            handler = StoreCountAllFeatureEndpointHandler(self, location)
            handlers['store.countall'] = handler
        if not location.features or 'query.count' in location.features:
            handler = QueryCountFeatureEndpointHandler(self, location)
            handlers['query.count'] = handler
        # The general feature endpoint
        if location.enableGeneralFeature:
            yield '_feature', post(path = location.path + '/%s/_feature/<featureName>' % self.name)(endpoint()(GeneralFeatureEndpointHandler(self, location, handlers)))

class FeatureEndpointHandler(object):
    """The feature endpoint handler
    """
    def __init__(self, service, location):
        """Create a new FeatureEndpointHandler
        """
        self.service = service
        self.location = location

    def raiseFeatureNotSupported(self, name):
        """Raise feature not supported error
        """
        raise BadRequestError(reason = 'Feature [%s] not supported' % name)

    def getIDs(self, params):
        """Get and remove ids
        """
        if not 'id' in params:
            raise BadRequestError(reason = 'Require id')
        ids = filter(lambda x: x, map(lambda x: x.strip(), params.pop('id').split(',')))
        if not ids:
            raise BadRequestError(reason = 'Require id')
        # Done
        return ids

    def getSorts(self, params):
        """Get and remove sorts
        """
        if params and 'sorts' in params:
            sorts = params.pop('sorts')
            if not isinstance(sorts, (list, tuple)):
                raise BadRequestError(reason = 'Invalid sorts')
            sortRules = []
            for sort in sorts:
                if not isinstance(sort, dict):
                    raise BadRequestError(reason = 'Invalid sorts')
                key, ascending = sort.get('key'), sort.get('ascending', True)
                if not key:
                    raise BadRequestError(reason = 'Invalid sorts, require key')
                # Add this rule
                sortRules.append(SortRule(key = key, ascending = ascending))
            # Done
            return sortRules

    def getQueryCondition(self, params):
        """Get the query condition from params
        """
        query = params.pop('query', None)
        if query:
            try:
                return loadCondition(query)
            except DataHubError as error:
                raise BadRequestError(reason = 'Invalid query [%s]' % error)

    def getConditionsFromParams(self, params):
        """Get conditions from params
        Returns:
            A list of conditions
        """
        # Found other parameters
        if not self.location.params:
            raise BadRequestError
        # Get conditions
        conditions = []
        for key, value in params.iteritems():
            if not key in self.location.params:
                raise BadRequestError(reason = 'Unknown parameter [%s]' % key)
            conditions.append(self.location.params[key].getCondition(value))
        # Done
        return conditions

class StoreExistFeatureEndpointHandler(FeatureEndpointHandler):
    """The store.exist / store.exists feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The exist api entry
        """
        # Get id or ids
        ids = self.getIDs(kwargs)
        # Check other parameters
        if not kwargs:
            # No more parameters, call store.exist or store.exists
            if len(ids) == 1:
                if self.location.features and not 'store.exist' in self.location.features:
                    self.raiseFeatureNotSupported('store.exist')
                return self.service.__resourcerequest__(self.location, 'store.exist', dict(id = ids[0]))
            else:
                if self.location.features and not 'store.exists' in self.location.features:
                    self.raiseFeatureNotSupported('store.exists')
                return self.service.__resourcerequest__(self.location, 'store.exists', dict(ids = ids))
        else:
            if self.location.features and not 'query.exists' in self.location.features:
                self.raiseFeatureNotSupported('query.exists')
            conditions = self.getConditionsFromParams(kwargs)
            conditions.append(KeyValueCondition(key = '_id', value = ids[0]) if len(ids) == 1 else KeyValuesCondition(key = '_id', values = ids))
            # Call the feature
            return self.service.__resourcerequest__(self.location, 'query.exists', dict(condition = AndCondition(conditions = conditions)))

class QueryExistsFeatureEndpointHandler(FeatureEndpointHandler):
    """The query exists feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The query exists feature api entry
        """
        # Get query from body
        body = context.request.content.data
        if not body:
            raise BadRequestError(reason = 'Require body')
        # Get the query
        condition = self.getQueryCondition(body)
        # Check body
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Check the kwargs
        if kwargs:
            conditions = self.getConditionsFromParams(kwargs)
            if condition:
                conditions.append(condition)
            condition = AndCondition(conditions = conditions)
        # Call the feature
        return self.service.__resourcerequest__(self.location, 'query.exists', dict(condition = condition))

class StoreGetFeatureEndpointHandler(FeatureEndpointHandler):
    """The store.get / store.gets feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The get api entry
        """
        # Get id or ids
        ids = self.getIDs(kwargs)
        # Get sorts
        sorts = self.getSorts(context.request.content.data)
        # Check other parameters
        if not kwargs:
            # No more parameters, call store.get or store.gets
            if len(ids) == 1:
                if self.location.features and not 'store.get' in self.location.features:
                    self.raiseFeatureNotSupported('store.get')
                model = self.service.__resourcerequest__(self.location, 'store.get', dict(id = ids[0]))
                if not model:
                    raise NotFoundError
                return model.dump()
            else:
                if self.location.features and not 'store.gets' in self.location.features:
                    self.raiseFeatureNotSupported('store.gets')
                # Call the feature
                return [ x.dump() for x in self.service.__resourcerequest__(self.location, 'store.gets', dict(ids = ids, sorts = sorts)) ]
        else:
            # Found other parameters
            if self.location.features and not 'query.gets' in self.location.features:
                self.raiseFeatureNotSupported('query.gets')
            # Get conditions
            conditions = self.getConditionsFromParams(kwargs)
            conditions.append(KeyValueCondition(key = '_id', value = ids[0]) if len(ids) == 1 else KeyValuesCondition(key = '_id', values = ids))
            # Call the features
            models = [ x.dump() for x in self.service.__resourcerequest__(
                self.location,
                'query.gets',
                dict(condition = AndCondition(conditions = conditions), sorts = sorts)
                )]
            # Make the same behavior
            if len(ids) == 1:
                if models:
                    return models[0]
                else:
                    raise NotFoundError
            else:
                return models

class StoreGetAllFeatureEndpointHandler(FeatureEndpointHandler):
    """The store.getall feature
    """
    def __call__(self, **kwargs):
        """The get api entry
        """
        body = context.request.content.data
        if body:
            # Get sorts
            sorts = self.getSorts(body)
            # Get start & size
            start = body.pop('start', 0)
            size = body.pop('size', 10)
            # Check the body
            if body:
                raise BadRequestError(reason = 'Invalid body')
        else:
            sorts = None
            start = 0
            size = 10
        # Check other parameters
        if not kwargs:
            if self.location.features and not 'store.getall' in self.location.features:
                self.raiseFeatureNotSupported('store.getall')
            # Call the feature
            return [ x.dump() for x in self.service.__resourcerequest__(self.location, 'store.getall', dict(sorts = sorts, start = start, size = size)) ]
        else:
            # Found other parameters
            if self.location.features and not 'query.gets' in self.location.features:
                self.raiseFeatureNotSupported('query.gets')
            # Get conditions
            conditions = self.getConditionsFromParams(kwargs)
            # Call
            return [ x.dump() for x in self.service.__resourcerequest__(
                self.location,
                'query.gets',
                dict(condition = AndCondition(conditions = conditions), sorts = sorts, start = start, size = size)
                )]

class QueryGetsFeatureEndpointHandler(FeatureEndpointHandler):
    """The query gets feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The query gets feature api entry
        """
        body = context.request.content.data
        # Get query from body
        if not body:
            raise BadRequestError(reason = 'Require body')
        # Get query
        condition = self.getQueryCondition(body)
        # Get sorts from body
        sorts = self.getSorts(body)
        # Get start & size
        start = body.pop('start', 0)
        size = body.pop('size', 10)
        # Check the body
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Call the feature
        if kwargs:
            # Get the condition
            conditions = self.getConditionsFromParams(kwargs)
            if condition:
                conditions.append(condition)
            condition = AndCondition(conditions = conditions)
        # Call
        return [ x.dump() for x in self.service.__resourcerequest__(
                self.location,
                'query.gets',
                dict(condition = condition, sorts = sorts, start = start, size = size)
                )]

class StoreCreateFeatureEndpointHandler(FeatureEndpointHandler):
    """The store.create feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The create api entry
        """
        if self.location.features and not 'store.create' in self.location.features:
            self.raiseFeatureNotSupported('store.create')
        # Get the post body and parameters
        if not context.request.content.data:
            raise BadRequestError(reason = 'Require body')
        body = context.request.content.data
        model, overwrite, configs = body.get('model'), body.get('overwrite', False), body.get('configs')
        if not model:
            raise BadRequestError(reason = 'Require model')
        # Create the model object
        model = self.service.manager.modelClass(model)
        # Check parameters
        if not kwargs:
            # No more parameters, call store.create
            return self.service.__resourcerequest__(self.location, 'store.create', dict(model = model, overwrite = overwrite, configs = configs)).dump()
        else:
            # Found other parameters
            if not self.location.params:
                raise BadRequestError
            # Set model
            for key, value in kwargs.iteritems():
                if not key in self.location.params:
                    raise BadRequestError(reason = 'Unknown parameter [%s]' % key)
                self.location.params[key].setModel(model, value)
            return self.service.__resourcerequest__(self.location, 'store.create', dict(model = model, overwrite = overwrite, configs = configs)).dump()

class StoreReplaceFeatureEndpointHandler(FeatureEndpointHandler):
    """The store.replace feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The replace api entry
        """
        if self.location.features and not 'store.replace' in self.location.features:
            self.raiseFeatureNotSupported('store.replace')
        # Get the post body and parameters
        if not context.request.content.data:
            raise BadRequestError(reason = 'Require body')
        body = context.request.content.data
        model, configs = body.get('model'), body.get('configs')
        try:
            # Check other parameters
            if not kwargs:
                # No more parameters, call store.replace
                return self.service.__resourcerequest__(self.location, 'store.replace', dict(model = model, configs = configs)).dump()
            else:
                # Found other parameters
                if not self.location.params:
                    raise BadRequestError
                # Set model
                for key, value in kwargs.iteritems():
                    if not key in self.location.params:
                        raise BadRequestError(reason = 'Unknown parameter [%s]' % key)
                    self.location.params[key].setModel(model, value)
                return self.service.__resourcerequest__(self.location, 'store.replace', dict(model = model, configs = configs)).dump()
        except ModelNotFoundError:
            # The model not found
            raise NotFoundError

class StoreUpdateFeatureEndpointHandler(FeatureEndpointHandler):
    """The store.update / store.updates feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The update api entry
        """
        # Get id or ids
        ids = self.getIDs(kwargs)
        # Get the post body and parameters
        if not context.request.content.data:
            raise BadRequestError(reason = 'Require body')
        body = context.request.content.data
        updates, configs = body.get('updates'), body.get('configs')
        # Load updates
        try:
            updates = [ loadUpdateAction(x) for x in updates ]
        except DataHubError as error:
            raise BadRequestError(reason = 'Data error [%s]' % error)
        # Check other parameters
        if not kwargs:
            # No more parameters, call store.update or store.updates
            if len(ids) == 1:
                if self.location.features and not 'store.update' in self.location.features:
                    self.raiseFeatureNotSupported('store.update')
                try:
                    return self.service.__resourcerequest__(self.location, 'store.update', dict(id = ids[0], updates = updates, configs = configs)).dump()
                except ModelNotFoundError:
                    # The model not found
                    raise NotFoundError
            else:
                if self.location.features and not 'store.updates' in self.location.features:
                    self.raiseFeatureNotSupported('store.updates')
                return self.service.__resourcerequest__(self.location, 'store.updates', dict(ids = ids, updates = updates, configs = configs)).dump()
        else:
            # Found other parameters
            if not self.location.params:
                raise BadRequestError
            if self.location.features and not 'query.updates' in self.location.features:
                self.raiseFeatureNotSupported('query.updates')
            # Get conditions
            conditions = [ KeyValueCondition(key = '_id', value = ids[0]) ] if len(ids) == 1 else [ KeyValuesCondition(key = '_id', values = ids) ]
            for key, value in kwargs.iteritems():
                if not key in self.location.params:
                    raise BadRequestError(reason = 'Unknown parameter [%s]' % key)
                conditions.append(self.location.params[key].getCondition(value))
            # Update
            return self.service.__resourcerequest__(self.location, 'query.updates', dict(
                condition = AndCondition(conditions = conditions),
                updates = updates,
                configs = configs
                )).dump()

class QueryUpdatesFeatureEndpointHandler(FeatureEndpointHandler):
    """The query updates feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The query updates feature api entry
        """
        body = context.request.content.data
        # Get query from body
        if not body:
            raise BadRequestError(reason = 'Require body')
        # Get condition
        condition = self.getQueryCondition(body)
        # Get updates
        updates = body.pop('updates', None)
        if not updates or not isinstance(updates, (list, tuple)):
            raise BadRequestError(reason = 'Invalid updates')
        updates = [ loadUpdateAction(x) for x in updates ]
        # Get configs
        configs = body.get('configs')
        # Check the body
        if body:
            raise BadRequestError(reason = 'Invalid body')
        if kwargs:
            conditions = self.getConditionsFromParams(kwargs)
            if condition:
                conditions.append(condition)
            condition = AndCondition(conditions = conditions)
        # Call the feature
        return self.service.__resourcerequest__(self.location, 'query.updates', dict(
            condition = condition,
            updates = updates,
            configs = configs
            )).dump()

class StoreDeleteFeatureEndpointHandler(FeatureEndpointHandler):
    """The store.delete / store.deletes feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The delete api entry
        """
        # Get id or ids
        ids = self.getIDs(kwargs)
        # Check other parameters
        if not kwargs:
            # No more parameters, call store.delete or store.deletes
            if len(ids) == 1:
                if self.location.features and not 'store.delete' in self.location.features:
                    self.raiseFeatureNotSupported('store.delete')
                model = self.service.__resourcerequest__(self.location, 'store.delete', dict(id = ids[0]))
                return model.dump() if model else None
            else:
                if self.location.features and not 'store.deletes' in self.location.features:
                    self.raiseFeatureNotSupported('store.deletes')
                return self.service.__resourcerequest__(self.location, 'store.deletes', dict(ids = ids)).dump()
        else:
            # Found other parameters
            if self.location.features and not 'query.deletes' in self.location.features:
                self.raiseFeatureNotSupported('query.deletes')
            # Get conditions
            conditions = self.getConditionsFromParams(kwargs)
            conditions.append(KeyValueCondition(key = '_id', value = ids[0]) if len(ids) == 1 else KeyValuesCondition(key = '_id', values = ids))
            # Call
            return self.service.__resourcerequest__(self.location, 'query.deletes', dict(condition = AndCondition(conditions = conditions))).dump()

class QueryDeletesFeatureEndpointHandler(FeatureEndpointHandler):
    """The query deletes feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The query updates feature api entry
        """
        body = context.request.content.data
        # Get query from body
        if not body:
            raise BadRequestError(reason = 'Require body')
        # Get condition
        condition = self.getQueryCondition(body)
        # Get configs
        configs = body.get('configs')
        # Check the body
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Get conditions
        if kwargs:
            conditions = self.getConditionsFromParams(kwargs)
            if condition:
                conditions.append(condition)
            condition = AndCondition(conditions = conditions)
        # Call the feature
        return self.service.__resourcerequest__(self.location, 'query.deletes', dict(condition = condition, configs = configs)).dump()

class StoreCountFeatureEndpointHandler(FeatureEndpointHandler):
    """The store count feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The query updates feature api entry
        """
        # Get id or ids
        ids = self.getIDs(kwargs)
        # Check other parameters
        if not kwargs:
            if self.location.features and not 'store.count' in self.location.features:
                self.raiseFeatureNotSupported('store.count')
            return self.service.__resourcerequest__(self.location, 'store.count', dict(ids = ids))
        else:
            # Found other parameters
            if self.location.features and not 'query.count' in self.location.features:
                self.raiseFeatureNotSupported('query.count')
            # Get conditions
            conditions = self.getConditionsFromParams(kwargs)
            conditions.append(KeyValueCondition(key = '_id', value = ids[0]) if len(ids) == 1 else KeyValuesCondition(key = '_id', values = ids))
            # Call
            return self.service.__resourcerequest__(self.location, 'query.count', dict(condition = AndCondition(conditions = conditions)))

class QueryCountFeatureEndpointHandler(FeatureEndpointHandler):
    """The query count feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The query updates feature api entry
        """
        body = context.request.content.data
        # Get query from body
        if not body:
            raise BadRequestError(reason = 'Require body')
        # Get condition
        condition = self.getQueryCondition(body)
        # Check the body
        if body:
            raise BadRequestError(reason = 'Invalid body')
        # Get conditions
        if kwargs:
            conditions = self.getConditionsFromParams(kwargs)
            if condition:
                conditions.append(condition)
            condition = AndCondition(conditions = conditions)
        # Call the feature
        return self.service.__resourcerequest__(self.location, 'query.count', dict(condition = condition))

class StoreCountAllFeatureEndpointHandler(FeatureEndpointHandler):
    """The store count all feature endpoint handler
    """
    def __call__(self, **kwargs):
        """The query updates feature api entry
        """
        # Check other parameters
        if not kwargs:
            if self.location.features and not 'store.countall' in self.location.features:
                self.raiseFeatureNotSupported('store.countall')
            return self.service.__resourcerequest__(self.location, 'store.countall')
        else:
            # Found other parameters
            if self.location.features and not 'query.count' in self.location.features:
                self.raiseFeatureNotSupported('query.count')
            # Get conditions
            conditions = self.getConditionsFromParams(kwargs)
            # Call
            return self.service.__resourcerequest__(self.location, 'query.count', dict(condition = AndCondition(conditions = conditions)))

class GeneralFeatureEndpointHandler(FeatureEndpointHandler):
    """The general feature endpoint handler
    """
    def __init__(self, service, location, handlers):
        """Create a new FeatureEndpointHandler
        """
        self.handlers = handlers
        # Super
        super(GeneralFeatureEndpointHandler, self).__init__(service, location)

    def __call__(self, featureName, **kwargs):
        """The general feature api entry
        """
        if not featureName in self.handlers:
            raise BadRequestError(reason = 'Feature [%s] not supported' % featureName)
        # Call the handler
        return self.handlers[featureName](**kwargs)
