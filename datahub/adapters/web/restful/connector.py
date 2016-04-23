# encoding=utf8

""" The restul resource connector
    Author: lipixun
    Created Time : 三  3/23 11:03:58 2016

    File Name: connector.py
    Description:

"""

import logging

from urllib import quote_plus
from contextlib import closing

import requests

from datahub.utils import json as _json
from datahub.manager import ResourceWatchChangeSet

class Connection(object):
    """The connector connection
    """
    def __init__(self, host, port, scheme = 'http', cert = None, key = None, verify = None, session = None):
        """Create a new Connection
        Parameters:
            host                            The restful web service host
            port                            The restful web service port
            scheme                          The request scheme
            cert                            The certificate file path in PEM format when using ssl client certificate authentication
            key                             The private key file path in PEM format when using ssl client certificate authentication
            verify                          Whether verify the service certificate or not if using https or the path to the certificate
            session                         The request session object
        """
        self.host = host
        self.port = port
        if scheme != 'http' and scheme != 'https':
            raise ValueError('Invalid scheme [%s], could either be http or https' % scheme)
        self.scheme = scheme
        self.cert = cert
        self.key = key
        self.verify = verify
        # Use or create the request session
        self.session = session or requests.session()

    def getUrl(self, paths = None):
        """Get the url path
        """
        if paths:
            return '%s://%s:%d/%s' % (self.scheme, self.host, self.port, '/'.join([ quote_plus(x) for x in paths ]))
        else:
            return '%s://%s:%d' % (self.scheme, self.host, self.port)

    def head(self, *paths, **kwargs):
        """send a head request
        Returns:
            requests.Response object
        """
        return self.request('HEAD', self.getUrl(paths), **kwargs)

    def get(self, *paths, **kwargs):
        """Send a get request
        Returns:
            requests.Response object
        """
        return self.request('GET', self.getUrl(paths), **kwargs)

    def put(self, *paths, **kwargs):
        """Send a put request
        Returns:
            requests.Response object
        """
        return self.request('PUT', self.getUrl(paths), **kwargs)

    def post(self, *paths, **kwargs):
        """Send a post request
        Returns:
            requests.Response object
        """
        return self.request('POST', self.getUrl(paths), **kwargs)

    def patch(self, *paths, **kwargs):
        """send a patch request
        Returns:
            requests.Response object
        """
        return self.request('PATCH', self.getUrl(paths), **kwargs)

    def delete(self, *paths, **kwargs):
        """send a delete request
        Returns:
            requests.Response object
        """
        return self.request('DELETE', self.getUrl(paths), **kwargs)

    def request(self, method, url, json = None, **kwargs):
        """Send a request
        Parameters:
            method                                  The request method
            url                                     The request url
            json                                    The json object to send as payload
        Returns:
            requests.Response object
        """
        # Do json serialize if json
        if json:
            if kwargs.get('data'):
                raise ValueError('Cannot set json and data at the same time')
            # Serialize json to data
            kwargs['data'] = _json.dumps(json, ensure_ascii = False).encode('utf8')
            # Set header
            if kwargs.get('headers'):
                kwargs['headers']['Content-Type'] = 'application/json; charset=utf-8'
            else:
                kwargs['headers'] = { 'Content-Type': 'application/json; charset=utf-8' }
        # Send request
        if self.scheme == 'http':
            # Http
            return self.session.request(method, url, **kwargs)
        else:
            # Https
            if not 'verify' in kwargs and not self.verify is None:
                kwargs['verify'] = verify
            if not 'cert' in kwargs and self.cert and self.key:
                kwargs['cert'] = (self.cert, self.key)
            # Send
            return self.session.request(method, url, **kwargs)

class ResourceConnector(object):
    """The resource connector
    """
    logger = logging.getLogger('datahub.adapters.web.restful.resourceConnector')

    def __init__(self, path, modelClass, connection):
        """Create a new ResourceConnector
        Parameters:
            path                            The resource path
            modelClass                      The resource model class
            connection                      The connection instance
        """
        # Set attributes
        self.path = path
        self.modelClass = modelClass
        self.connection = connection

    def handleError(self, response):
        """Handle the error response
        """
        response.raise_for_status()

    def buildPaths(self, paths, kwargs):
        """Build the paths
        """
        if isinstance(self.path, basestring):
            # A string path
            if not kwargs:
                raise ValueError('Do not support additional path parameters')
            # Good
            return [ self.path ] + paths
        elif isinstance(self.path, (list, tuple)):
            # A list of string
            if not kwargs:
                raise ValueError('Do not support additional path parameters')
            # Good
            return list(self.path) + paths
        elif callable(self.path):
            # A custom builder
            return self.path(paths, kwargs)
        else:
            raise ValueError('Invalid path')

    def getByID(self, id, **kwargs):
        """Get resource by id
        """
        rsp = self.connection.get(*self.buildPaths([ id ], kwargs))
        if rsp.status_code == 404:
            return
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Load the model
        model = self.modelClass(_json.loads(rsp.content)['value'])
        model.validate()
        # Done
        return model

    def getsByID(self, ids, **kwargs):
        """Get resources by id
        """
        rsp = self.connection.get(*self.buildPaths([ ','.join(ids) ], kwargs))
        if rsp.status_code == 404:
            return
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Load the models
        models = [ self.modelClass(x) for x in _json.loads(rsp.content)['value'] ]
        for model in models:
            model.validate()
        # Done
        return models

    def getsByQuery(self, condition = None, start = 0, size = 10, sorts = None, **kwargs):
        """Get by query
        """
        # Generate the payload
        body = {
            'start': start,
            'size': size,
        }
        if condition:
            body['query'] = condition.dumpAsRoot()
        if sorts:
            body['sorts'] = [ x.dump() for x in sorts ]
        # Send request
        rsp = self.connection.post(*self.buildPaths([ '_feature', 'query.gets' ], kwargs), json = body)
        if rsp.status_code == 404:
            return
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Load the models
        models = [ self.modelClass(x) for x in _json.loads(rsp.content)['value'] ]
        for model in models:
            model.validate()
        # Done
        return models

    def create(self, model, overwrite = False, configs = None, **kwargs):
        """Create a model
        """
        # Create the body
        body = {
            'model': model.dump(),
            'overwrite': overwrite
        }
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.post(*self.buildPaths([], kwargs), json = body)
        if rsp.status_code != 200:
            self.handleError(rsp)
        # Load the created model
        model = self.modelClass(_json.loads(rsp.content)['value'])
        model.validate()
        # Done
        return model

    def replace(self, model, configs = None, **kwargs):
        """Replace a model
        """
        # Create the body
        body = {
            'model': model.dump()
        }
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.put(*self.buildPaths([], kwargs), json = body)
        if rsp.status_code != 200:
            self.handleError(rsp)
        # TODO: Load the replace result

    def updateByID(self, id, updates, configs = None, **kwargs):
        """Update a model
        """
        # Create the body
        body = {
            'updates': [ x.dumpAsRoot() for x in updates ]
        }
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.patch(*self.buildPaths([], kwargs), json = body)
        if rsp.status_code != 200:
            self.handleError(rsp)
        # TODO: Load the update result

    def deleteByID(self, id, configs = None, **kwargs):
        """Delete model by id
        """
        # TODO:
        #   Support send configs
        rsp = self.connection.delete(*self.buildPaths([ id ], kwargs))
        if rsp.status_code != 200:
            self.handleError(rsp)

    def watch(self, condition = None, **kwargs):
        """Watch the resource
        Returns:
            Yield of ResourceWatchChangeSet
        """
        # Keep watching changes
        with closing(self.connection.post(*self.buildPaths([], kwargs), json = { 'query': condition.dump() } if condition else {}, stream = True, timeout = None)) as rsp:
            # Check header
            if rsp.status_code == 404:
                return
            elif rsp.status_code != 200:
                self.handleError(rsp)
            # Iterate reading content
            for line in rsp.iter_lines():
                jsonObj = _json.loads(line.strip().decode('utf8'))
                # It's a little tricky to load the change set
                if jsonObj.get('newModel'):
                    jsonObj['newModel'] = self.modelClass(jsonObj['newModel'])
                if jsonObj.get('oldModel'):
                    jsonObj['oldModel'] = self.modelClass(jsonObj['oldModel'])
                # Load the changeset
                changeSet = ResourceWatchChangeSet(jsonObj)
                changeSet.validate()
                yield changeSet
