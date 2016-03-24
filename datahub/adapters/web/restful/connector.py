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

    def getUrl(self, paths):
        """Get the url path
        """
        return '%s://%s:%d/%s' % (self.scheme, self.host, self.port, '/'.join([ quote_plus(x) for x in paths ]))

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

    def __init__(self, name, modelClass, connection, paths = None):
        """Create a new ResourceConnector
        Parameters:
            name                            The resource name
            modelClass                      The resource model class
            connection                      The connection instance
            paths                           The path list of this resource
        """
        self.name = name
        self.modelClass = modelClass
        self.connection = connection
        self.paths = paths or []

    def handleError(self, response):
        """Handle the error response
        """
        response.raise_for_status()

    def getByID(self, id, paths = tuple()):
        """Get resource by id
        """
        # Send request
        _paths = list(self.paths)
        _paths.extend(paths)
        _paths.append(self.name)
        _paths.append(id)
        rsp = self.connection.get(*_paths)
        if rsp.status_code == 404:
            return
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Load the model
        model = self.modelClass(_json.loads(rsp.content)['value'])
        model.validate()
        # Done
        return model

    def getsByID(self, ids, paths = tuple()):
        """Get resources by id
        """
        # Send request
        _paths = list(self.paths)
        _paths.extend(paths)
        _paths.append(self.name)
        _paths.append(','.join(ids))
        rsp = self.connection.get(*_paths)
        if rsp.status_code == 404:
            return
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Load the model
        models = [ self.modelClass(x) for x in _json.loads(rsp.content)['value'] ]
        for model in models:
            model.validate()
        # Done
        return models

    def getsByQuery(self, condition = None, start = 0, size = 10, sorts = None, paths = tuple()):
        """Get by query
        """
        raise NotImplementedError

    def watch(self, condition = None, paths = tuple()):
        """Watch the resource
        Returns:
            Yield of ResourceWatchChangeSet
        """
        _paths = list(self.paths)
        _paths.extend(paths)
        _paths.append(self.name)
        _paths.append('_watch')
        with closing(self.connection.post(*_paths, json = condition.dump() if condition else {}, stream = True, timeout = None)) as rsp:
            # Check header
            if rsp.status_code == 404:
                return
            elif rsp.status_code != 200:
                self.handleError(rsp)
            # Iterate reading content
            for line in rsp.iter_lines():
                jsonObj = _json.loads(line.strip().decode('utf8'))
                # It's a little tricky to load the change set
                if jsonObj.get('model'):
                    jsonObj['model'] = self.modelClass(jsonObj['model'])
                # Load the changeset
                changeSet = ResourceWatchChangeSet(jsonObj)
                changeSet.validate()
                yield changeSet
