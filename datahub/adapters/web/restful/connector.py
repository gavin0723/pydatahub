# encoding=utf8

""" The restul resource connector
    Author: lipixun
    Created Time : 三  3/23 11:03:58 2016

    File Name: connector.py
    Description:

"""

import logging

from urllib import quote_plus

import requests

from datahub.spec import *
from datahub.utils import json as _json
from datahub.errors import ModelNotFoundError, DuplicatedKeyError

class Connection(object):
    """The connector connection
    """
    def head(self, path, **kwargs):
        """send a head request
        Returns:
            requests.Response object
        """
        return self.request('HEAD', path, **kwargs)

    def get(self, path, **kwargs):
        """Send a get request
        Returns:
            requests.Response object
        """
        return self.request('GET', path, **kwargs)

    def put(self, path, **kwargs):
        """Send a put request
        Returns:
            requests.Response object
        """
        return self.request('PUT', path, **kwargs)

    def post(self, path, **kwargs):
        """Send a post request
        Returns:
            requests.Response object
        """
        return self.request('POST', path, **kwargs)

    def patch(self, path, **kwargs):
        """send a patch request
        Returns:
            requests.Response object
        """
        return self.request('PATCH', path, **kwargs)

    def delete(self, path, **kwargs):
        """send a delete request
        Returns:
            requests.Response object
        """
        return self.request('DELETE', path, **kwargs)

    def request(self, method, path, json = None, **kwargs):
        """Send a request
        Parameters:
            method                                  The request method
            path                                    The request path
            json                                    The json object to send as payload
        Returns:
            requests.Response object
        """
        raise NotImplementedError

class HttpConnection(Connection):
    """The http connection based on requests
    """
    def __init__(self, host, port, scheme = 'http', cert = None, key = None, verify = True, session = None):
        """Create a new Connection
        Parameters:
            host                            The restful web service host
            port                            The restful web service port
            session                         The request session object
        """
        # Check scheme and parameters
        if scheme != 'http' and scheme != 'https':
            raise ValueError('Invalid scheme, must be either http or https')
        if cert and not key or not cert and key:
            raise ValueError('Must provide cert with key at the same time')
        if cert and scheme != 'https':
            raise ValueError('Scheme must be https when setting cert')
        # Set parameters
        self.host = host
        self.port = port
        self.scheme = scheme
        self.cert = cert
        self.key = key
        self.verify = verify
        self.session = session or requests.Session()

    def request(self, method, path, json = None, **kwargs):
        """Send a request
        Parameters:
            method                                  The request method
            path                                    The request path
            json                                    The json object to send as payload
        Returns:
            requests.Response object
        """
        if path and path.startswith('/'):
            path = path[1: ]
        url = '%s://%s:%d/%s' % (self.scheme, self.host, self.port, path or '')
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
        if self.scheme == 'https':
            # Set https config
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

    def __init__(self, cls, connection):
        """Create a new ResourceConnector
        Parameters:
            cls                             The resource model class
            connection                      The connection instance
        """
        self.cls = cls
        self.connection = connection

    def handleError(self, response):
        """Handle the error response
        """
        response.raise_for_status()

    def getFeatureUrl(self, url, feature):
        """Get the feature url
        """
        if url.endswith('/'):
            return '%s_feature/%s' % (url, feature)
        else:
            return '%s/_feature/%s' % (url, feature)

    def exist(self, url, id = None, query = None, configs = None):
        """Exist
        """
        if not id is None and query:
            raise ValueError('Cannot both specify id and query')
        # Create body
        body = {}
        if not id is None:
            body['id'] = id
        if query:
            body['query'] = query.dump()
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.post(self.getFeatureUrl(url, FEATURE_QUERY_EXIST), json = body)
        # Handle response
        if rsp.status_code == 404:
            return False
        elif rsp.status_code == 200:
            return True
        else:
                self.handleError(rsp)

    def get(self, url, id = None, query = None, start = 0, size = 0, sorts = None, configs = None, cls = None):
        """Get
        Parameters:
            url                                 The request url
            id                                  The id or a list / tuple of ids or None
            query                               The Condition object or None
        Returns:
            - For single id get request, returns None or A model object
            - Otherwise returns a list of models (May be empty)
        """
        if not id is None and query:
            raise ValueError('Cannot both specify id and query')
        # Create body
        body = {}
        if not id is None:
            body['id'] = id
        if query:
            body['query'] = query.dump()
        if not start is None:
            body['start'] = start
        if not size is None:
            body['size'] = size
        if sorts:
            body['sorts'] = [ x.dump() for x in sorts ]
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.get(self.getFeatureUrl(url, FEATURE_QUERY_GET), json = body)
        # Handle response
        if rsp.status_code == 404:
            return
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Load the model
        cls = cls or self.cls
        raw = _json.loads(rsp.content)['value']
        if isinstance(raw, list):
            # A list of result
            models = [ cls.load(x) for x in raw ]
            for model in models:
                model.validate()
            # Done
            return models
        else:
            # Single result
            model = cls.load(raw)
            model.validate()
            # Done
            return model

    def create(self, url, model, configs = None):
        """Create a model
        Parameters:
            url                             The request url
            model                           The model object
            configs                         A dict of configs
        Returns:
            Nothing
        """
        # Create the body
        body = {
            'model': model.dump()
        }
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.post(url, json = body)
        # Handle response
        if rsp.status_code == 400:
            # Try to decode the error
            try:
                if _json.loads(rsp.content)['error']['code'] == ERROR_DUPLICATED_KEY:
                    raise DuplicatedKeyError('Duplicate key found when creating model', model.id)
            except DuplicatedKeyError:
                raise
            else:
                self.handleError(rsp)
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Done

    def replace(self, url, model, configs = None):
        """Replace
        Returns:
            Nothing
        """
        # Create the body
        body = {
            'model': model.dump()
        }
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.put(url, json = body)
        # Handle response
        if rsp.status_code == 404:
            raise ModelNotFoundError
        elif rsp.status_code != 200:
            self.handleError(rsp)
        # Done

    def update(self, url, updates, id = None, query = None, configs = None):
        """Update a model
        Returns:
            The count of matched models
        """
        if not id is None and query:
            raise ValueError('Cannot both specify id and query')
        if id is None and not query:
            raise ValueError('Require id or query')
        # Create the body
        body = {
            'updates': [ x.dump() for x in updates ]
        }
        if not id is None:
            body['id'] = id
        if query:
            body['query'] = query.dump()
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.patch(url, json = body)
        # Handle response
        if rsp.status_code != 200:
            self.handleError(rsp)
        # Load the update result
        return _json.loads(rsp.content)['value']

    def delete(self, url, id = None, query = None, configs = None):
        """Delete model by id
        Returns:
            The count of deleted models
        """
        if not id is None and query:
            raise ValueError('Cannot both specify id and query')
        if id is None and not query:
            raise ValueError('Require id or query')
        # Create body
        body = {}
        if not id is None:
            body['id'] = id
        if query:
            body['query'] = query.dump()
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.delete(url, json = body)
        # Handle response
        if rsp.status_code != 200:
            self.handleError(rsp)
        # Load the update result
        return _json.loads(rsp.content)['value']

    def count(self, url, id = None, query = None, configs = None):
        """Count
        Returns:
            The count of counting models
        """
        if not id is None and query:
            raise ValueError('Cannot both specify id and query')
        # Create body
        body = {}
        if not id is None:
            body['id'] = id
        if query:
            body['query'] = query.dump()
        if configs:
            body['configs'] = configs
        # Send request
        rsp = self.connection.get(self.getFeatureUrl(url, FEATURE_QUERY_COUNT), json = body)
        if rsp.status_code != 200:
            self.handleError(rsp)
        # Done
        return _json.loads(rsp.content)['value']

    def watch(self, url, query = None, configs = None):
        """Watch the resource
        Returns:
            A tuple of (List of models, Watcher)
        """
        raise NotImplementedError
