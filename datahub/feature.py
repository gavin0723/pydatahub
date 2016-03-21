# encoding=utf8

""" The feature utility
    Author: lipixun
    Created Time : 二  3/15 16:19:43 2016

    File Name: feature.py
    Description:

"""

from functools import partial

class Feature(object):
    """A feature
    """
    def __init__(self, name, params = None, method = None, implemented = False):
        """Create a new FeatureDescriptor
        """
        self._name = name
        self._params = params
        self._method = method
        self._implemented = implemented

    def __get__(self, instance ,cls):
        """Use this feature as a descriptor
        """
        if not instance:
            return self
        else:
            return partial(self._method, instance)

    @property
    def name(self):
        """Get the name
        """
        return self._name

    @property
    def params(self):
        """Get the parameters
        """
        return self._params

    @property
    def isImplemented(self):
        """Tell if the feature is implemented
        """
        return self._implemented

    def implement(self, method):
        """Implement this feature
        """
        return Feature(self.name, self.params, method, True)

class FeatureMetaClass(type):
    """The feature meta class
    """
    def __new__(cls, name, bases, attrs):
        """Create a new DataModel object
        """
        features = {}
        featureMethods = {}
        for base in bases:
            if hasattr(base, 'FEATURE_METHODS'):
                featureMethods.update(getattr(base, 'FEATURE_METHODS'))
            if hasattr(base, 'features'):
                features.update(map(lambda x: (x.name, x), getattr(base, 'features')))
        for key, field in attrs.iteritems():
            if isinstance(field, Feature):
                featureMethods[field.name] = key
                features[field.name] = field
        # Add to attrs
        attrs['FEATURE_METHODS'] = featureMethods
        attrs['features'] = features.values()
        # Super
        return type.__new__(cls, name, bases, attrs)

def feature(**kwargs):
    """Feature decorator
    """
    def decorate(method):
        """Decorate
        """
        return Feature(method = method, **kwargs)
    return decorate
