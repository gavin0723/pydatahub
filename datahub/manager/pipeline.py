# encoding=utf8

""" The manage invoke pipeline
    Author: lipixun
    Created Time : 三  4/13 17:23:51 2016

    File Name: pipeline.py
    Description:

"""

from types import MethodType

class FeatureInvokePipeline(object):
    """The feature invoke pipeline
    """
    def __init__(self, pipelines = None):
        """Create a new FeatureInvokePipeline
        """
        self.pipelines = pipelines or {}    # The pipelines:
                                            #   Key is feature name value is a list of tuple (handler, weight)
                                            #   Key is None means the global handlers

    def addHandler(self, handler):
        """Add a handler
        """
        if handler.hooks:
            for featureName, weight in handler.hooks:
                if not featureName in self.pipelines:
                    self.pipelines[featureName] = []
                self.pipelines[featureName].append((handler, weight))

    def removeHandler(self, handler):
        """Remove a handler
        """
        if handler.hooks:
            for featureName in handler.hooks.iterkeys():
                if featureName in self.pipelines:
                    self.pipelines.remove(handler)

    def clone(self):
        """Clone a new FeatureInvokePipeline
        """
        pipelines = {}
        for key, handlers in self.pipelines.iteritems():
            pipelines[key] = list(handlers)
        # Done
        return FeatureInvokePipeline(pipelines)

    def __call__(self, featureName, params, final, manager):
        """Call a feature in this pipeline
        """
        # Get handlers for this feature
        handlers = []   # A list of tuple (handler, weight)
        if featureName in self.pipelines:
            handlers.extend(self.pipelines[featureName])
        if None in self.pipelines:
            handlers.extend(self.pipelines[None])
        # Run the handler
        return FeaturePipelineExecutor(featureName, list(map(lambda x: x[0], sorted(handlers, key = lambda x: x[1], reverse = True))), params, final, manager)

class FeatureInvokeHandler(object):
    """The feature invoke handler
    """
    def __init__(self, method, hooks, shouldBind = False):
        """The feature invoke handler
        Parameters:
            method                          The method, should have parameters: (feature, params, manager, next)
            hooks                           The hooks a list of tuple (feature name, weight), if feature name is None, means hooks to all features
        """
        self.method = method
        self.hooks = hooks
        self.shouldBind = shouldBind

class FeaturePipelineExecutor(object):
    """The feature pipeline executor
    """
    def __init__(self, feature, handlers, params, final, manager):
        """Create a new FeaturePipelineExecutor
        """
        self.index = -1
        self.feature = feature
        self.handlers = handlers
        self.params = params
        self.final = final
        self.manager = manager

    def __call__(self):
        """Call this executor
        """
        self.index += 1
        # Check if we have already executed all handlers
        if self.index >= len(self.handlers):
            # Execute the final handler
            return self.final(self.feature, self.params, self.manager, None)
        else:
            # Execute the next handler
            handler = self.handlers[self.index]
            # Bind the method or not
            if handler.shouldBind:
                method = MethodType(handler.method, self.manager)
            else:
                method = handler.method
            # Call the handler method
            return method(self.feature, self.params, self.manager, self.__call__)
