# encoding=utf8

""" A data processing pipeline
    Author: lipixun
    Created Time : 二  3/15 23:15:33 2016

    File Name: pipeline.py
    Description:

"""

class Pipeline(object):
    """The data processing pipeline
    """
    def __init__(self, *nodes):
        """Create a new Pipeline
        """
        self.nodes = nodes

    def __call__(self, context = None, final = None):
        """Run this pipeline
        """
        return PipelineRuntime(self.nodes, context, final)()

class PipelineRuntime(object):
    """The pipeline runtime object
    """
    def __init__(self, nodes, context, final):
        """Create a new PipelineRuntime
        """
        self.nodes = list(nodes)
        self.context = context
        self.final = final
        self.index = -1

    def __call__(self):
        """Run this pipeline
        """
        return self.next()

    def next(self):
        """Call next node
        """
        self.index += 1
        if self.index < len(self.nodes):
            # Run this node
            return self.nodes[self.index](self.context, self.next)
        elif self.final:
            # Run the final
            return self.final(self.context, self.next)

class PipeNode(object):
    """The pipe node
    """
    def __call__(self, context, next):
        """Run this node
        """
        raise NotImplementedError
