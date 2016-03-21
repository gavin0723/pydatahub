# encoding=utf8

""" The sorts
    Author: lipixun
    Created Time : 二  3/15 17:01:02 2016

    File Name: sorts.py
    Description:

"""

from datahub.model import DataModel, StringType, BooleanType

class SortRule(DataModel):
    """The sort rule
    """
    # The sort key
    key = StringType(required = True)
    # The sort oriention
    ascending = BooleanType(required = True, default = True)
