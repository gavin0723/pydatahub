# encoding=utf8

""" The repository test
    Author: lipixun
    Created Time : 二  3/15 18:31:18 2016

    File Name: repository.py
    Description:

"""

from datahub.repository import Repository

def test_repository_basic():
    """The the repository basic
    """
    for feature in Repository.features:
        assert not feature.isImplemented
