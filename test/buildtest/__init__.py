# encoding=utf8

""" The build test package
    Author: lipixun
    Created Time : 五  3/18 10:26:17 2016

    File Name: __init__.py
    Description:

"""

import sys
import os
import __builtin__

from uuid import uuid4

from pymongo import MongoClient

ENVKEY_MONGODB_URL = 'TEST_MONGODB_URL'

mongoClient = None
mongoDatabase = None

def setup():
    """Setup the test package
    """
    global mongoClient
    global mongoDatabase
    # Setup
    mongoClient = MongoClient(host = os.environ[ENVKEY_MONGODB_URL])
    mongoDatabase = mongoClient['test_%s' % uuid4()]
    # Set builtin
    __builtin__.mongodb = mongoDatabase

def teardown():
    """Tear down the test package
    """
    global mongoClient
    global mongoDatabase
    # Teardown
    mongoClient.drop_database(mongoDatabase)
    mongoClient.close()

