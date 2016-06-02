# encoding=utf8

""" The spec definitions
    Author: lipixun
    Created Time : 四  5/26 23:42:26 2016

    File Name: spec.py
    Description:

"""

# -*- ---------- The feature specs ---------- -*-

# The store feature
FEATURE_STORE_EXIST                                 = 'store.exist'             # Check if exist by id
FEATURE_STORE_GET                                   = 'store.get'               # Get value by id / ids
FEATURE_STORE_CREATE                                = 'store.create'            # Create new value
FEATURE_STORE_REPLACE                               = 'store.replace'           # Replace value by model
FEATURE_STORE_UPDATE                                = 'store.update'            # Update value by id
FEATURE_STORE_DELETE                                = 'store.delete'            # Delete value by id
FEATURE_STORE_COUNT                                 = 'store.count'             # Count the value

# The query feature
FEATURE_QUERY_EXIST                                 = 'query.exist'             # Check exists by query
FEATURE_QUERY_GET                                   = 'query.get'               # Get values by query
FEATURE_QUERY_UPDATE                                = 'query.update'            # Update values by query
FEATURE_QUERY_DELETE                                = 'query.delete'            # Delete values by query
FEATURE_QUERY_COUNT                                 = 'query.count'             # Count values by query

# The high level feature
FEATURE_WATCH                                       = 'watch'                   # The watch feature

# -*- ---------- The data manager event specs ---------- -*-

EVENT_CREATED                                       = 'created'                 # Created
EVENT_REPLACED                                      = 'replaced'                # Replaced
EVENT_UPDATED                                       = 'updated'                 # Updated
EVENT_DELETED                                       = 'deleted'                 # Deleted

# -*- ---------- The error definition ---------- -*-

ERROR_DUPLICATED_KEY                                = 0x40000001                # Duplicated key found
