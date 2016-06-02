# encoding=utf8

""" Test the condition
    Author: lipixun
    Created Time : 二  4/12 19:42:53 2016

    File Name: test_condition.py
    Description:

"""

from datahub.conditions import Condition, AndCondition, KeyValueCondition

def test_condition_basic():
    """Test the condition basic
    """
    js = {
        'and': {
            'conditions': [
                {
                    'kv': {
                        'key': 'akey',
                        'value': 'avalue',
                        'equals': True
                    }
                }
            ]
        }
    }
    condition = Condition.load(js)
    assert isinstance(condition, AndCondition)
    assert len(condition.conditions) == 1
    assert isinstance(condition.conditions[0], KeyValueCondition)
    assert condition.conditions[0].key == 'akey' and condition.conditions[0].value == 'avalue'
    dumpJson = condition.dump()
    assert dumpJson == js
