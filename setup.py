# encoding=utf8

""" The datahub setup script
    Author: lipixun
    Created Time : 四 12/17 17:28:50 2015

    File Name: setup.py
    Description:

"""

import sys
reload(sys)
sys.setdefaultencoding('utf8')

from setuptools import setup, find_packages

requirements = [ x.strip() for x in open('requirements.txt').readlines() ]

setup(
    name = 'datahub',
    author = 'lipixun',
    author_email = 'lipixun@outlook.com',
    url = 'https://github.com/lipixun/pydatahub',
    packages = find_packages(),
    install_requires = requirements,
    description = 'The datahub framework',
    long_description = open('README.md').read(),
)

