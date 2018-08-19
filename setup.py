# -*- coding: utf-8 -*-

from os.path import join, dirname
from setuptools import find_packages

try:
    from setuptools import setup

except ImportError:
    from distutils.core import setup


def read(fname):
    return open(join(dirname(__file__), fname)).read()

config = {
        'description': 'AWS ETL Utility',
        'version': '0.0.1',
        'license': 'Apache 2.0',
        'url': 'https://github.com/mferi/aws-etl-utils',
        'author': 'Maria Feria',
        'install_requires': [
            'boto3',
        ],
        'packages': find_packages(exclude=['tests']),
        'name': 'AWS-etl-utils',
        'long_description': read('README.rst'),
    }

setup(**config)
