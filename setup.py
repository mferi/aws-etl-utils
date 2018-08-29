# -*- coding: utf-8 -*-

from os.path import join, dirname
from setuptools import find_packages

try:
    from setuptools import setup

except ImportError:
    from distutils.core import setup


with open(join(dirname(__file__), 'README.md')) as fr:
    long_description = fr.read()

config = {
        'name': 'aws-etl-utils',
        'description': 'AWS ETL Utility',
        'version': '0.0.3',
        'license': 'Apache 2.0',
        'url': 'https://github.com/mferi/aws-etl-utils',
        'author': 'Maria Feria',
        'author_email': 'mferiaa@gmail.com',
        'install_requires': [
            'boto3',
            'psycopg2-binary',
        ],
        'packages': find_packages(exclude=['tests']),
        'long_description': long_description,
        'long_description_content_type': 'text/markdown',
        'classifiers': (
            'Programming Language :: Python :: 3',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
        )
    }

setup(**config)
