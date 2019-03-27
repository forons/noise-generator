#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import os
from setuptools import find_packages, setup

# Package meta-data.
NAME = 'noise-generation'
DESCRIPTION = 'A library to introduce noise into a dataset, with multiple distributions.'
URL = 'https://github.com/forons/noise-generator'
EMAIL = 'daniele.foroni@unitn.it'
AUTHOR = 'Daniele Foroni'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.0.1'

REQUIRED = [
    'pyspark>=2.2.0', 'scipy'
]

here = os.path.abspath(os.path.dirname(__file__))

try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['data quality', 'noise generation', 'errors', 'rule', 'distribution', 'spark'],
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    install_requires=REQUIRED,
    include_package_data=True,
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ]
)
