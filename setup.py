#!/usr/bin/python3

import sys
import os
from setuptools import setup, find_packages


if sys.version_info[0] != 3:
    sys.exit("Python3 is required in order to install Selinon")


def get_requirements():
    with open('requirements.txt') as fd:
        return fd.read().splitlines()


def get_version():
    with open(os.path.join('selinon', 'version.py')) as f:
        version = f.readline()
    # dirty, remove trailing and leading chars
    return version.split(' = ')[1][1:-2]


def get_long_description():
    with open('README.rst', 'r') as f:
        return f.read()


setup(
    name='selinon',
    version=get_version(),
    entry_points={
        'console_scripts': ['selinon-cli=selinon.cli:cli']
    },
    packages=find_packages(),
    install_requires=get_requirements(),
    author='Fridolin Pokorny',
    author_email='fridolin.pokorny@gmail.com',
    maintainer='Fridolin Pokorny',
    maintainer_email='fridolin.pokorny@gmail.com',
    description='an advanced dynamic task flow management on top of Celery',
    long_description=get_long_description(),
    url='https://github.com/selinon/selinon',
    license='BSD',
    keywords='selinon celery yaml flow distributed-computing',
    extras_require={
        'celery': ['celery>=4'],
        'mongodb': ['pymongo'],
        'postgresql': ['SQLAlchemy', 'SQLAlchemy-Utils'],
        'redis': ['redis'],
        's3': ['boto3'],
        'sentry': ['sentry-sdk']
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy"
    ]
)
