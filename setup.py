#!/usr/bin/python3

import sys
import os
from setuptools import setup


NAME = 'selinon'


def get_requirements():
    with open('requirements.txt') as fd:
        return fd.read().splitlines()


def get_version():
    with open(os.path.join(NAME, 'version.py')) as f:
        version = f.readline()
    # dirty, remove trailing and leading chars
    return version.split(' = ')[1][1:-2]


if sys.version_info[0] != 3:
    sys.exit("Python3 is required in order to install selinon")

setup(
    name=NAME,
    version=get_version(),
    packages=[NAME],
    install_requires=get_requirements(),
    author='Fridolin Pokorny',
    author_email='fpokorny@redhat.com',
    maintainer='Fridolin Pokorny',
    maintainer_email='fpokorny@redhat.com',
    description='task flow management for Celery',
    url='https://github.com/selinon/selinon',
    license='BSD',
    keywords='celery selinonlib yaml condition flow',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy"
    ]
)
