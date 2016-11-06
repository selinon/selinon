#!/usr/bin/python3

import sys
from setuptools import setup


def get_requirements():
    with open('requirements.txt') as fd:
        return fd.read().splitlines()

if sys.version_info[0] != 3:
    sys.exit("Python3 is required in order to install selinon")

setup(
    name='selinon',
    version='0.1.0rc3',
    packages=['selinon'],
    install_requires=get_requirements(),
    author='Fridolin Pokorny',
    author_email='fpokorny@redhat.com',
    maintainer='Fridolin Pokorny',
    maintainer_email='fpokorny@redhat.com',
    description='task flow management for Celery',
    url='https://github.com/fridex/selinon',
    license='GPL',
    keywords='celery selinonlib yaml condition flow',
)
