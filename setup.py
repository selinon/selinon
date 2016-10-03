#!/usr/bin/python3

from setuptools import setup


def get_requirements():
    with open('requirements.txt') as fd:
        return fd.read().splitlines()


setup(
    name='selinon',
    version='0.1.0rc3',
    packages=['selinon', 'selinon.storage'],
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
