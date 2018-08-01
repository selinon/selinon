#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon library helpers."""

from contextlib import contextmanager
import json
import logging
import os
import subprocess
import tempfile

from .errors import RequestError

_logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def dict2strkwargs(dict_):
    """Convert a dictionary into arguments to a string representation that can be used as arguments to a function."""
    ret = ""
    for key, value in dict_.items():
        if ret:
            ret += ", "
        ret += "%s=%s" % (key, expr2str(value))
    return ret


def expr2str(expr):
    """Convert a Python expression into a Python code."""
    if isinstance(expr, dict):
        return str(expr)

    if isinstance(expr, list):
        # s/'['foo']['bar']'/['foo']['bar']/ (get rid of leading ')
        return "%s" % expr

    if isinstance(expr, str):
        return "'%s'" % expr

    # some built-in type such as bool/int/...
    return "%s" % str(expr)


def keylist2str(keylist):
    """Convert keylist to a string representation.

    :param keylist: keylist to be converted
    :type keylist: list
    :return: string representation
    :rtype: str
    """
    return "".join(map(lambda x: "['" + str(x) + "']", keylist))


@contextmanager
def pushd(new_dir):
    """Traverse a directory tree in a pushd/popd manner.

    :param new_dir: new directory to cd to
    :type new_dir: str
    """
    prev_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(prev_dir)


def dict2json(dict_, pretty=True, safe=True):
    """Convert dict to json (string).

    :param dict_: dictionary to be converted
    :type dict_: dict
    :param pretty: if True, nice formatting will be used
    :type pretty: bool
    :param safe: perform safe dump
    :type safe: bool
    :return: formatted dict in json
    :rtype: str
    """
    def default(obj):
        """Serialize some additional types that the default encoder does not support."""
        if isinstance(obj, set):
            return list(obj)
        return str(obj)

    kwargs = {}
    if pretty:
        kwargs['sort_keys'] = True
        kwargs['separators'] = (',', ': ')
        kwargs['indent'] = 2

    if safe:
        kwargs['default'] = default

    return json.dumps(dict_, **kwargs)


def get_function_arguments(func):
    """Get arguments of function.

    :param func: function to parse arguments
    :return: list of arguments that predicate function expects
    """
    return list(func.__code__.co_varnames[:func.__code__.co_argcount])


def check_conf_keys(dict_, known_conf_opts):
    """Check supplied configuration options against known configuration options.

    :param dict_: dict with configuration options
    :param known_conf_opts: known configuration options
    :return: configuration options that are now known with their values
    """
    return {k: v for k, v in dict_.items() if k not in known_conf_opts}


def git_previous_version(file_path):
    """Get hash of previous version of a file in Git CVS.

    :param file_path: a path to file to get previous version from
    :return: git hash of previous version and it's depth from master
    :rtype: tuple
    """
    cmd = "git log --max-count 1 --pretty=format:%H".split(' ')
    cmd.append(file_path)
    try:
        git_hash = subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True)
    except subprocess.CalledProcessError as exc:
        err_msg = "Failed to get previous version of file %r using git: %s" % (file_path, str(exc.output))
        raise RequestError(err_msg) from exc

    cmd = "git rev-list --ancestry-path {}..HEAD".format(git_hash).split(' ')
    try:
        depth = len(subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True).split('\n'))
    except subprocess.CalledProcessError as exc:
        err_msg = "Failed to get git log depth for file %r using git: %s" % (file_path, str(exc.output))
        raise RequestError(err_msg) from exc

    _logger.debug("Previous version found in Git CVS of file %r is %s with a depth of %d",
                  file_path, git_hash, depth)
    return git_hash, depth


def git_previous_version_file(git_hash, file_path, tmp_dir=None):
    """Get previous version of a file in Git CVS.

    :param git_hash: git hash for the given file
    :param file_path: a path to file to get previous version from
    :param tmp_dir: a directory to store the content of previous file version
    :return: a path to a temporary file with content from previous version
    """
    cmd = ["git", "show", "%s:%s" % (git_hash, file_path if file_path.startswith('/') else './' + file_path)]
    try:
        file_content = subprocess.check_output(cmd, stderr=subprocess.PIPE, universal_newlines=True)
    except subprocess.CalledProcessError as exc:
        err_msg = "Failed to get content of file %r in version %s using git: %s" \
                  % (file_path, git_hash, str(exc.output))
        raise RequestError(err_msg) from exc

    _logger.debug("Using git version %r for file %r", git_hash, file_path)
    with tempfile.NamedTemporaryFile(mode="w", dir=tmp_dir, delete=False) as temp_file:
        temp_file.write(file_content)
        return temp_file.name
