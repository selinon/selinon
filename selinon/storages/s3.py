#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon adapter for Amazon S3 storage."""

try:
    import boto3
    import botocore
except ImportError as exc:
    raise ImportError("Please install boto3 using `pip3 install selinon[s3]` in order to use S3 storage") from exc
from selinon import DataStorage


class S3(DataStorage):
    """Amazon S3 storage adapter.

    For credentials configuration see boto3 library configuration
    https://github.com/boto/boto3
    """

    def __init__(self, bucket, location, endpoint_url=None, use_ssl=None,
                 aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        """Initialize S3 storage adapter from YAML configuration file.

        :param bucket: bucket name to be used
        :param location: AWS location
        :param endpoint_url: S3 endpoint (if local instance)
        :param use_ssl: True if SSL should be used
        :param aws_access_key_id: AWS access key
        :param aws_secret_access_key: AWS secret access key
        :param region_name: region to be used
        """
        # AWS access key and access id are handled by Boto - place them to config or use env variables
        super().__init__()
        self._bucket_name = bucket
        self._location = location
        self._s3 = None
        self._use_ssl = use_ssl
        self._endpoint_url = endpoint_url
        self._session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                              aws_secret_access_key=aws_secret_access_key,
                                              region_name=region_name)

    def is_connected(self):  # noqa
        return self._s3 is not None

    def connect(self):  # noqa
        # we need signature version v4 as new AWS regions use this version and we won't be able to connect without this
        self._s3 = self._session.resource('s3', config=botocore.client.Config(signature_version='s3v4'),
                                          use_ssl=self._use_ssl, endpoint_url=self._endpoint_url)

        # check that the bucket exists - see boto docs
        try:
            self._s3.meta.client.head_bucket(Bucket=self._bucket_name)
        except botocore.exceptions.ClientError as exc:
            # if a client error is thrown, then check that it was a 404 error.
            # if it was a 404 error, then the bucket does not exist.
            error_code = int(exc.response['Error']['Code'])
            if error_code == 404:
                self._s3.create_bucket(Bucket=self._bucket_name,
                                       CreateBucketConfiguration={
                                           'LocationConstraint': self._location
                                       })
            else:
                raise

    def disconnect(self):  # noqa
        if self._s3:
            del self._s3
            self._s3 = None

    def retrieve(self, flow_name, task_name, task_id):  # noqa
        assert self.is_connected()  # nosec
        return self._s3.Object(self._bucket_name, task_id).get()['Body'].read()

    def store(self, node_args, flow_name, task_name, task_id, result):  # noqa
        assert self.is_connected()  # nosec
        self._s3.Object(self._bucket_name, task_id).put(Body=result)

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # noqa
        # just to make pylint happy
        raise NotImplementedError()
