#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Selinon adapter for Amazon S3 storage."""

import json
import os

try:
    import boto3
    import botocore
except ImportError as exc:
    raise ImportError("Please install boto3 using `pip3 install selinon[s3]` in order to use S3 storage") from exc
from selinon import DataStorage, SelinonMissingDataException


class S3(DataStorage):
    """Amazon S3 storage adapter.

    For credentials configuration see boto3 library configuration
    https://github.com/boto/boto3
    """

    def __init__(self, bucket, location=None, endpoint_url=None, use_ssl=None,
                 aws_access_key_id=None, aws_secret_access_key=None, region_name=None, serialize_json=False):
        """Initialize S3 storage adapter from YAML configuration file.

        :param bucket: bucket name to be used
        :param location: AWS location
        :param endpoint_url: S3 endpoint (if local instance)
        :param use_ssl: True if SSL should be used
        :param aws_access_key_id: AWS access key
        :param aws_secret_access_key: AWS secret access key
        :param region_name: region to be used
        :param serialize_json: serialize JSON output (dict or list) to a blob - needed as S3 objects are blobs
        """
        # AWS access key and access id are handled by Boto - place them to config or use env variables
        super().__init__()
        self._bucket_name = bucket.format(**os.environ)
        self._location = location.format(**os.environ) if location else None
        self._s3 = None
        self._use_ssl = bool(use_ssl.format(**os.environ) if isinstance(use_ssl, str) else use_ssl)
        self._endpoint_url = endpoint_url.format(**os.environ) if endpoint_url else None
        self._serialize_json = serialize_json
        aws_access_key_id = aws_access_key_id.format(**os.environ) if aws_access_key_id else None
        aws_secret_access_key = aws_secret_access_key.format(**os.environ) if aws_secret_access_key else None
        region_name = region_name.format(**os.environ) if region_name else None
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

        blob = self._s3.Object(self._bucket_name, task_id).get()['Body'].read()
        if not self._serialize_json:
            return blob

        return json.loads(blob.decode())

    def store(self, node_args, flow_name, task_name, task_id, result):  # noqa
        assert self.is_connected()  # nosec

        if self._serialize_json:
            result = json.dumps(result).encode()

        self._s3.Object(self._bucket_name, task_id).put(Body=result)

    def store_error(self, node_args, flow_name, task_name, task_id, exc_info):  # noqa
        # just to make pylint happy
        raise NotImplementedError()

    def delete(self, flow_name, task_name, task_id):
        assert self.is_connected()  # nosec

        s3_object = self._s3.Object(self._bucket_name, task_id)

        try:
            s3_object.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                raise SelinonMissingDataException from e
            raise e
        s3_object.delete()
