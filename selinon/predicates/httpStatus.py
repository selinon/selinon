#!/usr/bin/env python3
# pragma: no cover

from http.client import HTTPConnection


def httpStatus(host, path, status):
    try:
        conn = HTTPConnection(host)
        conn.request("HEAD", path)
        return conn.getresponse().status == status
    except:
        return False
