#!/bin/sh

echo "Generating new version identifier..."

VERSION=`git describe 2>/dev/null || git rev-parse --short HEAD`
echo -e "celeriac_version = '${VERSION}'\n" > celeriac/version.py

echo "Celeriac version is '${VERSION}'"
