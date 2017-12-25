#!/bin/sh

set -ex

selinon-cli migrate -N ver_1/nodes.yml -F ver_1/flows/ -n ver_2/nodes.yml -f ver_2/flows/ --migration-dir .
selinon-cli migrate -N ver_2/nodes.yml -F ver_2/flows/ -n ver_3/nodes.yml -f ver_3/flows/ --migration-dir .
