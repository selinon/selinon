#!/bin/bash

set -ex

for conf in *.yaml; do
	selinon-cli plot --nodes-definition ${conf} --flow-definitions ${conf} --format png --output-dir . || exit 1
done
echo 'Generated flows are present in the current directory'

