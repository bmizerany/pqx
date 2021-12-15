#!/bin/sh

set -e
set -x

# See: https://github.com/docker-library/postgres/issues/415
localedef -i en_US -f UTF-8 en_US.UTF-8

su - postgres -c /app/ci.sh
