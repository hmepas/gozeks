#!/bin/sh
# Run a throwaway MinIO on 127.0.0.1:9000 with the same credentials as docker-compose.yml.
set -e
dir=$(mktemp -d)
export MINIO_ROOT_USER=gozeks MINIO_ROOT_PASSWORD=gozeks-secret
echo "minio data in $dir"
exec minio server "$dir" --address 127.0.0.1:9000 --quiet
