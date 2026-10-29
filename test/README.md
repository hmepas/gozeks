# Integration tests

The tests in this directory drive several in-process gozeks daemons against a real
S3-compatible endpoint and need the `integration` build tag.

Start MinIO, either with Docker:

    docker-compose -f test/docker-compose.yml up -d

or with a local binary (`brew install minio`):

    ./test/minio.sh

Then:

    make integration

Endpoint and credentials can be overridden with `GOZEKS_TEST_S3_ENDPOINT`,
`GOZEKS_TEST_S3_ACCESS_KEY`, `GOZEKS_TEST_S3_SECRET_KEY` (defaults match the compose file).
Each test creates and drops its own bucket.
