#!/bin/sh

set -e
export GOFLAGS="-mod=vendor"

go build -o bin/rest-service-example cmd/rest-service-example/main.go
