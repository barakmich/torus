#!/bin/sh

GOGOPROTO_ROOT="${GOPATH}/src"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"
#protoc --gogofaster_out=plugins=grpc:. -I=.:"${GOGOPROTO_PATH}" *.proto
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=plugins=grpc:. *.proto
