#!/bin/sh
# Run in a disposable golang:1.27-alpine container with the repo at /repo.
set -eu
apk add --no-cache protobuf protobuf-dev
GOBIN=/usr/local/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
cd /repo
protoc -I 'Poller Service/BMKG Poller Service/hazard' -I /usr/include \
  --go_out='Poller Service/BMKG Poller Service/hazard' \
  --go_opt=paths=source_relative --go_opt=Mhazard.proto=bmkg-poller/hazard hazard.proto
protoc -I 'Poller Service/PVMBG Poller Service' -I /usr/include \
  --go_out='Poller Service/PVMBG Poller Service' \
  --go_opt=paths=source_relative proto/hazard.proto
protoc -I 'Aggregate Service/proto' -I 'Auth Service/proto' -I /usr/include \
  --include_imports --descriptor_set_out='API Gateway/descriptors/api.pb' \
  hazard/hazard.proto auth/v1/auth.proto
