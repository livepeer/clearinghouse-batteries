#!/bin/sh
set -eu

archive=$1
version=$2
commit=$3
bundle=$(basename "$archive" .tar.gz)

cd "$(dirname "$archive")"
sha256sum -c "$bundle.tar.gz.sha256"
mkdir -p /tmp/unpack /tmp/install/bin
tar -xzf "$archive" -C /tmp/unpack
cd "/tmp/unpack/$bundle"
sha256sum -c SHA256SUMS
grep -Fx "version=$version" VERSION
grep -Fx "commit=$commit" VERSION
cp bin/clearinghouse /tmp/install/bin/
cd /tmp
rm -rf /tmp/unpack
export PATH=/tmp/install/bin:/usr/bin:/bin
unset GOROOT GOPATH GOTOOLCHAIN GOENV GOWORK GOFLAGS
sh /runtime-smoke.sh "$version" "$commit"
