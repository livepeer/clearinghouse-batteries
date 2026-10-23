#!/bin/sh
set -eu

image=$1
version=$2
commit=$3

test "$(docker run --rm --network none --read-only "$image" --version)" = "clearinghouse version $version ($commit)"
docker run --rm --network none --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges --user 65534:65534 \
  --tmpfs /tmp:rw,mode=1777 \
  -v "$PWD/.github/scripts/runtime-smoke.sh:/smoke.sh:ro" \
  --entrypoint /bin/sh "$image" /smoke.sh "$version" "$commit"
