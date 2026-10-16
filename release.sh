#!/usr/bin/env bash

set -euo pipefail

remote=${1:?Usage: ./release.sh <remote>}
version=$(git show HEAD:VERSION)
if ! [[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "VERSION must be MAJOR.MINOR.PATCH: $version" >&2
  exit 1
fi
if ! git diff --quiet HEAD -- :/VERSION; then
  echo 'Commit VERSION before releasing.' >&2
  exit 1
fi

git tag -a "v$version" -m "Release v$version"
git push "$remote" "v$version"
