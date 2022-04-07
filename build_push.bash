#!/usr/bin/env bash

VERSION_RULE=$1

[ -z "$VERSION_RULE" ] && echo "Version rule is empty" && exit 1

poetry update
poetry version $VERSION_RULE
poetry publish --build

git tag "v$(poetry version -s)"
git push
git push --tags
