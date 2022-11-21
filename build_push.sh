#!/usr/bin/env bash

#VERSION_RULE=$1

#[ -z "$VERSION_RULE" ] && echo "Version rule is empty" && exit 1

poetry update
#poetry version $VERSION_RULE  # TODO: this should be committed before pushing
#poetry publish --build
poetry build

git tag "v$(poetry version -s)"
git push
git push --tags
