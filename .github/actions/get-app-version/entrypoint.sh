#!/bin/bash

VERSION_FILE=$1

if [[ -f $VERSION_FILE ]]; then
    version_value="$(head -n 1 $VERSION_FILE)"
else
    echo "${VERSION_FILE} file does not exists"
    exit 1
fi

echo "::set-output name=version::$version_value"
