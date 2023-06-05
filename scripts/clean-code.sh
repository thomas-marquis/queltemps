#!/bin/bash

DIR=${1:-.}

set -e

black $DIR
isort $DIR
flake8 $DIR
