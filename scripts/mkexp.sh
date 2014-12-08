#!/bin/bash

set -o errexit
set -o nounset

name="$1"
dirname="experiments/$(date +%Y-%m-%d)-$name"

mkdir "$dirname"
