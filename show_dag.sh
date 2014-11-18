#!/bin/bash

set -o errexit
set -o nounset

# Safely generate a temp file
tmpfile=$(mktemp dagview-$USER.XXXXXXX --tmpdir)

# Make the image
/usr/share/pegasus/visualize/dax2dot $1 | dot -Tps > $tmpfile

# Show it
evince $tmpfile &
