#!/bin/bash

# Based on the instructions
# at https://github.com/malawski/cloudworkflowsimulator/wiki/Running-simulations-with-CWS

# Ensure workflows dir does not exist before running this script

set -o errexit
set -o nounset

# mkdir workflows
cd workflows

# Get the workflows (~300MB)
# wget https://download.pegasus.isi.edu/misc/SyntheticWorkflows.tar.gz
# tar -xzf SyntheticWorkflows.tar.gz


# Check that we have GNU parallel and not the other one from the
# moreutils package.
parallel --version 2&>1 > /dev/null \
    || echo "you have moreutils parallel, install GNU parallel instead"

echo "Converting .dax files to .dag using all processor cores, this may take a few minutes"

# Convert to .dag files (using all processor cores)
find -name '*.dax' -print0 \
    | parallel -0 -n 1 --no-notice ruby ../cloudworkflowsimulator/scripts/converters/dax2dag.rb

echo "Done, you can now delete the .dax files and the .tar.gz if you need to save space."
