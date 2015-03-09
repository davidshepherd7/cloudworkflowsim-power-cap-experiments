#!/bin/bash

set -o errexit
set -o nounset

# Java + ant build tool. Java version >= 7 is needed.
sudo apt-get install -y default-jdk default-jre ant

# The -y flag just shuts up the yes/no prompt

# Python and ruby for plotting (cloudworkflowsim scripts need python2, mine
# use python3. If this is a problem you should be able to use the 2to3
# utility to convert everything to python3.) I'm assuming that 'python'
# alone will install python2.
sudo apt-get install -y python python3 python3-matplotlib ruby gem python3-scipy

# GNU parallel for running parameter sweeps in parallel (note: moreutils
# parallel is no good!)
sudo apt-get install -y parallel

# Ruby gems needed for some plotting scripts
sudo gem install gnuplot main

# Build cloudworkflowsim, test and create a jar file
cd cloudworkflowsimulator
ant clean test dist

echo "Now you should be able to run the run-***.sh scripts in the experiments dirs (hopefully)"
