#!/bin/bash

set -o errexit
set -o nounset

out_dir="$(pwd)/out"
out_base="${out_dir}/simulation_out.csv"
root="../.."

# Create dirs
mkdir -p $out_dir bin

# clean
touch "${out_dir}/temp" bin/temp
rm ${out_dir}/*
rm -r bin/*

# compile
javac -cp "${root}/lib/*" -d bin/ src/*.java

# run
java -cp "${root}/lib/*:./bin" MySimulation \
     --inputDir 'input/dags' \
     --outputFile "$out_base" \
     --vmFile "input/default.vm.yaml" \
     --application MONTAGE

# GENOME LIGO SIPHT MONTAGE CYBERSHAKE


# parse
cd ~/workflows/cloudworkflowsimulator/scripts
find "$out_dir" -name '*.log' | parallel -n1 python -m log_parser.parse_experiment_log "{}" "{}.parsed"

# validate
cd ~/workflows/cloudworkflowsimulator/scripts
find "$out_dir" -name '*.log.parsed' \
    | parallel -n1 python -m validation.experiment_validator "{}" 2>&1 \
    | tee "${out_dir}/validation_out"

# plot
cd ~/workflows/cloudworkflowsimulator/scripts/visualisation
find "$out_dir" -name '*.log.parsed' | parallel -n1 ruby plot_gantt.rb results {} {}.results
find "$out_dir" -name '*.log.parsed' | parallel -n1 ruby plot_gantt.rb workflow {} {}.workflow

# show output?
if [ $# -gt 0 ]; then
    cat ${out_dir}/simulation_out.csv.*.log
fi

# show plots?
if [ $# -gt 1 ]; then
    eog ${out_dir}/*.png
fi
