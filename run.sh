#!/bin/bash

set -o errexit
set -o nounset

out_dir="$(pwd)/out"
out_base="${out_dir}/simulation_out.csv"

# compile
ant clean compile

# clean
touch "${out_dir}/temp"
rm ${out_dir}/*

# run
java -cp 'lib/*:bin/' driver.MySimulation \
    --input-dir '../example_workflows/dags' \
    --output-file "$out_base" \
    --application GENOME \
    --distribution pareto_unsorted \
    --algorithm SPSS

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

# show plots?
if [ $# -gt 0 ]; then 
    eog ${out_dir}/*.png
fi
