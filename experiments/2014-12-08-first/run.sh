#!/bin/bash

set -o errexit
set -o nounset

applications="GENOME LIGO SIPHT MONTAGE CYBERSHAKE"
powerCapTimes="0.0 100 400"
powerCapValues="200.001 100.001 400.001"


main="$(pwd)"
out_dir="$(pwd)/output"
out_base="${out_dir}/simulation_out.csv"
root="../.."

# Create dirs
mkdir -p $out_dir bin

# clean
touch "${out_dir}/temp" bin/temp
rm -r ${out_dir}/*
rm -r bin/*

# compile
javac -cp "${root}/lib/*" -d bin/ src/*.java

# run
for application in $applications
do
    cd $main

    dir="${out_dir}/${application}"
    mkdir -p ${dir}
    java -cp "${root}/lib/*:./bin" MySimulation \
         --inputDir 'input/dags' \
         --outputFile "${dir}/out.csv" \
         --vmFile "input/default.vm.yaml" \
         --powerCapTimes $powerCapTimes \
         --powerCapValues $powerCapValues \
         --application $application


    # parse
    cd ~/workflows/cloudworkflowsimulator/scripts
    find "$dir" -name '*.log' | parallel -n1 python -m log_parser.parse_experiment_log "{}" "{}.parsed"

    # validate
    cd ~/workflows/cloudworkflowsimulator/scripts
    find "$dir" -name '*.log.parsed' \
        | parallel -n1 python -m validation.experiment_validator "{}" 2>&1 \
        | tee "${dir}/validation_out"

    # plot
    cd ~/workflows/cloudworkflowsimulator/scripts/visualisation
    find "$dir" -name '*.log.parsed' | parallel -n1 ruby plot_gantt.rb results {} {}.results
    find "$dir" -name '*.log.parsed' | parallel -n1 ruby plot_gantt.rb workflow {} {}.workflow

done
