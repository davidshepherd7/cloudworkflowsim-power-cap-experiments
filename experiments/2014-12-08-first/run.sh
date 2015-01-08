#!/bin/bash

set -o errexit
set -o nounset

applications="GENOME LIGO SIPHT MONTAGE CYBERSHAKE"
# applications="GENOME"
powerCapTimes="0.0 10000 40000"
powerCapValues="200.001 100.001 400.001"

variations="0 1 2 3 4 5 6 7 8 9"
# variations="0"
sizes="50 100 200 300 400 500 600 700 800 900 1000"
# sizes="50"


main="$(readlink -f $(pwd))"
out_dir="${main}/output"
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
for size in $sizes; do
    for i in $variations; do
        for application in $applications
        do
            cd $main

            dagfile_base="${application}.n.${size}.${i}"
            echo "Running dag $dagfile_base"

            dir="${out_dir}/${dagfile_base}"
            out="${dir}/out.log"

            mkdir -p ${dir}
            java -cp "${root}/lib/*:./bin" MySimulation \
                 --dagFileName "${main}/input/dags/${dagfile_base}.dag" \
                 --outputFile "$out" \
                 --vmFile "input/default.vm.yaml" \
                 --powerCapTimes $powerCapTimes \
                 --powerCapValues $powerCapValues


            # parse
            cd ~/workflows/cloudworkflowsimulator/scripts
            python -m log_parser.parse_experiment_log "${out}" "${out}.parsed"

            # validate
            cd ~/workflows/cloudworkflowsimulator/scripts
            python -m validation.experiment_validator "${out}.parsed" 2>&1 \
                | tee "${dir}/validation_out"

            # plot gantt charts
            cd ~/workflows/cloudworkflowsimulator/scripts/visualisation
            ruby plot_gantt.rb results ${out}.parsed ${out}.results
            ruby plot_gantt.rb workflow ${out}.parsed ${out}.workflow

            # plot power usage
            cd $main
            ./plot.py ${out}.power-log ${out}.power-log.png

        done
    done
done
