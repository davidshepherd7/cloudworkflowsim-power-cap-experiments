#!/bin/bash

set -o errexit
set -o nounset

if [ $# -gt 0 ]; then
    applications="GENOME MONTAGE"
    variations="0"
    sizes="50 900"
else
    applications="GENOME LIGO SIPHT MONTAGE CYBERSHAKE"
    variations="0 1 2 3 4 5 6 7 8 9"
    sizes="50 100 200 300 400 500 600 700 800 900 1000"
fi

main="$(readlink -f $(pwd))"
out_dir_root="${main}/output"
project_root="../.."

# Create dirs
mkdir -p $out_dir_root bin

# clean
touch "${out_dir_root}/temp" bin/temp
rm -r ${out_dir_root}/*
rm -r bin/*

# compile
javac -cp "${project_root}/lib/*" -d bin/ src/*.java

worker()
{
    set -o errexit
    set -o nounset

    variation="$1"
    sizes="$2"
    applications="$3"

    main="$(readlink -f $(pwd))"
    out_dir_root="${main}/output"
    project_root="../.."

    for size in $sizes; do
        for application in $applications; do

            cd $main

            dagfile_base="${application}.n.${size}.${variation}"
            echo "Running dag $dagfile_base"

            out_dir_base="${out_dir_root}/${dagfile_base}"

            mkdir -p ${out_dir_base}
            java -cp "${project_root}/lib/*:./bin" MySimulation \
                 --dagFileName "${main}/input/dags/${dagfile_base}.dag" \
                 --outputDirBase "$out_dir_base" \
                 --vmFile "input/default.vm.yaml"

            for out_dir in $(ls -d ${out_dir_base}/*); do

                outfile="${out_dir}/out.log"
                powerfile="${out_dir}/power.log"

                # parse
                cd ~/workflows/cloudworkflowsimulator/scripts
                python -m log_parser.parse_experiment_log "${outfile}" "${outfile}.parsed"

                # validate
                cd ~/workflows/cloudworkflowsimulator/scripts
                python -m validation.experiment_validator "${outfile}.parsed" 2>&1 \
                    | tee "${outfile}.validation"

                # plot gantt charts
                cd ~/workflows/cloudworkflowsimulator/scripts/visualisation
                ruby plot_gantt.rb results ${outfile}.parsed ${outfile}.results
                ruby plot_gantt.rb workflow ${outfile}.parsed ${outfile}.workflow

                # plot power usage
                cd $main
                ./plot.py ${powerfile} ${powerfile}.png

            done
        done
    done
}

export -f worker


if [ $# -gt 0 ]; then
    applications="GENOME LIGO SIPHT MONTAGE CYBERSHAKE"
    variations="0"
    sizes="50 900"

    worker 0 "$sizes" "$applications"
else
    # run in parallel
    SHELL="bash" parallel -n 1 -j 8 "worker {} \"$sizes\" \"$applications\"" ::: $variations
fi
