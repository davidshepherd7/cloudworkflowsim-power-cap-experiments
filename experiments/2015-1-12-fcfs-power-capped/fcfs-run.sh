#!/bin/bash

set -o errexit
set -o nounset

main="$(readlink -f $(pwd))"
out_dir_root="${main}/output"
project_root="../.."

# Create dirs
mkdir -p $out_dir_root bin

# clean
touch "${out_dir_root}/temp" bin/temp
rm -r ${out_dir_root}/*
rm -r bin/*

# (re)compile cws
cd ../../cloudworkflowsimulator/
ant clean dist
cd "$main"

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

    script_dir="$(readlink -f ${project_root}/cloudworkflowsimulator/scripts)"
    export PYTHONPATH="$PYTHONPATH:$script_dir"


    for size in $sizes; do
        for application in $applications; do

            cd $main

            dagfile_base="${application}.n.${size}.${variation}"
            echo "Running dag $dagfile_base"

            out_dir_base="${out_dir_root}/${dagfile_base}"

            mkdir -p ${out_dir_base}
            java -cp "${project_root}/lib/*:./bin" FCFSPowerCapped \
                 --dagFileName "${main}/input/dags/${dagfile_base}.dag" \
                 --outputDirBase "$out_dir_base" \
                 --vmFile "input/default.vm.yaml" \
                 --application "$application" \
                 --size "$size"

            for out_dir in $(ls -d ${out_dir_base}/*); do

                outfile="${out_dir}/out.log"
                powerfile="${out_dir}/power.log"

                # parse
                python -m log_parser.parse_experiment_log "${outfile}" "${outfile}.parsed"

                # validate
                python -m validation.experiment_validator "${outfile}.parsed" 2>&1 \
                    | tee "${outfile}.validation"

                # plot gantt charts
                ruby -C "$script_dir/visualisation/" plot_gantt.rb \
                     results ${outfile}.parsed ${outfile}.results --crop-from 0.0

                # plot power usage
                ./plot-power.py ${powerfile} ${powerfile}.png

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
    applications="GENOME LIGO SIPHT MONTAGE CYBERSHAKE"
    variations="0 1 2 3 4 5 6 7 8 9"
    sizes="50 100 200 300 400 500 600 700 800 900 1000"

    # Check that we have GNU parallel and not the other one from the
    # moreutils package.
    parallel --version 2&>1 > /dev/null \
        || echo "you have moreutils parallel, install GNU parallel instead"

    # run in parallel
    SHELL="bash" parallel -n 1 --no-notice "worker {} \"$sizes\" \"$applications\"" ::: $variations
fi
