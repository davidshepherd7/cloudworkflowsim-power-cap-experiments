
# Experiments with power-capped HEFT


## Installation

1. Recursively clone the repository:

       git clone --recursive git@github.com:davidshepherd7/cloudworkflowsim-power-cap-experiments.git

   note the `--recursive`, without it you won't get cloudworkflowsimulator itself!

2. Install required software: On Debian-based systems run
   `./install_prereqs.sh` (requires sudo, you should probabaly read it
   first). On other systems read the script and install the same packages
   using your package manager.

3. Get the .dag files from the Pegasus website: run
   `grab_synthetic_workflows.sh` (downloads a large tar file, may take some
   time).


## Running the experiments

Run experiments with power-capped HEFT:

    cd experiments/2014-12-08-first
    ./first-run.sh

Run experiments with power-capped FCFS (for comparison):

    cd experiments/2015-1-12-fcfs-power-capped
    ./fcfs-run.sh


In both cases output goes into the `output` subdir.


## Plotting

Individual plots of the schedule and the power usage over time for each run
are contained in the output subdirs as png files.

To plot the schedule length ratios use `experiments/2014-12-08-first/plot-slrs.py` with a list of `slr_plot_data` files as input. For example to plot data from both experiments with min power cap at 0.5*max: cd to the `experiments` dir and run

    find -name 'slr_plot_data' -path '*/0.5/*' | xargs ./2014-12-08-first/plot-slrs.py

Note that you may not be able to plot all experiments at once due to limitations on the number of command line arguments.
