#!/usr/bin/env python3

import sys
import argparse
import os
import os.path
import itertools as it

from os.path import join as pjoin
from ast import literal_eval

import scipy as sp
import matplotlib.pyplot as plt

def group_by(iterator, key):
    keyf = lambda d: d[key]
    return it.groupby(sorted(iterator, key=keyf), key=keyf)


def main():
    """Plot scatter of slr vs number of nodes in dag

    Run as `find -name slr_plot_data | xargs ./plot-slrs.py`.

    """

    parser = argparse.ArgumentParser(description=main.__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('data_files', nargs="*")
    parser.add_argument('--save-as')
    args = parser.parse_args()

    # Parse data
    data = []
    for fname in args.data_files:
        data_dict = {}

        with open(fname, 'r') as f:
            for line in f.readlines():
                (key, value) = line.strip().split()
                data_dict[literal_eval(key)] = literal_eval(value)

        data.append(data_dict)


    data = sorted(data,
                  key=lambda d: (d['application'], d['powerDipFraction'], d['algorithmName'], d['size']))


    # Plot data. Each application gets its own figure, each power cap
    # function gets its own marker type.
    for application, application_dataset in group_by(data, 'application'):

        markers = it.cycle(['x', 'o', '.'])
        colors = iter(['r', 'g', 'b', 'c', 'k', 'y'])

        fig, axes = plt.subplots(1,1)
        # fig.suptitle(application)
        axes.set_xlabel("Number of nodes in DAG")
        axes.set_ylabel("Makespan ratio")


        for power_dip_fraction, dataset_1 in group_by(application_dataset, 'powerDipFraction'):

            output_data = {}

            for algorithm, dataset in group_by(dataset_1, 'algorithmName'):

                sizes = []
                means = []
                for size, a in group_by(dataset, 'size'):
                    means.append(sp.mean([d['makespan'] for d in a]))
                    sizes.append(int(size))

                output_data[algorithm] = (means, sizes)

            main_algo = "HEFT-like"
            relative_algo = "FCFS-like"

            if output_data[relative_algo][1] != output_data[main_algo][1]:
                print("Some sizes missing for one of the algorithms, got",
                      output_data[relative_algo][1], output_data[main_algo][1])
                continue

            makespan_ratios = sp.array(output_data[main_algo][0]) /  sp.array(output_data[relative_algo][0])
            sizes = output_data[main_algo][1]


            axes.scatter(sizes, makespan_ratios, marker=next(markers),
                         color=next(colors)) # label=str(power_dip_fraction) if npower_functions != 1 else None)


        axes.legend(loc=0)
        axes.set_ylim([0.5, 1.0])
        axes.set_xlim([0, 1100])

        if args.save_as is not None:
            fig.savefig(args.save_as + "-" + application + ".pdf",
                        bbox_inches='tight',
                        pad_inches=0.0,
                        transparent=True)

    plt.show()

    return 0


if __name__ == "__main__":
    sys.exit(main())
