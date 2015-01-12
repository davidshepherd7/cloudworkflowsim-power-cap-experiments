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


    # Plot data. Each application gets its own figure, each power cap
    # function gets its own marker type.
    for application, application_dataset in group_by(data, 'application'):

        markers = iter(['x', 'o', '.'])
        colors = iter(['r', 'g', 'b'])

        fig, axes = plt.subplots(1,1)
        fig.suptitle(application)
        axes.set_xlabel("Number of nodes in DAG")
        axes.set_ylabel("Normalised makespan")


        for power_dip_fraction, dataset in group_by(application_dataset, 'powerDipFraction'):

            sizes = []
            means = []
            for size, a in group_by(dataset, 'size'):
                means.append(sp.mean([d['makespan']/ d['optimalMakespan'] for d in a]))
                sizes.append(size)

            axes.scatter(sizes, means,
                         marker=next(markers), color=next(colors),
                         label=power_dip_fraction)

        axes.legend(loc=0)
        axes.set_ylim([1.0, 2.0])

        if args.save_as is not None:
            fig.savefig(args.save_as + "-" + application + ".pdf")

    plt.show()

    return 0


if __name__ == "__main__":
    sys.exit(main())
