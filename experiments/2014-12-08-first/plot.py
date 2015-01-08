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


def main():
    """Plot list of piecewise constant functions from a file. Format is:

        ( label, initial_value, dict_of_jumps )
    """

    # Parse arguments
    parser = argparse.ArgumentParser(description=main.__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('power_log_file')
    parser.add_argument('save_plot_file', nargs="?", default=None)
    args = parser.parse_args()

    # do it
    fig = plot_power(parse_power_log(args.power_log_file))

    if args.save_plot_file is None:
        plt.show()
    else:
        plt.savefig(args.save_plot_file)

    return 0


class Power(object):
    pass


def parse_power_log(filename):

    # Parse file
    with open(filename, 'r') as power_file:
        parsed = [literal_eval(l) for l in power_file.readlines()]

    # Last time of all jumps
    maxtime = max(it.chain(*[p[2].keys() for p in parsed]))

    # Convert to list of Power struct
    final = []
    for func in parsed:
        label, start, jump_dict = func

        p = Power()
        p.label = label

        # Extract jumps of first power function, store as two lists
        pairs = [(0.0, start)] + \
                sorted(zip(jump_dict.keys(), jump_dict.values()),
                       key=lambda pair: pair[0])

        # Add final time for nicer plotting
        pairs.append((maxtime, pairs[-1][1]))

        # Store
        p.jump_times, p.jump_values = zip(*pairs)
        final.append(p)

    return final


def plot_power(power_list):

    for power in power_list:
        plt.step(power.jump_times, power.jump_values,
                 label=power.label, where="post")

    plt.legend()

    return plt.gcf()


if __name__ == "__main__":
    sys.exit(main())
