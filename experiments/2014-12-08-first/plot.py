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

    # Parse file
    with open("output/GENOME/out.power-log", 'r') as power_file:
        parsed = [literal_eval(l) for l in power_file.readlines()]

    times = [p[2].keys() for p in parsed]
    endtime = max(it.chain(*times))
    print(endtime)

    for func in parsed:
        # Extract jumps of first power function
        d = func[2]
        pairs = sorted(zip(d.keys(), d.values()), key=lambda p: p[0]) 
        times, values = zip(*pairs)

        # Extend jump lists to include a final value
        times = list(times) + [endtime]
        values = list(values) + [values[-1]]

        # And plot
        plt.step(times, values, label=func[0], where="post")

    plt.legend()
    plt.show()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())


