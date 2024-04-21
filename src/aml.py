#!/usr/bin/python3
from ctypes import *
import pandas as pd

pattern_names = [
    "UNDECIDED",
    "CLEAN",
    "FAN_OUT",
    "FAN_IN",
    "GATHER_SCATTER",
    "SCATTER_GATHER",
    "CYCLE",
    "RANDOM",
    "BIPARTITE",
    "STACK",
]

# period = 691200
period = 1000
transactionThreshold = 141
launderingEfficiency = 0.8

all_patterns = []

if __name__ == "__main__":
    lib = cdll.LoadLibrary('../lib/libpatterndetect.so')

    lib.extern_get_pattern.argtypes = [c_uint32, c_uint32, c_double, c_double]

    lib.extern_load_data(
        b'/home/calin/Documents/git/HBN2/resources/predicted.csv')

    df = pd.read_csv(
        '/home/calin/Documents/git/HBN2/resources/initial_predicted.csv')
    print(df)
    for transactionId in range(len(df)):
        print(str(transactionId), pattern_names[lib.extern_get_pattern(
            transactionId, period, transactionThreshold, launderingEfficiency)])
        pattern = lib.extern_get_pattern(
            transactionId, period, transactionThreshold, launderingEfficiency)
        all_patterns.append(pattern_names[pattern])
    print(all_patterns)
    df['Pattern'] = all_patterns
    df.to_csv('/home/calin/Documents/git/HBN2/resources/augmented.csv')
