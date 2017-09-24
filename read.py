from __future__ import print_function
import argparse
import collections
import csv
import io
import pandas as pd
import sys

import backports.csv


# @profile
def dumb_reader(fin, **kwargs):
    delimiter = kwargs.get('delimiter', ',')
    for line in fin:
        stripped = line.rstrip('\n')
        split = stripped.split(delimiter)
        yield split


def dumb_unicode_reader(fin, **kwargs):
    delimiter = kwargs.get('delimiter', ',')
    for line in fin:
        decoded = line.decode('utf-8')
        stripped = decoded.rstrip(u'\n')
        split = stripped.split(delimiter)
        yield split


def stdlib_reader(fin, **kwargs):
    reader = csv.reader(fin, **kwargs)
    for row in reader:
        yield row


def backports_reader(fin, **kwargs):
    reader = backports.csv.reader(fin, **kwargs)
    for row in reader:
        yield row


def pandas_reader(fin, **kwargs):
    delimiter = kwargs.get('delimiter', ',')
    names = fin.readline().rstrip('\n').split(delimiter)
    yield names
    data_types = {name: str for name in names}
    data = pd.read_csv(fin, delimiter=delimiter, header=None, names=names, dtype=data_types,
                       quoting=csv.QUOTE_NONE, escapechar=None, engine='c')
    for index, series in data.iterrows():
        yield series.tolist()


_READERS = {'dumb': dumb_reader, 'stdlib': stdlib_reader,
            'backports': backports_reader, 'pandas': pandas_reader,
            'dumb_unicode': dumb_unicode_reader}


def read(reader):
    counter = collections.Counter()
    header = next(reader)
    fill_count = [0 for _ in header]
    for i, row in enumerate(reader):
        counter[len(row)] += 1
        if len(row) != len(header):
            continue
        for j, column in enumerate(row):
            if column == '':
                fill_count[j] += 1
    print(counter)
    print(fill_count)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reader', default='dumb', choices=sorted(_READERS.keys()))
    parser.add_argument('--file')
    parser.add_argument('--delimiter', default='|')
    args = parser.parse_args()
    stream = sys.stdin if args.file is None else open(args.file, 'r')
    reader = _READERS[args.reader](stream, delimiter=args.delimiter)
    read(reader)


if __name__ == '__main__':
    main()
