"""Split a CSV into columns, one file per column."""
from __future__ import print_function
import csv
import sys


def open_file(fpath, mode):
    return open(fpath, mode)


def split(fin, open_file=open_file):
    reader = csv.reader(fin, delimiter='|')
    header = next(reader)
    paths = ['gitignore/col-%d.txt' % col_num for col_num, col_name in enumerate(header)]
    fouts = [open(path, 'wb') for path in paths]
    for row in reader:
        for col_num, col_value in enumerate(row):
            fouts[col_num].write(col_value + b'\n')
    for fout in fouts:
        fout.close()
    return paths


if __name__ == '__main__':
    paths = split(sys.stdin)
    for path in paths:
        print(path)
