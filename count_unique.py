from __future__ import print_function

import sys


def count_unique(values):
    num_unique = 1
    prev_value = next(values)
    for value in values:
        if value != prev_value:
            num_unique += 1
            prev_value = value
    return num_unique


def main():
    print(count_unique(line for line in sys.stdin))


if __name__ == '__main__':
    main()
