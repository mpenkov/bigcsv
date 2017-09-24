from __future__ import print_function

import collections
import multiprocessing
import sys

_DELIMITER = '|'
_NUM_WORKERS = multiprocessing.cpu_count()
_SENTINEL = None


def parse_line(line):
    return line.rstrip('\n').split(_DELIMITER)


def read(line_queue, header, result_queue):
    counter = collections.Counter()
    fill_count = [0 for _ in header]
    while True:
        line = line_queue.get()
        if line is _SENTINEL:
            break
        row = parse_line(line)
        counter[len(row)] += 1
        if len(row) != len(header):
            continue
        for j, column in enumerate(row):
            if column == '':
                fill_count[j] += 1
    result_queue.put((counter, fill_count))


def collate(header, results):
    all_counter = collections.Counter()
    all_fill_count = [0 for _ in header]
    for counter, fill_count in results:
        all_counter.update(counter)
        for i, value in enumerate(fill_count):
            all_fill_count[i] += value
    return all_counter, all_fill_count


def main():
    header = parse_line(sys.stdin.readline())
    line_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    workers = [multiprocessing.Process(target=read, args=(line_queue, header, result_queue))
               for _ in range(_NUM_WORKERS)]
    for worker in workers:
        worker.start()
    for line in sys.stdin:
        line_queue.put(line)
    for _ in workers:
        line_queue.put(_SENTINEL)
    for worker in workers:
        worker.join()
    counter, fill_count = collate(header, (result_queue.get() for _ in workers))
    print(counter)
    print(fill_count)


if __name__ == '__main__':
    main()
