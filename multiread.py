from __future__ import print_function
from __future__ import division

import collections
import multiprocessing
import sys

_DELIMITER = '|'
_NUM_WORKERS = multiprocessing.cpu_count()
_SENTINEL = None


def parse_line(line):
    return line.rstrip('\n').split(_DELIMITER)


@profile
def read(line_queue, header, result_queue):
    counter = collections.Counter()
    fill_count = [0 for _ in header]
    max_len = [0 for _ in header]
    min_len = [sys.maxint for _ in header]
    sum_len = [0 for _ in header]
    while True:
        line = line_queue.get()
        if line is _SENTINEL:
            break
        row = parse_line(line)
        row_len = len(row)
        counter[row_len] += 1
        if row_len != len(header):
            continue
        for j, column in enumerate(row):
            col_len = len(column)
            max_len[j] = max(max_len[j], col_len)
            min_len[j] = min(min_len[j], col_len)
            sum_len[j] += col_len
            if col_len > 0:
                fill_count[j] += 1
    result_queue.put((counter, fill_count, max_len, min_len, sum_len))


def collate(header, results):
    all_counter = collections.Counter()
    all_fill_count = [0 for _ in header]
    all_max = [0 for _ in header]
    all_min = [0 for _ in header]
    all_sum = [0 for _ in header]
    for counter, fill_count, max_len, min_len, sum_len in results:
        all_counter.update(counter)
        for i, _ in enumerate(header):
            all_fill_count[i] += fill_count[i]
            all_max[i] = max(all_max[i], max_len[i])
            all_min[i] = min(all_min[i], min_len[i])
            all_sum[i] += sum_len[i]

    num_rows = sum(all_counter.values())
    all_avg = [the_sum / num_rows for the_sum in all_sum]
    return all_counter, all_fill_count, all_max, all_min, all_avg


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
    counter, fill_count, max_len, min_len, avg_len = collate(
        header, (result_queue.get() for _ in workers)
    )
    print(counter)
    print(fill_count)
    print(max_len)
    print(min_len)
    print(avg_len)


def main_singleprocess():
    class FakeQueue(object):
        def get(self):
            line = sys.stdin.readline()
            if line:
                return line
            return _SENTINEL
    header = parse_line(sys.stdin.readline())
    result_queue = multiprocessing.Queue()
    read(FakeQueue(), header, result_queue)
    counter, fill_count, max_len, min_len, avg_len = result_queue.get()

    print(counter)
    print(fill_count)
    print(max_len)
    print(min_len)
    print(avg_len)

if __name__ == '__main__':
    main_singleprocess()
