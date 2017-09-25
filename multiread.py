from __future__ import print_function
from __future__ import division

import collections
import logging
import multiprocessing
import sys

_DELIMITER = '|'
_NUM_WORKERS = multiprocessing.cpu_count()
_SENTINEL = None


def parse_line(line):
    return line.rstrip('\n').split(_DELIMITER)


# @profile
def read(line_queue, header, result_queue):
    logging.debug('args: %r', locals())
    counter = collections.Counter()
    column_lengths = [list() for _ in header]
    while True:
        line = line_queue.get()
        if line is _SENTINEL:
            break
        row = parse_line(line)
        logging.debug('row: %r', row)
        row_len = len(row)
        counter[row_len] += 1
        if row_len != len(header):
            continue
        for j, column in enumerate(row):
            column_lengths[j].append(len(column))

    logging.debug('column_lengths: %r', column_lengths)

    fill_count, min_len, max_len, sum_len = zip(
        *[[sum(x > 0 for x in l), min(l), max(l), sum(l)] for l in column_lengths]
    )
    logging.debug('fill_count: %r', fill_count)
    result_queue.put((counter, list(fill_count), list(max_len), list(min_len), list(sum_len)))


def collate(header, results):
    all_counter = collections.Counter()
    all_fill_count = [0 for _ in header]
    all_max = [0 for _ in header]
    all_min = [sys.maxint for _ in header]
    all_sum = [0 for _ in header]
    for (counter, fill_count, max_len, min_len, sum_len) in results:
        all_counter.update(counter)
        for i, _ in enumerate(header):
            all_fill_count[i] += fill_count[i]
            all_max[i] = max(all_max[i], max_len[i])
            all_min[i] = min(all_min[i], min_len[i])
            all_sum[i] += sum_len[i]

    num_rows = sum(all_counter.values())
    all_avg = [the_sum / num_rows for the_sum in all_sum]
    return {
        'counter': all_counter, 'fill_count': all_fill_count, 'max_len': all_max,
        'min_len': all_min, 'avg_len': all_avg
    }


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
    result = collate(header, (result_queue.get() for _ in workers))

    print(result['counter'])
    print(result['fill_count'])
    print(result['max_len'])
    print(result['min_len'])
    print(result['avg_len'])


def read_stream(stream):
    header = parse_line(stream.readline())
    result_queue = multiprocessing.Queue()
    read(FakeQueue((line for line in stream)), header, result_queue)
    result = result_queue.get()
    return collate(header, [result])


class FakeQueue(object):
    def __init__(self, iterator):
        self._iterator = iterator

    def get(self):
        try:
            return next(self._iterator)
        except StopIteration:
            return _SENTINEL


if __name__ == '__main__':
    read_stream(sys.stdin)
    # main()
