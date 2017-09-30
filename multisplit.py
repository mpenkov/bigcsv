"""Split a CSV into columns, one file per column."""
from __future__ import print_function
import csv
import collections
import sys
import threading
import Queue

SENTINEL = None
DEFAULT_BATCH_SIZE = 10000  # Empirically proven to work best


def open_file(fpath, mode):
    return open(fpath, mode)


def writer_thread(queue_in, fpath_out, open_):
    with open_(fpath_out, 'wb') as fout:
        lines = True
        while lines is not SENTINEL:
            lines = queue_in.get()
            if lines is not SENTINEL:
                fout.write(b'\n'.join(lines) + b'\n')
            queue_in.task_done()


def make_batches(iterable, batch_size=DEFAULT_BATCH_SIZE):
    batch = []
    for row in iterable:
        if len(batch) == batch_size:
            yield batch
            batch = []
        batch.append(row)
    if batch:
        yield batch


def populate_queues(header, reader, queues):
    histogram = collections.Counter()
    for batch in make_batches(reader):
        histogram.update(len(row) for row in batch)
        columns = [list() for _ in header]
        for row in batch:
            if len(header) != len(row):
                continue
            for col_num, value in enumerate(row):
                columns[col_num].append(value)
        for queue, values in zip(queues, columns):
            queue.put(values)
    for queue in queues:
        queue.put(SENTINEL)
    return histogram


def split(fin, open_file=open_file):
    reader = csv.reader(fin, delimiter='|')
    header = next(reader)
    paths = ['gitignore/col-%d.txt' % col_num for col_num, col_name in enumerate(header)]
    queues = [Queue.Queue() for _ in paths]
    threads = [threading.Thread(target=writer_thread, args=(queue, path, open_file))
               for (queue, path) in zip(queues, paths)]
    for thread in threads:
        thread.start()

    histogram = populate_queues(header, reader, queues)

    for queue in queues:
        queue.join()

    return histogram


if __name__ == '__main__':
    histogram = split(sys.stdin)
    print(histogram)
