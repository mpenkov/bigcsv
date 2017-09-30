from __future__ import division
import collections
import io
import logging
import Queue

import pytest

import read
import read_column
import multiread
import multisplit

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture
def toy_example():
    stream = io.BytesIO("a|b\n"
                        "1|\n"
                        "foobar|baz\n"
                        "x")
    expected = {
        'counter': collections.Counter({2: 2, 1: 1}),
        'fill_count': [2, 1],
        'max_len': [6, 3],
        'min_len': [1, 0],
        'avg_len': [7/3, 1],
    }
    return stream, expected


def test_read(toy_example):
    stream, expected = toy_example
    actual = read.read_stream(stream)
    assert expected == actual


def test_multiread(toy_example):
    stream, expected = toy_example
    actual = multiread.read_stream(stream)
    assert expected == actual


def never_close(buf):
    buf.close = lambda: None
    return buf


def test_writer_thread():
    buf = never_close(io.BytesIO())

    def open_(fpath, mode):
        return buf

    queue = Queue.Queue()
    for batch in (['foo', 'bar'], ['baz'], multisplit.SENTINEL):
        queue.put(batch)

    multisplit.writer_thread(queue, '/dummy/file', open_)
    expected = b'foo\nbar\nbaz\n'
    assert buf.getvalue() == expected


def test_make_batches():
    assert list(multisplit.make_batches([1, 2], 1)) == [[1], [2]]
    assert list(multisplit.make_batches([1, 2], 2)) == [[1, 2]]
    assert list(multisplit.make_batches([1, 2, 3], 2)) == [[1, 2], [3]]


def test_read_column():
    column = io.BytesIO(b'\n1\n2\n2\n3\n3\n3\naa\n')
    expected = {
        'num_values': 8,
        'num_fills': 7,
        'fill_ratio': 7./8,
        'max_len': 2,
        'min_len': 0,
        'avg_len': 1.0,
        'num_uniques': 5
    }
    assert read_column.read_column(column) == expected


def test_run_length_encode():
    expected = [(1, 1), (2, 2), (3, 3)]
    actual = list(read_column.run_length_encode(iter([1, 2, 2, 3, 3, 3])))
    assert expected == actual

    with pytest.raises(ValueError):
        list(read_column.run_length_encode(iter([2, 1])))
