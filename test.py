from __future__ import division
import collections
import io
import logging
import Queue

import pytest

import read
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
