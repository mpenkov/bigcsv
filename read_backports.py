from __future__ import print_function
from __future__ import unicode_literals

import io
import sys

import six
if six.PY2:
    from backports import csv
else:
    import csv


def wrap_stdio(stdin=sys.stdin, stdout=sys.stdout, encoding='utf-8'):
    if six.PY3:
        return stdin, stdout
    stdin = io.TextIOWrapper(stdin.buffer, encoding=encoding)
    stdout = io.TextIOWrapper(stdout.buffer, encoding=encoding)
    return stdin, stdout


stdin, stdout = wrap_stdio()
magic = u'\u00e9'
reader = csv.reader(stdin, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='')
counter = 0
for row in reader:
    for col in row:
        if magic in col:
            counter += 1
print(counter, file=stdout)
