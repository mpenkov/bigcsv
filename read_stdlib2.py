from __future__ import print_function

import csv
import sys

magic = u'\u00e9'
reader = csv.reader(sys.stdin, delimiter=b'|', quoting=csv.QUOTE_NONE, escapechar=b'')
counter = 0
for row in reader:
    for col in row:
        if magic in col.decode('utf-8'):
            counter += 1
print(counter, file=sys.stdout)
