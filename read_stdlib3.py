import csv
import sys

magic = u'\u00e9'
reader = csv.reader(sys.stdin, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='')
counter = 0
for row in reader:
    for col in row:
        if magic in col:
            counter += 1
print(counter, file=sys.stdout)
