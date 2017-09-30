import json
import os
import sys

import multisplit
import summarize


def main():
    histogram, paths = multisplit.split(sys.stdin)
    sys.stdout.write(json.dumps(histogram, sort_keys=True) + '\n')
    results = summarize.multi_summarize(paths)
    for path, result in zip(paths, results):
        result['_path'] = path
        sys.stdout.write(json.dumps(result, sort_keys=True) + '\n')
        os.unlink(path)


if __name__ == '__main__':
    main()
