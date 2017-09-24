# Working with Large CSV Files

I need to parse relatively large (tens to hundreds of GB) CSV files.
These files are too large to fit in memory, but [not really large enough for "big data"](https://www.chrisstucchio.com/blog/2013/hadoop_hatred.html) - Hadoop, etc.
So, I prefer to process them on a single machine.

## Problem Description

The CSV format is relatively simple:

- pipe-delimited
- no quotes
- no escapes

The above means that one row occupies a single line, and the delimiter never occurs inside a cell value.

The tasks I need to solve are, in approximate order of increasing difficulty:

- Histogram of row size (how many columns each row has)
- For each column:
  - Number and ratio of non-empty values
  - The maximum, minimum and mean lengths of the values
  - **Number of unique values** (this is the hard one)
  - **The top 20 most frequent values** (this is also hard)

## Questions

1. What is the bottleneck?  Is it I/O or parsing?
2. What is the fastest way to parse CSV?
3. Does it matter if you're reading a file from disk or from a pipe?
4. Is it faster to work with bytes instead of Unicode?
5. Does the version of Python (2 or 3) make a difference?

## Answers

### What is the Bottleneck?

Let's start with a "dumb" parser that splits the input into lines then columns and see how well it does:

```
bash-3.2$ pv sampledata.csv | kernprof -v -l read.py --reader dumb
 362MiB 0:00:27 [13.2MiB/s] [==================================================>] 100%
Counter({98: 699182})
[0, 474061, 474069, 474061, 233726, 639752, 43879, 43879, 272219, 0, 232697, 352034, 506889, 419834, 238963, 253763, 587626, 0, 267186, 267186, 270990, 435364, 206037, 206037, 458097, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 0, 523037, 0, 652521, 525829, 528151, 590963, 650090, 309059, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 583518, 583518, 0, 0, 0, 0, 632934, 632934, 0, 0, 0, 0, 403372, 403372, 0, 0, 0, 0, 682333, 682333, 147462, 179818, 146352, 215427, 166945, 351125, 335831, 201459, 681185, 0, 9, 192561, 192562, 609841, 664372, 664676, 657087, 657113, 471408, 471411, 464545, 570827, 570827, 535957, 535957]
Wrote profile results to read.py.lprof
Timer unit: 1e-06 s

Total time: 4.89751 s
File: read.py
Function: dumb_reader at line 11

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    11                                           @profile
    12                                           def dumb_reader(fin, **kwargs):
    13         1            3      3.0      0.0      delimiter = kwargs.get('delimiter', ',')
    14    699184       641110      0.9     13.1      for line in fin:
    15    699183       661942      0.9     13.5          stripped = line.rstrip('\n')
    16    699183      3314306      4.7     67.7          split = stripped.split(delimiter)
    17    699183       280150      0.4      5.7          yield split
```

The above output suggests that we spend the majority of our time parsing (67.7%), hinting that we may have a CPU bottleneck.
Indeed, if we look at the CPU usage of our process while it's running, it's close to 100%.
Unless our dumb parser is somehow extremely defective, we can conclude that we have a CPU bottleneck in this particular case.

### What is the Fastest Way to Parse CSV?

We have several options:

- We could roll our own, like we did above.
- The standard library has a [csv](https://docs.python.org/2/library/csv.html) module.
- Numpy and Pandas also have their own CSV readers
- Any others?

Someone has compared a variety of options on [StackExchange](https://softwarerecs.stackexchange.com/questions/7463/fastest-python-library-to-read-a-csv-file).
Their conclusions were that [numpy](https://docs.scipy.org/doc/numpy/) was the clear winner, followed by [pandas](http://pandas.pydata.org/).
Unfortunately, np.fromfile requires a very specific CSV format incompatible with our requirements, and the more robust np.loadtxt is notoriously slow.

So, let's try pandas:

```python
def pandas_reader(fin, **kwargs):
    delimiter = kwargs.get('delimiter', ',')
    names = fin.readline().rstrip('\n').split(delimiter)
    data_types = {name: str for name in names}
    data = pd.read_csv(fin, delimiter=delimiter, header=None, names=names, dtype=data_types,
                       quoting=csv.QUOTE_NONE, escapechar=None, engine='c')
    for index, series in data.iterrows():
        yield series.tolist()
```

This yields the entire thing into memory, which is something we want to avoid, but we can work on that in the future.
Let's time it:

```
bash-3.2$ time pv sampledata.csv | python read.py --reader pandas
 362MiB 0:00:10 [33.9MiB/s] [==================================================>] 100%
Counter({98: 699181})
[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

real    1m40.432s
user    1m36.533s
sys     0m2.762s
```

That's rather slow...
Pandas was quick to gobble up the file (10 seconds) but took a long time to process it (1min 40s).
(Did we do something wrong?)

Let's try the standard library's parser:

```
bash-3.2$ time pv sampledata.csv | python read.py --reader stdlib
 362MiB 0:00:27 [13.4MiB/s] [==================================================>] 100%
Counter({98: 699182})
[0, 474061, 474069, 474061, 233726, 639752, 43879, 43879, 272219, 0, 232697, 352034, 506889, 419834, 238963, 253763, 587626, 0, 267186, 267186, 270990, 435364, 206037, 206037, 458097, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 0, 523037, 0, 652521, 525829, 528151, 590963, 650090, 309059, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 583518, 583518, 0, 0, 0, 0, 632934, 632934, 0, 0, 0, 0, 403372, 403372, 0, 0, 0, 0, 682333, 682333, 147462, 179818, 146352, 215427, 166945, 351125, 335831, 201459, 681185, 0, 9, 192561, 192562, 609841, 664372, 664676, 657087, 657113, 471408, 471411, 464545, 570827, 570827, 535957, 535957]

real    0m27.086s
user    0m26.214s
sys     0m0.944s
```

Our dumb parser:

```
bash-3.2$ time pv sampledata.csv | python read.py --reader dumb

 362MiB 0:00:23 [15.3MiB/s] [==================================================>] 100%
Counter({98: 699182})
[0, 474061, 474069, 474061, 233726, 639752, 43879, 43879, 272219, 0, 232697, 352034, 506889, 419834, 238963, 253763, 587626, 0, 267186, 267186, 270990, 435364, 206037, 206037, 458097, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 0, 523037, 0, 652521, 525829, 528151, 590963, 650090, 309059, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 583518, 583518, 0, 0, 0, 0, 632934, 632934, 0, 0, 0, 0, 403372, 403372, 0, 0, 0, 0, 682333, 682333, 147462, 179818, 146352, 215427, 166945, 351125, 335831, 201459, 681185, 0, 9, 192561, 192562, 609841, 664372, 664676, 657087, 657113, 471408, 471411, 464545, 570827, 570827, 535957, 535957]

real    0m23.671s
user    0m23.294s
sys     0m0.788s
```

So the results so far are:

1. Our home-brewed parser: 23.7s
2. Standard library: 27.0s
3. Pandas: 1min 40s

Our simple but dumb parser ended up being the fastest, followed closely by the standard library's.
It's not really a fair comparison, because the standard library's parser is much more robust - it handles quoting, escape characters, multi-line rows, etc.
Nevertheless, for our limited purposes, the simple parser will do fine.

### Does It Matter If You're Reading from A File Or A Pipe?

```
bash-3.2$ time python read.py --reader dumb --file sampledata.csv
Counter({98: 699182})
[0, 474061, 474069, 474061, 233726, 639752, 43879, 43879, 272219, 0, 232697, 352034, 506889, 419834, 238963, 253763, 587626, 0, 267186, 267186, 270990, 435364, 206037, 206037, 458097, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 0, 523037, 0, 652521, 525829, 528151, 590963, 650090, 309059, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 583518, 583518, 0, 0, 0, 0, 632934, 632934, 0, 0, 0, 0, 403372, 403372, 0, 0, 0, 0, 682333, 682333, 147462, 179818, 146352, 215427, 166945, 351125, 335831, 201459, 681185, 0, 9, 192561, 192562, 609841, 664372, 664676, 657087, 657113, 471408, 471411, 464545, 570827, 570827, 535957, 535957]

real    0m24.319s
user    0m23.855s
sys     0m0.333s
```

This is about the same time as with the pipe, so the answer to this question is no.
There are advantages and disadvantages to both options:

- When reading from a pipe you can read output from other processes, but can't seek around.
- When reading from a file, you can seek around it, but you're stuck with that file being on disk.

### Is It Faster To Work With Bytes or Unicode?

CSV is typically a text format.
However, in our particular case, we may get away with treating it as binary, because our separator is a pipe, which has the same value (character code) regardless of whether it's encoded as e.g. UTF-8.
If we go down this path, then we'll be calculating the _byte length_ of the values, not the _character length_.
The alternative is to decode the binary data as UTF-8 prior to CSV parsing:

```python
def dumb_unicode(fin, **kwargs):
    delimiter = kwargs.get('delimiter', ',')
    for line in fin:
        decoded = line.decode('utf-8')
        stripped = decoded.rstrip(u'\n')
        split = stripped.split(delimiter)
        yield split
```

but this comes at a price:

```
bash-3.2$ time pv sampledata.csv | python read.py --reader dumb_unicode
 362MiB 0:00:27 [  13MiB/s] [==================================================>] 100%
Counter({98: 699182})
[0, 474061, 474069, 474061, 233726, 639752, 43879, 43879, 272219, 0, 232697, 352034, 506889, 419834, 238963, 253763, 587626, 0, 267186, 267186, 270990, 435364, 206037, 206037, 458097, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 0, 523037, 0, 652521, 525829, 528151, 590963, 650090, 309059, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 583518, 583518, 0, 0, 0, 0, 632934, 632934, 0, 0, 0, 0, 403372, 403372, 0, 0, 0, 0, 682333, 682333, 147462, 179818, 146352, 215427, 166945, 351125, 335831, 201459, 681185, 0, 9, 192561, 192562, 609841, 664372, 664676, 657087, 657113, 471408, 471411, 464545, 570827, 570827, 535957, 535957]

real    0m27.874s
user    0m27.449s
sys     0m0.843s
```

This price amounts to approx. a 10% increase in processing time.

### Does the Version of Python (2 or 3) Make a Difference?

If we were using the standard library's parser, then yes, it'd make a difference because that parser decodes binary data first (bytes to Unicode), which can cost.
If we were using our own parser, then it wouldn't matter much, as long as we make sure there are no implicit conversions between bytes and Unicode, because Python 3 doesn't like those.

## Halfway Summary

1. What is the bottleneck?  CPU.
2. What is the fastest way to parse CSV?  Write our own simple parser.
3. Does it matter if you're reading a file from disk or from a pipe?  No.
4. Is it faster to work with bytes instead of Unicode?  Yes.
5. Does the version of Python (2 or 3) make a difference?  Maybe.

## Can You Make It Go Faster?

We have a CPU-bound problem.
Even when Python (CPython, to be more precise) runs on a multi-core machine, because of the [GIL](https://stackoverflow.com/questions/1294382/what-is-a-global-interpreter-lock-gil), each Python process can only use a single core.
So if we can let our other cores join the party, processing should happen faster.
Our processing consists of the following steps:

1. Read bytes (I/O bound)
2. Parse CSV (CPU bound)
3. Count non-empty values, etc. (CPU bound)

The bottleneck is steps 2 and 3, so offloading them to multiple processes makes sense:

```
Bytes -> Reader -> Processor 1 -> Collator
                -> Processor 2 ->
                -> ...
                -> Processor N ->
```

Let's see how this implementation goes:

```
bash-3.2$ time pv sampledata.csv | python multiread.py
 362MiB 0:00:20 [17.4MiB/s] [==================================================>] 100%
Counter({98: 699182})
[0, 474061, 474069, 474061, 233726, 639752, 43879, 43879, 272219, 0, 232697, 352034, 506889, 419834, 238963, 253763, 587626, 0, 267186, 267186, 270990, 435364, 206037, 206037, 458097, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 582415, 0, 523037, 0, 652521, 525829, 528151, 590963, 650090, 309059, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 583518, 583518, 0, 0, 0, 0, 632934, 632934, 0, 0, 0, 0, 403372, 403372, 0, 0, 0, 0, 682333, 682333, 147462, 179818, 146352, 215427, 166945, 351125, 335831, 201459, 681185, 0, 9, 192561, 192562, 609841, 664372, 664676, 657087, 657113, 471408, 471411, 464545, 570827, 570827, 535957, 535957]

real    0m21.673s
user    1m3.031s
sys     0m13.219s
```

This kept all of our 4 cores busy: 100% use during the running time of the program.
However, it actually doesn't buy us _that_ much: only 1-2s, which is a modest increase.
This is because multiprocessing also comes at the price of additional I/O overhead between the subprocesses.

Is it worth it?
In this particular case, no, not really.
However, if we were doing some more CPU-intensive processing, then the benefit of using additional CPU cores would outweight the cost of I/O overhead.
Let's make our processor more feature complete, and keep a track of the maximum, minimum and average lengths.
