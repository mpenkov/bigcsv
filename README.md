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

The machine this needs to run on only has Py2, but I'll try to keep this as version-agnostic as possible.

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

The best option is to handle both, if you can, and let someone else decide what's best for them.

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
Let's make our processor more feature-complete, and keep a track of the maximum, minimum and average lengths.

```
bash-3.2$ time pv sampledata.csv | python multiread.py > /dev/null
 362MiB 0:00:57 [ 6.3MiB/s] [==================================================>] 100%

real    1m0.263s
user    3m29.807s
sys     0m9.886s
bash-3.2$ time pv sampledata.csv | python read.py --reader dumb > /dev/null
 362MiB 0:01:33 [3.89MiB/s] [==================================================>] 100%

real    1m33.120s
user    1m31.859s
sys     0m1.486s
```

We've slightly increased the computational complexity of our processing.
The difference in execution time is now much more visible: on our 4-core laptop, the multiprocessing version is 1.5 times faster.
I expect that as the processing gets more CPU-hungry (there is still more to do), this lead will continue to increase.

## Can You Make It Go Even Faster?

Let's have a look what's taking up the most time:

```
bash-3.2$ time pv sampledata.csv | kernprof -v -l multiread.py
... snip ...
Wrote profile results to multiread.py.lprof
Timer unit: 1e-06 s

Total time: 375.052 s
File: multiread.py
Function: read at line 17

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    17                                           @profile
    18                                           def read(line_queue, header, result_queue):
    19         1           29     29.0      0.0      counter = collections.Counter()
    20        99          119      1.2      0.0      fill_count = [0 for _ in header]
    21        99          103      1.0      0.0      max_len = [0 for _ in header]
    22        99          120      1.2      0.0      min_len = [sys.maxint for _ in header]
    23        99           73      0.7      0.0      sum_len = [0 for _ in header]
    24    699183       431777      0.6      0.1      while True:
    25    699183      4638839      6.6      1.2          line = line_queue.get()
    26    699183       528746      0.8      0.1          if line is _SENTINEL:
    27         1            1      1.0      0.0              break
    28    699182      5794840      8.3      1.5          row = parse_line(line)
    29    699182       558756      0.8      0.1          row_len = len(row)
    30    699182      1169373      1.7      0.3          counter[row_len] += 1
    31    699182       574065      0.8      0.2          if row_len != len(header):
    32                                                       continue
    33  69219018     46546899      0.7     12.4          for j, column in enumerate(row):
    34  68519836     47239480      0.7     12.6              col_len = len(column)
    35  68519836     67689088      1.0     18.0              max_len[j] = max(max_len[j], col_len)
    36  68519836     67689050      1.0     18.0              min_len[j] = min(min_len[j], col_len)
    37  68519836     56056394      0.8     14.9              sum_len[j] += col_len
    38  68519836     44426354      0.6     11.8              if col_len > 0:
    39  38433446     31707478      0.8      8.5                  fill_count[j] += 1
    40         1          445    445.0      0.0      result_queue.put((counter, fill_count, max_len, min_len, sum_len))


real    10m34.739s
user    10m25.871s
sys     0m4.919s
```

We're spending a lot of time in the inner loop.
Is there any way we can speed this up?
One idea is to look at batching up the max, min and sum operations.
This would require keeping column lengths in memory.

```
bash-3.2$ time pv sampledata.csv | kernprof -v -l multiread.py
... snip...
Wrote profile results to multiread.py.lprof
Timer unit: 1e-06 s

Total time: 190.55 s
File: multiread.py
Function: read at line 17

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    17                                           @profile
    18                                           def read(line_queue, header, result_queue):
    19         1           41     41.0      0.0      counter = collections.Counter()
    20        99          159      1.6      0.0      column_lengths = [list() for _ in header]
    21    699183       672771      1.0      0.4      while True:
    22    699183      8128471     11.6      4.3          line = line_queue.get()
    23    699183       852492      1.2      0.4          if line is _SENTINEL:
    24         1            0      0.0      0.0              break
    25    699182      9638751     13.8      5.1          row = parse_line(line)
    26    699182       888174      1.3      0.5          row_len = len(row)
    27    699182      2236587      3.2      1.2          counter[row_len] += 1
    28    699182       860060      1.2      0.5          if row_len != len(header):
    29                                                       continue
    30  69219018     66830359      1.0     35.1          for j, column in enumerate(row):
    31  68519836     92805650      1.4     48.7              column_lengths[j].append(len(column))
    32
    33         1            1      1.0      0.0      fill_count, min_len, max_len, sum_len = zip(
    34        99      7632206  77093.0      4.0          *[(l.count(0), min(l), max(l), sum(l)) for l in column_lengths]
    35                                               )
    36         1         3872   3872.0      0.0      result_queue.put((counter, fill_count, max_len, min_len, sum_len))


real    5m9.481s
user    4m21.393s
sys     0m6.887s
```

This is significant.
We've nearly halved our execution time, at the expense of storing all the column lengths in memory.
We keep these benefits when we stop profiling and go back to using multiple cores:

```
bash-3.2$ time python multiread.py < sampledata.csv > /dev/null

real    0m30.520s
user    1m27.877s
sys     0m13.709s
```

But what about the price we paid?
We're keeping the length of each column in memory.
We have hundreds of columns (1e2), and potentially hundreds of millions (1e8) of rows.
This means we'll be keeping tens of billions (1e10) of _integers_ in memory.
Python integers are a whopping 24 bytes, so we could need trillions (1e12) of bytes.
This is a pretty _rough_ estimate, as it doesn't take into account some cool things like [integer interning](https://docs.python.org/2/c-api/int.html).
But it still sounds little bit more than what we have available, so...  what do we do next?

## But Wait, How Do You Know This Thing Still Works?

We've been doing something very naughty: writing code without writing tests.
It's time we redeem ourselves and write some:

```
bash-3.2$ py.test test.py -q
..
2 passed in 0.33 seconds
```

This way, we know that our refactorings don't break anything down the road.
As a bonus, the tests also caught several bugs in multiread.py.

## Memory Profiling

We can use [pympler](https://pythonhosted.org/Pympler/) to tell us the true size of our column length lists:

```python
from pympler.asizeof import asizeof

logging.info('num_rows: %r asizeof(column_lengths): %.2f MB',
             sum(counter.values()), asizeof(column_lengths) / 1024**2)
```

This costs time to calculate, but it's worth knowing at the moment:

```
bash-3.2$ time pv sampledata.csv | python multiread.py > /dev/null
 362MiB 0:00:25 [14.3MiB/s] [==================================================>] 100%
INFO:root:num_rows: 174431 asizeof(column_lengths): 139.21 MB
INFO:root:num_rows: 174025 asizeof(column_lengths): 139.20 MB
INFO:root:num_rows: 175471 asizeof(column_lengths): 139.21 MB
INFO:root:num_rows: 175255 asizeof(column_lengths): 139.20 MB

real    1m18.618s
user    4m33.919s
sys     0m13.292s
```

Wow, at this rate, we'll be paying 1GB of memory for each 1M rows.
Let's see if we can cut that down a bit.

We don't really need to keep a _list_ of all the length of each columns.
A tally (length, number of columns) will do.
We can implement that easily using a [collections.Counter](https://docs.python.org/2/library/collections.html) and observe a significant drop in memory usage:

```
bash-3.2$ time pv sampledata.csv | python multiread.py > /dev/null
 362MiB 0:00:38 [9.32MiB/s] [==================================================>] 100%
INFO:root:num_rows: 174287 asizeof(column_lengths): 0.34 MB
INFO:root:num_rows: 175428 asizeof(column_lengths): 0.34 MB
INFO:root:num_rows: 175052 asizeof(column_lengths): 0.37 MB
INFO:root:num_rows: 174415 asizeof(column_lengths): 0.34 MB

real    0m40.787s
user    2m10.863s
sys     0m11.074s
```

This is after making sure our tests still pass, of course :)

However, once we disable pympler, we'll find that our code runs slower than before, when everything was in memory:

```
bash-3.2$ time python multiread.py < sampledata.csv > /dev/null

real    0m36.809s
user    2m4.985s
sys     0m9.345s
```

If we profile our read function again, we'll see why:

```
bash-3.2$ time pv sampledata.csv | kernprof -v -l multiread.py
... snip...

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    29                                           @profile
    30                                           def read(line_queue, header, result_queue):
    31         1           18     18.0      0.0      logging.debug('args: %r', locals())
    32         1           15     15.0      0.0      counter = collections.Counter()
    33        99          596      6.0      0.0      column_lengths = [collections.Counter() for _ in header]
    34    699183       475593      0.7      0.3      while True:
    35    699183      2239169      3.2      1.4          line = line_queue.get()
    36    699183       539042      0.8      0.3          if line is _SENTINEL:
    37         1            0      0.0      0.0              break
    38    699182      5573801      8.0      3.5          row = parse_line(line)
    39    699182      3890694      5.6      2.5          logging.debug('row: %r', row)
    40    699182       567693      0.8      0.4          row_len = len(row)
    41    699182       905227      1.3      0.6          counter[row_len] += 1
    42    699182       597672      0.9      0.4          if row_len != len(header):
    43                                                       continue
    44  69219018     49692721      0.7     31.6          for j, column in enumerate(row):
    45  68519836     92626690      1.4     59.0              column_lengths[j][len(column)] += 1
... snip...
```

Updating the counters one by one is sub-optimal.
Perhaps if we cached a few rows in memory before updating our counters, things'd be a bit faster?

Let's write a new class to abstract away our counter-updating details:

```python
class BufferingCounter(object):
    def __init__(self, header, maxbufsize=10000):
        self._header = header
        self._maxbufsize = maxbufsize
        self._counters = [collections.Counter() for _ in self._header]
        self._buffer = [list() for _ in self._header]
        self._bufsize = 0

    def add_row(self, row):
        for j, column in enumerate(row):
            self._buffer[j].append(len(column))
        self._bufsize += 1

        if self._bufsize % self._maxbufsize == 0:
            self.flush_buffer()

    def flush_buffer(self):
        for j, values in enumerate(self._buffer):
            self._counters[j].update(values)
        self._buffer = [list() for _ in self._header]
        self._bufsize = 0
```

Looks good on paper, but the results aren't what we expected - it's actually _slower_ than before:

```
bash-3.2$ time python multiread.py < sampledata.csv > /dev/null

real    0m52.460s
user    2m45.889s
sys     0m12.671s
```

The profiler tells us why:

```
bash-3.2$ time pv sampledata.csv | kernprof -v -l multiread.py
... snip...
Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    38                                               @profile
    39                                               def add_row(self, row):
    40  69219018     34960103      0.5     21.4          for j, column in enumerate(row):
    41  68519836     56867368      0.8     34.8              self._buffer[j].append(len(column))
    42    699182       544912      0.8      0.3          self._bufsize += 1
    43
    44    699182       609252      0.9      0.4          if self._bufsize % self._maxbufsize == 0:
    45        69     70347319 1019526.4     43.1              self.flush_buffer()
... snip...
```

Flushing the buffer and updating the underlying collections.Counter takes a surprising amount of time.

It doesn't look like this approach will work, so we're stuck with updating the Counters one-by-one :(

## But What about the Hard Problems?

If you recall, the harder problems to solve were:

1. Calculate the exact number of unique values for each column
2. Calculate the 20 most frequent values

Let's look at the first problem.
In theory, assuming we've isolated the values for a single column, this kind of task is easy to achieve:

```python
num_unique = len(set(all_values_in_column))
```

Unfortunately, this requires loading the entire column (or at least all the unique items) into memory.
In our case, this number is much larger than the amount of memory we have, so we have to look at other methods.

**Assuming our list is sorted**, then we can calculate the number of unique items without keeping the whole thing in memory:

```python
def count_unique(values):
    num_unique = 1
    prev_value = next(values)
    for value in values:
        if value != prev_value:
            num_unique += 1
            prev_value = value
    return num_unique
```

Assuming a large list can be sorted without violating memory limits is actually not that bold an assumption.
In fact, [GNU sort]() does that sort of thing all the time: it writes temporary results to disk, and then merges them when necessary.
Of course, we could implement the same thing in Python, but why bother?
We could just pipe our column values into GNU sort, and read the sorted values back.
So our pipeline for a single column looks like:

```
      extract             sort                   count_unique
CSV ----------> column ----------> sorted columns ----------> num_unique
```

That works for one column, but what about multiple columns?
We have several options:

1. Repeat the above process (extract, sort, count_unique) for each column
2. Extract all columns to individual files, then run each file through sort_unique
3. Anything I've missed?

The first option is attractive because it's simple, but it requires running the I/O-expensive extract step for each column.
How does the cost of the extract step compare to the sort step?
Let's find out.

Extract only:

```
bash-3.2$ for col in 1 2 3 4 5; do time python extract.py $col < sampledata.csv > /dev/null; done

real    0m5.017s
user    0m4.832s
sys     0m0.171s

real    0m5.205s
user    0m5.016s
sys     0m0.176s

real    0m5.279s
user    0m5.086s
sys     0m0.182s
```

Extract then sort:

```
bash-3.2$ for col in 1 2 3; do time bash -c "python extract.py $col < sampledata.csv | LC_ALL=C sort" > /dev/null; done

real    0m5.283s
user    0m5.091s
sys     0m0.201s

real    0m5.259s
user    0m5.065s
sys     0m0.202s

real    0m5.238s
user    0m5.047s
sys     0m0.196s
```

I/O is significantly more expensive that sorting: the former takes seconds, the latter takes hundreds of milliseconds.
If we have a hundred columns, the extraction alone can take nearly 10 minutes, which is _way_ too long to wait for a file with under a million rows.
Let's try the second option: extract all columns to individual files, then run each file through sort and then count_unique.

Extract:

```
(bigcsv)bash-3.2$ time python split.py < sampledata.csv | head
gitignore/col-0.txt
gitignore/col-1.txt
gitignore/col-2.txt
gitignore/col-3.txt
gitignore/col-4.txt
gitignore/col-5.txt
... snip ...

real    0m59.243s
user    0m57.020s
sys     0m1.183s
```

Sort and count_unique:

```
bash-3.2$ time for f in gitignore/*.txt; do echo -n "$f "; LC_ALL=C sort $f | python count_unique.py; done
gitignore/col-0.txt 699182
gitignore/col-1.txt 218086
gitignore/col-10.txt 153250
gitignore/col-11.txt 38320
gitignore/col-12.txt 538
... snip...

real    0m37.239s
user    0m40.164s
sys     0m2.635s
```

So extracting our 97 columns took 1 min; sorting and count_unique took 40s.
That's far better than the first option we looked at, which would have taken close to 10 min.

If we wanted to speed things up, we could apply some of the tricks from above, as well as:

1. Profile split.py
2. Performs the sorts in parallel

If we profile, we get this:

```
Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    11                                           @profile
    12                                           def split(fin, open_file=open_file):
    13         1           10     10.0      0.0      reader = csv.reader(fin, delimiter='|')
    14         1          110    110.0      0.0      header = next(reader)
    15        99          215      2.2      0.0      paths = ['gitignore/col-%d.txt' % col_num for col_num, col_name in enumerate(header)]
    16        99      1947987  19676.6      9.9      fouts = [open(path, 'wb') for path in paths]
    17    100000       919215      9.2      4.7      for row in reader:
    18   9899901      5419047      0.5     27.6          for col_num, col_value in enumerate(row):
    19   9799902     11272701      1.2     57.5              fouts[col_num].write(col_value + b'\n')
    20        99          200      2.0      0.0      for fout in fouts:
    21        98        61326    625.8      0.3          fout.close()
    22        99          128      1.3      0.0      for path in paths:
    23        98          491      5.0      0.0          print(path)
```

The main cost here is the I/O of writing to the temporary files: it takes up over half of the execution time.
It's actually _so_ expensive that it would make the first approach attractive for cases where the column number is small.

So here we're dealing with an I/O bottleneck.
We're writing to a hundred or so files synchronously, and paying the price for it.
What if we could write to the files asynchronously?
One way to do that, if we were using Python 3.x, would be asynchronous I/O (we might get to that later).
In our world, we don't have that, but we have the next best thing: threads.

Threads effectively allow you to do more non-CPU-bound tasks in a shorter amount of wall time.
They are lightweight than multiprocesses, so you can create many more of them.
Because they do not bypass the limitations of the GIL, so they are best applied to cases like I/O bottlenecks.

The strategy is simple:

```python
def writer_thread(queue_in, fpath_out, open_):
    with open_(fpath_out, 'wb') as fout:
        lines = True
        while lines is not SENTINEL:
            lines = queue_in.get()
            if lines is not SENTINEL:
                fout.write(b'\n'.join(lines) + b'\n')
            queue_in.task_done()
```

We start a separate thread for each column, and that thread writes to its own file.
We batch lines together for efficiency: otherwise, the overhead of working with some many queues and items becomes too high.
Let's time it:

```
bash-3.2$ time python multisplit.py < sampledata.csv

real    0m27.865s
user    0m26.215s
sys     0m1.969s
```

An interesting result is that our thread-based solution chews through this file around 20s faster than the multiprocessing solution does its calculations.
This is despite the fact that the former does a **lot** of I/O.

## Putting It All Together

Let's combine our solutions, i.e. do:

- Histogram of row size (how many columns each row has)
- For each column:
  - Number and ratio of non-empty values
  - The maximum, minimum and mean lengths of the values
  - Number of unique values (this is the hard one)
  - ~~The top 20 most frequent values~~ (I'll leave this for later, this article is already long enough).

We can do the histogram as part of split - it's trivial.
For the rest, we need a script that reads a sorted column and outputs the results.

Now that our output is sorted, we can easily [run-length encode](https://en.wikipedia.org/wiki/Run-length_encoding) it.
This is good because the number of runs automatically gives us the number of unique values.
Furthermore, the number of runs is guaranteed to be less than or equal to the number of values, so working with runs is more efficient and convenient.
Once we have our runs and run lengths, calculating the above is trivial.
It's also relatively quick:

```
bash-3.2$ time for f in gitignore/col-*.txt; do LC_ALL=C sort $f | python summarize.py > /dev/null; done

real    1m7.055s
user    1m10.568s
sys     0m2.700s
```

It took 1min 7s seconds to sort and summarize all of our hundred or so columns.
Taking into account the 30 seconds it took to split the original file, we can expect to process the file in under 2 min.
Let's see if we can do better: start by profiling summarize.py:

```
Total time: 4.71134 s
File: summarize.py
Function: read_column at line 22

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    22                                           @profile
    23                                           def read_column(iterator):
    24         1            3      3.0      0.0      num_values = 0
    25         1            1      1.0      0.0      num_uniques = 0
    26         1            0      0.0      0.0      num_empty = 0
    27         1            1      1.0      0.0      max_len = 0
    28         1            0      0.0      0.0      min_len = sys.maxint
    29         1            1      1.0      0.0      sum_len = 0
    30
    31    218087      3644832     16.7     77.4      for run_value, run_length in run_length_encode(line.rstrip(b'\n') for line in iterator):
    32    218086       142615      0.7      3.0          if run_value == BLANK:
    33         1            1      1.0      0.0              num_empty = run_length
    34    218086       137396      0.6      2.9          num_values += run_length
    35    218086       134973      0.6      2.9          num_uniques += 1
    36    218086       143855      0.7      3.1          val_len = len(run_value)
    37    218086       178101      0.8      3.8          max_len = max(max_len, val_len)
    38    218086       170990      0.8      3.6          min_len = min(min_len, val_len)
    39    218086       158565      0.7      3.4          sum_len += val_len * run_length
    40
    41         1            1      1.0      0.0      return {
    42         1            0      0.0      0.0          'num_values': num_values,
    43         1            1      1.0      0.0          'num_fills': num_values - num_empty,
    44         1            1      1.0      0.0          'fill_ratio': (num_values - num_empty) / num_values,
    45         1            0      0.0      0.0          'max_len': max_len,
    46         1            1      1.0      0.0          'min_len': min_len,
        47         1            2      2.0      0.0          'avg_len': sum_len / num_values,
    48         1            1      1.0      0.0          'num_uniques': num_uniques,
    49                                               }
```

Most of the time gets spent in the run_length_encode function.
Drilling down:

```
Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
     9                                           @profile
    10                                           def run_length_encode(iterator):
    11         1      2449072 2449072.0     60.2      run_value, run_length = next(iterator), 1
    12    699182       667812      1.0     16.4      for value in iterator:
    13    699181       297065      0.4      7.3          if value < run_value:
    14                                                       raise ValueError('unsorted iterator')
    15    699181       280812      0.4      6.9          elif value != run_value:
    16    218085        82619      0.4      2.0              yield run_value, run_length
    17    218085        96326      0.4      2.4              run_value, run_length = value, 1
    18                                                   else:
    19    481096       192319      0.4      4.7              run_length += 1
    20         1            0      0.0      0.0      yield run_value, run_length
```

Not much we can do here.
Most of the time is spent reading the iterator (in our case, it's a file).
The first hit is particularly bad: it's likely because of buffering.
Fortunately, this is another I/O bound problem, and we can solve it using threads.
Before we do that, we have to tend to another problem: when to sort?

1. Sort all columns first, and then run them through summarize.py on multiple threads
2. Sort each column and pipe it to summarize.py directly

Let's look at the first option: sorting everything **first**.

```
bash-3.2$ time for f in gitignore/col*.txt; do sort $f > $f.sorted; done

real    9m17.598s
user    9m6.205s
sys     0m4.773s
```

gsort is slightly faster because it uses parallelization, unlike the default sort on MacOS, but still relatively slow:

```
bash-3.2$ time for f in gitignore/col*.txt; do gsort $f > $f.sorted; done

real    3m40.958s
user    10m33.544s
sys     0m8.451s
```

Why does sorting take so long?
Is it because we're writing to a file?  Piping to summarize was lightning-fast.

Let's try the second option with some pseudo-code:

```
split
for each column:
   sort column | summarize
collect summaries
clean up
```

There's a Python module for helping us out with the sort and pipe part.
It's appropriately called [pipes](https://docs.python.org/2/library/pipes.html).
We use it like this:

```python
def sort_and_summarize(path):
    template = pipes.Template()
    template.append('LC_ALL=C sort', '--')
    with template.open(path, 'r') as fin:
        result = summarize(fin)
    return result
```

The LC_ALL=C forces a sort by native byte values, as opposed to locale-specific characters.
This gives the same result as sorting within Python.

Since we've seen that sorting and summarizing is CPU-intensive, we can spread the work across multiple cores.
The result:

```
bash-3.2$ time python summarize.py gitignore/col-*.txt > /dev/null

real    0m36.105s
user    2m10.014s
sys     0m2.899s
```

Now all we need is a script that does the following:

```
split
for each column:
    sort column | summarize
collect summaries
clean up
```

Let's time our end result:

```
bash-3.2$ time python bigcsv.py < sampledata.csv > /dev/null

real    1m20.490s
user    3m5.546s
sys     0m5.448s
```

1 minute and 20 seconds.
It's good enough for me.

## Summary

- More than one way to parse CSV - the best method depends on your application
- CSV parsing is CPU bound, but
- Splitting CSV files is I/O bound
- [pympler](https://pythonhosted.org/Pympler/) is helpful for memory profiling
- [line_profiler](https://github.com/rkern/line_profiler) is helpful for CPU usage profiling
- [pipes](https://docs.python.org/2/library/pipes.html) module is helpful for using pipes within your Python programs
- Watch [this video](https://youtu.be/QJwVYlDzAXs) for a good intro to Python profiling
- Watch [this video](https://youtu.be/iG6fr81xHKA) for a good intro to Python 3's asyncio
