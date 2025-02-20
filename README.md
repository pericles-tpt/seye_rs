# seye_rs
A Rust rewrite of my unfinished [seye](https://github.com/pericles-tpt/seye) project which "allows you to scan one or more directories to identify current characteristics (largest size, number of files, duplicates, etc) as well as the change in disk usage over time."

## Goals
This is my first rust project, so some of these goals are focused on allowing me to test out approaches, ideas and a new language. However, as I further develop the prototype I plan to shift the focus from self education to improving the tool:
- ~~Learn how to program effectively in Rust and understand the borrow checker~~
- ~~Learn how to do **iterative** tree traversal~~
- ~~Benchmark performance characteristics of iterative vs recursive tree traversal techniques~~ (skipped)
- Implement memory limits for directory scans (down to a reasonable limit, probably 100M)
- ~~Implement multithreading for directory scans~~
- Implement multithreading for diff combining
  - Example - 4 diffs, 1 additional threads:
    - MainThread -> combine(diff0, diff1) = diff01
    - Thread1    -> combine(diff2, diff3) = diff23
    - MainThread -> combine(diff01, diff23) = diffAll
- ~~Learn about performance optimisation techniques~~

## Progress
The following functionality is currently working:

- Scan: Scan a directory and store the binary output of the scan in an output directory, subsequent scans will just store the "diff".
- Report: Generates a basic report of which directories were: added, removed or modified. Reports look like this:
```
running as ROOT user
MOD: "/home/pt/Downloads/OnbOxDty_export(2)" (+2G)
ADD: "/home/pt/Downloads/blah2" (+46M)
MOD: "/home/pt/Downloads" (-70M)
REM: "/home/pt/Downloads/Geekbench-6.2.2-Linux" (-476M)
Total change is: +2G
```
- Find: Find all files containing a substring in their name, it's currently configured to match `fd` as closely as possible (with more configuration options to come)

### How it works
1. Each time you run `scan` the program will do an iterative traversal of the target directory, gathering paths, size, modified dates, etc for each directory and file. It pushes those results onto a `Vector<CDirEntry>` which is returned in path-sorted order.
2. The behaviour then branches:
    - IF it's the initial scan, then that result is saved to a file.
    - Otherwise it'll read any existing diffs, combine them, add them to the INITIAL scan and finally compare the "initial scan + diff" to the current scan, this produces a new diff which is saved to a file.

PROS:
- Saves disk space by storing just the diffs (scans of directories containing 1M+ files and 100K+ directories can take 100MB+ of space).

CONS:
- Slower than storing the entire scan each time, as the previous diff needs to be generated (before comparing to the current diff) by combining all previous diffs into a combined diff and then adding that combined diff to the initial scan.

#### Multithreading
You can run the scans across multiple threads by settings the `-t` parameter >= 2. All messaging between the main and additional threads is done with channels. When multithreading the responsibilities of the threads are:
- Main Thread: Redistributes incoming paths from each thread, back to all the threads *roughly* evenly. Sends an EXIT command to all threads when there's no paths left.
- Other Threads: Receive incoming paths, then walks directories iteratively up to a limit (specified with `-tdl` flag). Each directory's information is stored in a `CDirEntry` and the information for each file is also stored in the `CDirEntry`. All `CDirEntry` are stored in a vector managed by the thread which is returned on termination. Once the thread reaches its traversal limit it returns any remaining (i.e. not traversed) paths back to the main thread to redistribute.

### Current Performance (avg of 3 runs)
#### Scan (ran on Linux, on a copy of my home directory on a PCIE gen4 SSD)
Performance results of scans without multithreading:
```
Scanned 1367818 files, 231186 directories in: 4059ms
```
The same test conditions as above with threads=364 and thread_directory_limit=320:
```
Scanned 1367818 files, 231186 directories in: 1538ms
```
The size of the initial scan is 169.7MB, a subsequent diff with one item added is 482B

#### Find
The `find` command is still a WIP and doesn't have as many options for configuration as the existing [`fd`](https://github.com/sharkdp/fd) tool (also written in Rust), it's currently configured to match the behaviour of `fd`'s defaults as closely as possible. Despite that, the `find` command returns 6 more results than `fd`, so I need to refine it a bit more. The benchmark scores (generated with the `hyperfine` benchmarking tool) for `find` vs `fd` are, find:
```
Benchmark 1: sudo ./target/release/seye_rs find -t 168 -tdl 1024 Document /run/media/pt/gen4_test/pt > b.txt
  Time (mean ± σ):      90.3 ms ±   4.1 ms    [User: 4.6 ms, System: 4.9 ms]
  Range (min … max):    82.9 ms … 109.6 ms    100 runs
```
fd:
```
Benchmark 1: sudo fd Document /run/media/pt/gen4_test/pt > a.txt
  Time (mean ± σ):     115.4 ms ±   4.2 ms    [User: 4.6 ms, System: 4.8 ms]
  Range (min … max):   105.8 ms … 134.0 ms    100 runs
```

The average of 3 runs of each command is:
| Program  | Time    |
| -------- | ------- |
| fd       | 0.1137s |
| find     | 0.0947s |

## Usage
Run the following command to see valid command and usage:
```
seye_rs --help
```

### Planned Features
- Ability to specify a time range to compare scan diffs (e.g. 2 weeks ago until now)
- Identify file renames (as another diff type like Modify, Remove and Add)
- Add an option to compare files by hash instead of size difference, allowing changes to be identified even if the size and modified time haven't changed
- Add an option to specify a memory usage limit (lower bound likely to be 100M, upper bound undecided)
- ~~Add an option to specify a number of threads to run the scan on~~
  - Currently planning to do READDIR syscalls on the main thread and delegate STAT calls to auxiliary threads  
