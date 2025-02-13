# seye_rs
A Rust rewrite of seye which "allows you to scan one or more directories to identify current characteristics (largest size, number of files, duplicates, etc) as well as the change in disk usage over time."

## Goals
This is my first rust project, so some of these goals are focused on allowing me to test out approaches, ideas and a new language. However, as I further develop the prototype I plan to shift the focus from self education to improving the tool:
- Learn how to program effectively in Rust and understand the borrow checker
- Learn how to do **iterative** tree traversal
- Benchmark performance characteristics of iterative vs recursive tree traversal techniques
- Implement memory limits for directory scans (down to a reasonable limit, probably 100M)
- Implement multithreading for directory scans
- Implement multithreading for diff combining
  - Example - 4 diffs, 1 additional threads:
    - MainThread -> combine(diff0, diff1) = diff01
    - Thread1    -> combine(diff2, diff3) = diff23
    - MainThread -> combine(diff01, diff23) = diffAll
- Learn about performance optimisation techniques

## Progress
The following functionality is currently working:

- Scan: You can currently scan a directory and store the binary output of the scan in an output directory
- Report: You can generate a basic report of which directories were: added, removed or modified. Reports look like this:
```
running as ROOT user
MOD: "/home/pt/Downloads/OnbOxDty_export(2)" (+2G)
ADD: "/home/pt/Downloads/blah2" (+46M)
MOD: "/home/pt/Downloads" (-70M)
REM: "/home/pt/Downloads/Geekbench-6.2.2-Linux" (-476M)
Total change is: +2G
```

### How it works
1. Each time `scan` is run the program will do an iterative traversal of the target directory, gathering paths, size, modified dates, etc for each directory and file. It pushes those results onto a `Vector<CDirEntry>` which is returned in path-sorted order.
2. The behaviour then branches:
    - IF it's the initial scan, then that result is saved to a file.
    - Otherwise it'll read any existing diffs, combine them, add them to the INITIAL scan and finally compare the "initial scan + diff" with the current scan, to produce a new diff which is saved to a file.

PROS:
- Saves disk space by storing just the diffs (full scans take 100MB+ for my home directory)

CONS:
- Worse performance than storing the entire scans each time, as diffs need to be combined and then added to the initial scan before comparing the initial and current scans

### Current Performance
Performance results of an initial `sudo scan` on my home directory (NOTE: This occurred after some previous scans were run, so some syscall results are likely cached):
```
Scanned 1366164 files, 230408 directories in: 3736ms
```
Running the scan a second time with some files added to 3 sibling directories (so that it generates a non-zero diff) produces this result:
```
Scanned 1366169 files, 230408 directories in: 4450ms
```
The size of the initial scan is 165.2MB and the subsequent scan is 1.5KB

Multithreading hasn't been implemented yet so performance will likely improve once I've added that
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
- Add an option to specify a number of threads to run the scan on
  - Currently planning to do READDIR syscalls on the main thread and delegate STAT calls to auxiliary threads  
