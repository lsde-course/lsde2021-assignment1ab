# Large Scale Data Engineering 2021 – Assignment 1

<https://event.cwi.nl/lsde/2021/practical1.shtml>

## Assignments 1a and 1b

To build and run the `cruncher` binary on SF100, use:

```bash
make cruncher
./cruncher /opt/lsde/dataset-sf100-bidirectional/ queries-test.csv out.csv
# in a separate terminal
tail -f out.csv
```

In 1a, your task is to optimize the code in `cruncher.c` so that it finishes within the timeout on the leaderboard machine.

In 1b, your task is to implement the reorganizer (`reorg.c`) code, adjust the code in `cruncher.c` accordingly, and potentially add further optimizations.

## Assignment 1c

See the `spark/` directory.
