.. _monitoring:

Monitoring Apsis
================

Statistics
----------

Apsis produces statistics describing its internal functioning, with the
`apsis.program.internal.stats.StatsProgram` program type.  You can create a
normal job using this program, and schedule it to run at suitable intervals.

This program produces a JSON-encoded object with internal stats as its output.
If you provide the `path` program arg, it also appends the JSON object on a new
line in this file; the file is created, if necessary, but the parent directory
must exist.

This job configuration produces stats once a minute and appends them to a file,
one for each UTC calendar date.

.. code:: yaml

    params: [date]

    schedule:
      type: interval
      interval: 60

    program:
      type: apsis.program.internal.stats.StatsProgram
      path: "/opt/apsis/stats/{{ date }}.json"


The statistics object contains these fields:

- `start_time`: When the Apsis process started.
- `time`: When the statistics were produced.
- `async.latency`: A recent estimate of the Python event loop latency.  Apsis
  runs an internal task every 10 sec and compares the time the task wakes up to
  its scheduled time, to estimate the latency.
- `rusage_maxrss`: High-watermark RSS obtained from `getrusage`.
- `rusage_utime` and `rusage_stime`: Process user CPU time and system CPU time
  obtained from `getrusage`.
- `scheduled.num_entries`: The number of scheduled runs.
- `scheduled.num_heap`: The size of the scheduled run heap.  This may be larger
  than the number of scheduled runs, because runs are not removed from the heap
  if cancelled.
- `tasks.num_waiting`: The number of async tasks for waiting runs.  Apsis uses
  one task per waiting run.
- `tasks.num_running`: The number of async tasks for running runs.  Apsis uses
  one task per running run.
- `tasks.num_action`: The number of async tasks for executing actions.  Apsis
  uses one task per action; a run may trigger multiple actions.
- `len_runlogdb_cache`: The number of cached run log entries.
- `run_store.num_runs`: The number of runs stored in memory.
- `run_store.num_queues`: The number of queues maintained to serve live run
  updates, including live web UI updates.
- `run_store.len_queues`: The total size across queues maintained to serve live
  run updates.

Note that the schema of a stats object is not part of Apsis's API, and may
change in future versions.  A stats object may omit certain fields, especially
during startup and shutdown.  If you consume these stats, your logic should be
robust to added, moved, and renamed fields.

