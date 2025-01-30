# v0.31

- `ArchiveProgram` no longer vacuums the database file.  Use
  `apsis.program.internal.vacuum.VacuumProgram` to schedule this separately.
- Configs `python.gc.enabled` and `python.gc.threshold` configure the Python
  garbage collector.


# v0.30

- Config `database.timeout` sets the sqlite lock timeout.
- The run archive program takes `chunk_size` and `chunk_sleep` parameters, to
  divide the archive operation into chunks with pauses in between.


# v0.29

- `apsisctl check-jobs --check-dependencies-scheduled` attempts to check that
  dependencies of scheduled runs are themselves scheduled.


# v0.28

- Apsis checks at startup that the job dir exists and all YAML files within it
  are valid jobs.  It fails to start if there are any job errors.
- Apsis, when checking jobs, instantiates runs from jobs according to schedules,
  to test that expansions function correctly.
- In a Procstar program with `sudo_user` set, Apsis runs the command under
  `sudo` for the specified username.

This release requires changes to the schema of Apsis database files.  To migrate
an existing database,

0. Stop Apsis.
1. Create a backup of the database.
2. `python scripts/migrate-db.py path/to/apsis.db`
3. Start the new version.


# v0.27

- Apsis checks all jobs in the job directory before starting, and does not start
  if it encounters any errors.


# v0.26

- The Control page in the web UI and the live log are removed.
- The run view contains a pane showing direct dependencies and dependents of the run.


# v0.24

- Changes to jobs update live in the web UI.
- Changes to Procstar agent connections update live in the web UI.
- The run log updates live in the web UI.
- Output data updates live in the web UI.
- `apsis output --follow` and `apsis output --tail` for live output in CLUI.
- `apsis watch` for live run changes in CLUI.
- Apsis sets the `APSIS_RUN_ID` env var in procstar programs.


# v0.23

- Requires [Procstar](https://github.com/alexhsamuel/procstar) v0.3.
- The Procstar agent protocol has changed substantially.  Now uses msgpack.
- The `procstar.agent.connection.start_timeout` config controls how long Apsis
  waits for a suitable agent connection to start a run.
- The `procstar.agent.connection.reconnect_timeout` config controls how long
  Apsis waits for an agent to reconnect, before dropping it.
- An agent now unregisters itself cleanly from Apsis during normal shutdown.
- SIGUSR1 triggers a shutdown-on-idle state in an agent; see [`shutdown` in the
  Procstar docs](https://github.com/alexhsamuel/procstar/blob/main/docs/shutdown.rst).
- The "Agents" view in the web UI shows the state of each conencted agent.


# v0.22

- [Procstar](https://github.com/alexhsamuel/procstar) agent-based programs.
- The child-class API of `apsis.program.base.Program` has changed.  Implement
  `run()` and `connect()` in subclasses, rather than deprecated `start()` and
  `reconnect()`.  Default implementations of the new methods provide
  compatibility with the old methods, but these will be removed in the future.


# v0.21

- New `exists` option to a
  [dependency condition](https://apsis-scheduler.readthedocs.io/en/latest/jobs.html#dependencies),
  to detect if the dependency doesn't exist at all.  #316
- Keyword search in the web UI is case-insensitive.  #312
- Collapse state of jobs in the job view is persistent.  #314


# v0.20

- Actions are now passed a run snapshot, rather than the `Run` object which may
  subsequently change.
- Add `apsis.action.ThreadAction`, a base class for an action that runs
  synchronously in a separate thread.
- Add `apsis.cond.ThreadPolledCondition`, a base class for a condition whose
  check runs synchronously in a separate thread.
- Add `/api/v1/live` and `/api/v1/stats` endpoints, to monitor basic liveness
  and stats, respectively.
- Add estimate of async event loop latency to stats.


# v0.18

- Add support for async conditions.  Custom conditions need to be updated
  accordingly; see [#278](https://github.com/alexhsamuel/apsis/pull/278).
- Polling of agent programs is now async.  This allows more concurrently running
  programs without degraded performance.
- Update build config to `pyproject.toml`.


# v0.17.0

- The config `runs_lookback` is now `runs.lookback`.  When configured, Apsis
  retires older runs from memory runs while running.
- The internal program `ArchiveProgram` moves old runs out of the Apsis database
  into a separate archive database file.  This also retires the old runs from
  memory.  This program replaces the old `apsisctl migrate` manual process.
- The internal program `StatsProgram` collects and writes Apsis usage stats.
  Apsis no longer logs stats to its own log.
- The Python builtin `format` and the Ora functions `from_local` and `to_local`
  are available in jinja2 expressions when binding jobs.
- The `shell` and `program` program types now accept a `timeout`.
- Durations in the Apsis config file and in job definitions now support values
  with units such as `10 min`, `8h`, and `1 day`.


# v0.16.0

- The web UI has been substantially improved.  The new design permits navigation
  of a much larger number of runs, without displaying them all at one time.
- Filters for runs and views are more flexible and explicit.  Click help icons
  for explanations.
- The URL scheme for runs and views filters has changed substantially.  Any
  bookmarked or otherwise saved URLs to these views won't work anymore.
- `apsisctl check-jobs` checks conditions (including dependencies) and actions
  for missing and extraneous args.  #245


# v0.15.0

- Add a "Schedule Run" frame to the web UI job view, for manual scheduling.
- Apsis now compresses program outputs with Brotli before storing to the
  database.  The HTTP service also returns Brotli-compressed output, if the user
  agent supports this encoding.


# v0.14.0

- Add the _skipped_ state and _skip_ operation.  Remove the _cancel_ operation;
  superceded by _skip_.
- Add `run` param to the signature for `Condition.check_runs()`.  Custom
  condition classes need to be updated.
- Add support for inducing a transition from `Condition.check_runs()` by
  returning a `Condition.Transition` value.
- Add the `skip_dependency` condition type.
- Group scheduler config under `schedule` key.  Add `schedule.horizion` config.
  Rename `schedule_max_age` to `schedule.max_age`.


# v0.13.1

- Remove UIkit from the web UI, for performance.  Minor visual and behavioral changes.
- Various other performance fixes to web UI and supporting HTTP API.
- Fix #181, incorrect job fuzzy match in web UI.


# v0.13.0

- Add "no-op" program type.
- Add `waiting.max_time` config to impose maximum waiting time on all runs.
- Log basic stats as a JSON document in a minutely INFO log line.


