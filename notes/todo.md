- if `send_signal` raises, error the run
- improve `apsis job` output style


### Database schema

- Remove `message` from run JSON and `runs` table.

- Move metadata out of `runs` into its own table, along with any other column
  that isn't part of the run summary.

- Clean up `rerun` column as well.

- Rename `run_history` to `run_log`.

### Other

- To finish `check-job`, we need to remove the program, schedule, action, cond
  aliases from `config.yaml` and move them to a config file specific to the job
  dir.

- Factor loop watchers (`log.critical`) into a common pattern.

- Clean up the restore task.


# Idle thoughts

Change scheduler, scheduled, wait loops into async iters that return
instructions to Apsis, who then applies them, rather than applying them directly.


# Redis

- refactor on master:
  - split out run ID allocator
  - configure Redis instance in config.yaml
  - add a `RedisRunStore` and populate it from `RunStore.add` / `.remove`
  - add async `query()` and `subscribe()` methods


# Docs

- schedule config
- web UI


# Current

- in ora, truncate 0s from `Time.__str__` and `Daytime.__str__`
- support configured extension symbols in `template_expand`
- favicon: animated!
- better action UI
  - proper button
  - confirmation dialog
  - no redirect
- add version to web UI
- label for ad-hoc jobs
- attach job to run when scheduling, rather than loading jobs later
- job label search
- schedule time jitter
- better env vars in running programs, ala `APSIS_RUN_ID`.
- send debug log to file, from config
- handle more runs
  - query db for older runs instead of holding them in memory (not possible?)
  - in job view, include time limitation
- check out: FastAPI, uvicorn, Starlett
- clean up command line UI
  - implement `apsis runs --times`
  - nicer run list
  - nice job display
- convert `run.times` to log, with schema (timestamp, message, state)
  - retire run/transition message
  - retire times in run?  or hide from UI
- configuration
  - provide a way to add functions to jinja2 eval namespace
- refactor runs websocket into event stream
  - contains:
    - run updates
    - output updates
    - job refresh
    - other things?
  - watch runs live in CLUI
  - connection indicator in web UI
- normalize environment for another user, locally and by ssh
- SSL for Apsis
- factor out Vue API client code into module
  - error handling
- integrate [ansi-to-html](https://www.npmjs.com/package/ansi-to-html) for run output
- a good set of demo jobs
- change --host, --port to --bind
- job YAML template
- store schedule horizon per (job, schedule) ???
  - limit number of runs scheduled per job
- support multiple outputs per run in API/UI
  - add option to split stdout/stderr
- sanity checker
  - check for unscheduled runs
  - check for late scheduled runs
  - check for orphaned running runs
  - check for runs that ran late
- web UI for ad hoc runs
- document API
- factor args out of schedule classes
- indicate in web UI when a job started late


# cron

- format time with tz
- control precision in time, daytime formats
- parse weekdays (see repo.py)
- load named calendars from files


# Prior art

- check out: https://beepb00p.xyz/scheduler.html (and comment?)
- check out: https://github.com/jhuckaby/Cronicle
- check out: https://github.com/huginn/huginn
- check out: https://apscheduler.readthedocs.io/en/stable/

