Jobs
====

Jobs are configured by files in the jobs directory.  The jobs directory may
contain subdirectories, to organize jobs.  Each job file uses the `.yaml` file
suffix, and specifies one job, with a unique job ID, in YAML format.

The Apsis config file specifies the location of the jobs directory; see
[[config]].

A job config contains these top-level keys:

- `params` (optional)
- `program`, which specifies what to run 
- `schedule`, which specifies when to schedule runs
- `metadata` (optional), additional information not interpreted by Apsis
- `conditions` (optional) that must be met for a run
- `actions` (optional) to take when a run changes state

For durations in seconds, you may also use durations like `30s`, `10 min` (600
seconds), `1.5h` (5400 seconds), and `1 day` (86400 seconds).


Job ID
------

The job's ID is given by the path under the jobs directory, with the `.yaml`
suffix removed.  For example, if the jobs directory is `/path/to/jobs`, the job
file `/path/to/jobs/data/pipeline/start.yaml` has the job ID
`data/pipeline/start`.


Params
------

The `params` key in the job config takes a list of parameter names.  For
example,

.. code:: yaml

    params:
      - date
      - message

or equivalently,

.. code:: yaml

    params: ["date", "message"]

If `params` is omitted, the job has no parameters.

Parameters aren't required; a job without parameters can be run repeatedly, just
like a cron job.


Program
-------

The `program` key program describes how a run executes.  Apsis provides several
types of programs, and you may extend Apsis with additional program types as
well.

See :ref:`programs` for more information.


Schedule
--------

The `schedule` key specifies when new runs are created and for when they are
scheduled.

A job may have a single schedule, given as a dict, or multiple schedules, as a
list of dicts.

.. code:: yaml

    # Single schedule
    schedule:
        type: interval
        interval: 3600

    # Two schedules
    schedule:
      - type: interval
        interval: 3600
      - type: daily
        tz: UTC
        daytime: 12:00:00

See :ref:`schedules` for more information.


Metadata
--------

A job can store arbitrary metadata, such as descriptive text, tags, and operator
instructions.  The `metadata` key accepts arbitrary subkeys.  None affect how
runs of the job are executed.

Apsis does understand certain metadata keys.  The `description` key contains
descriptive Markdown text shown in the UI.

.. code:: yaml

    metadata:
        description: |
            Daily cleanup job.

            Removes temporary files that have been created within the last 24
            hours.

The `labels` key is an array of string labels, also shown in the UI.

.. code:: yaml

    metadata:
        labels:
            - test
            - blue-team

Any other metadata keys are preserved but ignored by Apsis.


Conditions
----------

The `condition` key describes zero or more conditions that must be satisfied
before Apsis starts the run's program.  Apsis provides several types of
conditions, and you may extend Apsis with additional condition types as well.

See :ref:`conditions` for more information.


Actions
-------

FIXME: Write this.


.. _binding:

Binding
-------

Apsis creates specific runs for a job, according to the job's schedule.  When
Apsis creates a run, it **binds** the run's arguments in the program,
conditions, and actions.  Each string-valued config field is expanded as a
`jinja2 template <https://jinja.palletsprojects.com/en/2.11.x/templates/>`_.
The run's args are available as substitution variables.

For example, consider this job config:

.. code:: yaml

    params:
    - color
    - fruit

    program:
        type: procstar-shell
        command: "echo The color of {{ fruit }} is {{ color }}."

When Apsis creates a run with `color: red` and `fruit: apple`, it expands the
program to,

.. code:: yaml

    program:
        type: procstar-shell
        command: "echo The color of apple is red."

The contents of a `{{ ... }}` expansion is evaluated as a `jinja2 expression
<https://jinja.palletsprojects.com/en/3.1.x/templates/#expressions>`_.  The
following additional Ora types and functions are available:

- `Date <https://ora.readthedocs.io/en/latest/dates.html#dates>`_
- `Daytime`
- `Time <https://ora.readthedocs.io/en/latest/times.html#times>`_
- `TimeZone <https://ora.readthedocs.io/en/latest/time-zones.html#time-zone-objects>`_
- `get_calendar <https://ora.readthedocs.io/en/latest/calendars.html#finding-calendars>`_
- `from_local <https://ora.readthedocs.io/en/latest/localization.html#local-to-time>`_
- `to_local <https://ora.readthedocs.io/en/latest/localization.html#time-to-local>`_

These functions and types allow you to perform time computations on program and
condition dates and times.  For example, this job has a dependency on another
job *load data*.  Each run of this job is labeled with a date, and depends on a
*load data* run with the previous date, according to the *workdays* calendar.

.. code:: yaml

    params: [region, date]

    ...

    condition:
        type: dependency
        job_id: load data
        args:
            date: {{ get_calendar('workdays').before(date) }}

Keep in mind that Apsis run arguments are always strings, so Apsis converts the
result using `str`.


Checking jobs
=============

Invoke `apsisctl check-jobs JOBDIR` to load and check the jobs in a job
directory.  This runs the same checks that Apsis uses when starting.

With `--check-dependencies-scheduled`, this also attemps to check that the
dependencies of each scheduled job are also scheduled.  This check is heuristic,
as it checks dependencies only of runs scheduled in the next 24 hours, and
considers only dependency runs scheduled in the last 24 hours and the next 24
hours.


Changes to jobs
===============

Once Apsis creates a run from a job and binds the run's args to the job's
program, conditions, and actions, these items are fixed.  Any later changes to a
job will not affect existing runs that were previously created from that job.

However, whenever a job is changed, or when the Apsis server restarts, Apsis
removes scheduled runs and recreates them from jobs' schedules.  (Any runs
manually scheduled for the job through the UI are exempt from this; only runs
that Apsis created from a job's schedule are recreated.)  This means that
changes to a job's schedules immediately affect future scheduled runs.  When
Apsis recreates a run, it uses the current definition of its job, and picks up
any changes to programs, conditions, and actions.

As a result, these rules generally describe a run's relationship to its job:

1. Scheduled runs in the future reflect a job's current schedules.  Runs
   scheduled in the past are of course not affected by subsequent changes to the
   job's schedules.

2. A run's program, conditions, and actions effectively reflect the state of its
   job at the time the run transitioned from *scheduled* to *waiting*, which
   generally at the time the run's scheduled time occurred.

3. If you rerun a run, the rerun always reflects its job's current program,
   conditions, and actions.

If a run is *waiting* (or in a subsequent state) and you make changes to its
program, conditions, or actions, skip or kill the run if necessary, and rerun it
to pick up the changes.

