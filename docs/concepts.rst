********
Concepts
********

The purpose of Apsis is to schedule various programs or tasks to run at specific
times, to run them at those times, and to track and report their progress.

Typically, the programs or tasks are recurring: the same program will run
periodically.  Various schedules may specify the recurrence, such as every 15
minutes, or at 10:00 New York time every Wednesday.

The individual instances of the recurring program are usually very similar.
The identical program may run, or it might be parameterized by the date, time,
or other information.


Jobs and Runs
-------------

Apsis models two main concepts:

- A **job** is a template for a program or task.  It specifies exactly how to
  run the program.  It also gives the recurring schedule on which the program
  should be run.  It may also carry other control information, such as
  dependencies, triggered actions, and metadata.

- A **run** is a *single instance* of a job, that is scheduled to run at a
  specific time.

You configure Apsis's jobs.  Apsis periodically creates runs from these jobs,
according to their schedules.  (You may also create a single *ad hoc* run
explicitly, if necessary.)

Apsis tracks scheduled runs, and starts each when the time comes.  Apsis then
tracks the progress of the run, and stores and reports status and output from
its execution.


Parameters and arguments
------------------------

A job may have **parameters** (or "params").  This allows you to vary its
behavior based on the time or date, or on other arbitrary values.  For example,
you might run a cleanup job on each of several hosts.  You can tell Apsis which
host to run for using a param, and in fact you can specify different schedules
for each.

When Apsis creates a run, it fills in the params with values as **arguments**
(or "args").  The run fills each of the job's params with an arg.

For example, the job `cleanup(date, host)` takes two parameters, "date" and
"host".  A particular run `cleanup(date=2020-11-06, host=server7.example.com)`
specifies values for each.

Params are unordered, so args may also be specified in any order.  Arg values
are always strings.

A job need not have params.  You may schedule a job with no params to run
periodically.  You may also run multiple runs with the same args.  However,
params make it easier to track programs that need to be run exactly once for
each param value, for example a job that needs to be run once a day.


Programs
--------

A **program** is what Apsis actually runs.  This is usually a program (*i.e.* a
shell command or process invocation), but you may extend Apsis to allow other
kinds of programs.


Conditions
----------

You may specify certain conditions that must hold before a run is allowed to
start.  This is in addition to the run's schedule time; Apsis will not start the
run until the scheduled time (unless you override it).

- A run may have a condition on the state of another run.  For instance, you can
  tell Apsis not to start a run until another run has completed successfully.

- A run may require an arbitrary external condition.  You can extend Apsis to
  allow various types of conditions.  Apsis will occasionally check the
  condition, where necessary, to determine whether the run can be started.


Actions
-------

FIXME


Run states
----------

Each run, once created, is in one of these states:

- **scheduled**: The run is waiting for its schedule time.
- **waiting**: The run is waiting for a condition to be met.
- **starting**: The run is starting.
- **running**: The run has started and is currently running.
- **stopping**: Apsis is stopping the run.
- **success**: The run has completed successfully.
- **failure**: The run has completed unsuccessfully.
- **error**: Some other problem has occurred with the run.  This can include a
  problem with Apsis itself; with the job configuration; or the runtime
  environment (for instance, a host is unresponsive).  The run may or may not
  have started or completed, depending on the nature of the error.

Each run includes a log of state transition times with additional details.


State Model
===========

Apsis transitions a run among these state as follows:

- New runs start in the **scheduled** state.  Apsis schedules new runs
  automatically according to job schedules, but you can create one explicitly as
  well.

- When the schedule time for **scheduled** run arrives, Apsis transitions it to
  **waiting**.

- Apsis periodically checks for the conditions of a **waiting** run.  When all
  of them have been satisfied, it starts the run's program and transitions it
  to **starting**.  If a run has no conditions, this happens immediately.

- Before a run has started, it may be manually or automatically **skipped**

- If Apsis is unable to start the program, it transitions the run to **error**.

- If Apsis is able to start the program, it transitions the run to **running**.

- When a **running** run finishes, Apsis transitions it to **success** or
  **failure**, depending on whether the run's program was successful.

- If Apsis encounters an internal error while handling a run in any state, it
  transitions it to **error**.

You can apply the following operations, to induce transitions explicitly:

- You can *start* a **scheduled** run.  Apsis no longer waits for its schedule
  time, and transitions it immediately to **waiting**.  If it has conditions
  that are not yet fulfilled, it will not start immediately.

- You can *start* a **waiting** run.  Apsis no longer checks its conditions,
  starts the run's program, and transitions the run to **running**.

- You can *skip* a **scheduled** or **waiting** run.  Apsis no longer waits for
  its schedule time or conditions, and transitions it to **skipped**.

- You can *stop* a **running** run.  Apsis requests that the run shut down in an
  orderly manner.  How this works depends on the run's program.  For a program
  that runs a (local or remote) UNIX process, this entails sending a termination
  signal (usually SIGTERM), then waiting for a grace period and then sending
  SIGKILL if the process has not terminated.  While Apsis is waiting for the run
  to terminate, it is in the **stopping** state.

  You can also schedule Apsis to stop a run automatically; see
  :ref:`stop-schedules`.

- You can *mark* a finished run (**success**, **failure**, **skipped**, or
  **error**) to a different finished state.

