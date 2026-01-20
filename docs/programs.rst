.. _programs:

Programs
========

Apsis provides a number of ways to specify a job's program.

A job's program is configured with the top-level `program` key.  The subkey
`type` indicates the program type; remaining keys are specific to the
configuration of each program type.

Apsis provides these program types, and you can extend Apsis with your own.

- `no-op`: Does nothing.
- `procstar`: Invokes an executable directly via a Procstar agent.
- `procstar-shell`: Executes a shell command via a Procstar agent.

Additionally, Apsis includes several internal program types, which deal with its
own internal housekeeping.


No-op programs
--------------

A `no-op` program runs instantly and always succeeds.

.. code:: yaml

    program:
        type: no-op


Procstar Programs
-----------------

`Procstar <https://github.com/apsis-scheduler/procstar>`_ is a system for
managing running processes.  Apsis can run programs via Procstar agents,
possibly on other hosts.  For Apsis to do this, at least one Procstar agent with
the matching group ID must connect to the Apsis server.

Apsis runs the program on one of the Procstar agents with the specified group ID
that is connected.  If no such agent is connected, Apsis waits for such an agent
to connect; the run is meanwhile in the *starting* state.


Shell commands
^^^^^^^^^^^^^^

The `procstar-shell` program executes a shell command, given in the `command` key.

.. code:: yaml

    program:
        type: procstar-shell
        command: /usr/bin/echo 'Hello, world!'

Note the following:

- The command is a string.  YAML provides multiple syntaxes for specifying
  strings, each with different rules for line breaks, whitespace, and special
  characters.  See, for example,
  `yaml-multiline.info <https://yaml-multiline.info/>`_.

- The command actually a short program, executed by a new shell instance.
  The shell is
  `bash <https://www.gnu.org/software/bash/manual/bash.html>`_.  All shell
  quoting, escaping, variable expansion, etc. rules apply.

- The command may be multiline, execute multiple programs, use shell flow
  control constructs, etc.

- Binding happens after the YAML is parsed by Apsis (when job config is loaded),
  but before the shell interprets the command (when a run starts).


Argv invocations
^^^^^^^^^^^^^^^^

The `procstar` program invokes an executable directly, without starting a shell.
Instead of a shell command, give `argv`, a list of strings containing the
arguments.  The first argument is the executable.

.. code:: yaml

    program:
        type: procstar
        argv:
        - /usr/bin/echo
        - Hello, world!

Since no shell evaluation takes place, there is no quoting, escaping, argument
splitting, or substitution.  Note Apsis still performs :doc:`binding <./jobs>` on the `argv`
strings, as described above.


Running as another user
^^^^^^^^^^^^^^^^^^^^^^^

The program process runs as whichever user runs the Procstar agent.  To run
as another user, specify `sudo_user` in the program.  Procstar will attempt to
run the program under `sudo` as that user.  The host on which the agent is
running must be configured with an appropriate sudoers configuration that allows
the user running the Procstar agent to run the command as the sudo user, without
any explicit password.

.. code:: yaml

    program:
        type: procstar-shell
        group_id: default
        sudo_user: appuser
        command: "whoami"


Timeouts
^^^^^^^^

You can specify a timeout duration for a Procstar program.  If the timeout
elapses before the program completes, Apsis sends the program a signal.

.. code:: yaml

    program:
        type: procstar-shell
        command: /usr/bin/takes-too-long
        timeout:
            duration: 300
            signal: SIGTERM

In this example, Apsis sends SIGTERM to the program after five minutes, if it
hasn't completed yet.  The `signal` key is optional and defaults to SIGTERM.


.. _program-stop:

Program Stop
------------

Many program types provide a stop method, by which Apsis can request an orderly
shutdown of the program before it terminates on its own.  Keep in mind,

- Not all program types provide a program stop.
- The program may not stop immediately.
- The program stop may fail.

Apsis requests a program to stop if the program's run is configured with a stop
schedule, or in response to an explicit stop operation invoked by the user.

Before Apsis requests a program to stop, it transitions the run to the
*stopping* state.  If the program terminates correctly (zero exit code) in response 
to the stop request, Apsis transitions the run to *success*; if the program terminates 
in an unexpected way (non-zero exit code), *failure*.

The program types above that create a UNIX process (
`procstar`, `procstar-shell`) all implement program stop similarly.  In response
to a program stop request,

1. Apsis immediately sends the process a signal, by default `SIGTERM`.
2. Apsis waits for the process to terminate, up to a configured grace period, by
   default 60 seconds.
3. If the process has not terminated, Apsis sends it `SIGKILL`.

To configure the program stop, use the `stop` key.  For example, Apsis will
request this program to stop by sending `SIGUSR2` instead of `SIGTERM`, and will
only wait 15 seconds before sending `SIGKILL`.

.. code:: yaml

    program:
        type: procstar
        group_id: default
        argv: ["/usr/bin/echo", "Hello, world!"]
        stop:
            signal: SIGUSR2
            grace_period: 15s

Only if the process terminates with zero exit code the run will be considered as 
successfull.


Internal Programs
-----------------

An *internal program* is a special program that operates on Apsis itself.  These
internal program types are available:


Stats
^^^^^

A `apsis.program.internal.stats` program generates internal statistics about
Apsis's state and resource use, in JSON format.  Stats are generated as program
output.  If you specify the `path` key, the JSON stats are also appended, with a
newline, to the specified file.

This job produces a run once a minute, which appends the stats to a dated file:

.. code:: yaml

    params: [date]

    schedule:
        type: interval
        interval: 60

    program:
        type: apsis.program.internal.stats.StatsProgram
        path: "/path/to/apsis/stats/{{ date }}.json"



Archive
^^^^^^^

An `ArchiveProgram` program moves data pertaining to older runs out of the Apsis
database file, into a separate archive file.  Keeping the main Apsis database
file from growing too large can avoid performance degredation.

The archive program retires a run from Apsis's memory before archiving it.  The
run is no longer visible through any UI.  A run that is not completed cannot be
archived.

This job archives up to 10,000 runs older than 14 days (1,209,600 seconds), in
chunks of 1,000 runs at a time, with a 10 second pause between chunks:

.. code:: yaml

    schedule:
        type: daily
        tz: UTC
        time: 01:30:00

    program:
        type: apsis.program.internal.archive.ArchiveProgram
        age: 1209600
        count: 10000
        chunk_size: 1000
        chunk_sleep: 10
        path: '/path/to/apsis/archive.db'

The archive program blocks Apsis from performing other tasks for each chunk of
archive runs.  Adjust the `chunk_size`, `chunk_sleep`, and `count` parameters so
that the archiving process pauses every few seconds, to avoid long delays in
starting scheduled runs.  If the `chunk_size` parameter is omitted, all runs are
archived in one chunk.  If the `chunk_sleep` parameter is omitted, Apsis does
not pause between chunks.

The archive file is also an SQLite3 database file, and contains the subset of
columns from the main database file that contains run data.  The archive file
cannot be used directly by Apsis, but may be useful for historical analysis and
forensics.


Vacuum
^^^^^^

A `VacuumProgram` run vacuums (defragments and frees unused pages from) the
Apsis database file.  The program blocks Apsis while it is running; schedule it
to run only during times the scheduler is otherwise quiet.


