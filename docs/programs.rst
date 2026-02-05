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
- `procstar-ecs`: Runs a program on AWS ECS via a Procstar agent.

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


Procstar ECS Programs
---------------------

Apsis can run programs on AWS ECS (Elastic Container Service). Unlike standard
Procstar programs that connect to long-running, pre-existing agents, ECS programs
launch a dedicated ECS task for each run. The task runs a container with an
ephemeral Procstar agent that connects to Apsis, executes the program, and exits
when complete, terminating the task. This provides complete isolation between runs.

The ``procstar-ecs`` program type accepts either a shell command or an argument
vector:

.. code:: yaml

    # Shell command form
    program:
        type: procstar-ecs
        command: "echo 'Hello from ECS!' && date"
        mem_gb: 2
        vcpu: 1

.. code:: yaml

    # Argv form (direct execution without shell)
    program:
        type: procstar-ecs
        argv: ["/usr/bin/python", "script.py", "--verbose"]
        mem_gb: 4
        vcpu: 2

This launches an ECS task, waits for the Procstar agent inside to connect, then
executes the program. When the program completes, the ECS task automatically
terminates.

Exactly one of ``command`` or ``argv`` must be specified.


Resource Configuration
^^^^^^^^^^^^^^^^^^^^^^

ECS programs support the following resource parameters:

- ``mem_gb``: Memory allocation in GB for the container (e.g., 2 for 2 GB)
- ``vcpu``: vCPU allocation for the container (e.g., 1 for 1 vCPU)
- ``disk_gb``: Ephemeral EBS storage in GB

All three parameters have global defaults configured in the Apsis configuration.

.. note::

   Values are specified in GB (gigabyte, SI decimal, 1000³ bytes) and converted
   to GiB (gibibyte, binary, 1024³ bytes) for AWS. For example, ``mem_gb: 2``
   provides approximately 1.86 GiB (1907 MiB) to the container.

The EBS volume is intended for temporary working data during job execution. By
default, volumes are deleted when the task terminates. To retain volumes after
task completion set ``retain_ebs: true`` in the Apsis ECS configuration.

EBS volumes use the gp3 volume type with IOPS and throughput automatically
scaled to the maximum allowed for the volume size:

- **IOPS**: 500 per GiB, up to 16,000 max
- **Throughput**: 0.25 MiB/s per IOPS, up to 1,000 MiB/s max

For example, a 32 GiB volume gets 16,000 IOPS and 1,000 MiB/s throughput (both
at maximum), while a 10 GiB volume gets 5,000 IOPS and 1,000 MiB/s throughput.

.. code:: yaml

    program:
        type: procstar-ecs
        command: "python train_model.py"
        mem_gb: 8
        vcpu: 4
        disk_gb: 100


IAM Roles
^^^^^^^^^

Specify an IAM role for the ECS task to assume:

.. code:: yaml

    program:
        type: procstar-ecs
        command: "aws s3 sync s3://bucket/data /data"
        role: my-ecs-task-role
        mem_gb: 2

The ``role`` parameter specifies the IAM role name (not the full ARN). Apsis
constructs the full ARN using the ``aws_account_id`` from the ECS
configuration.


Task Definition
^^^^^^^^^^^^^^^

By default, ECS programs use the task definition specified in
``default_task_definition`` in the Apsis configuration. You can override this
per-job to use a different task definition (and thus a different container image):

.. code:: yaml

    program:
        type: procstar-ecs
        command: "python run_tests.py"
        task_definition: procstar-ose-qa
        mem_gb: 2

This is useful when you have multiple task definitions with different container
images (e.g., production vs. QA environments).


Timeouts and Stop
^^^^^^^^^^^^^^^^^

ECS programs support the same ``timeout`` and ``stop`` configuration as standard
Procstar programs:

.. code:: yaml

    program:
        type: procstar-ecs
        command: "python long_running_job.py"
        mem_gb: 4
        timeout:
            duration: 3600      # 1 hour timeout
            signal: SIGTERM
        stop:
            signal: SIGTERM
            grace_period: 30s


ECS Task Definition Requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ECS task definition used by Apsis must meet these requirements:

1. **Container name**: The container running the Procstar agent must have a name
   that matches the ``container_name`` setting in the Apsis configuration (under
   ``procstar.agent.ecs.container_name``).

2. **Procstar agent flags**: The Procstar agent must be started with ``--wait``
   and ``--wait-timeout`` flags. This ensures:

   - The agent exits immediately after the job completes (via ``--wait``)
   - The agent exits if no work is assigned within the timeout period (via
     ``--wait-timeout``), preventing orphaned tasks if Apsis fails to connect,
     crashes during startup, or encounters any error before assigning work

   Example container command:

   .. code:: json

       ["--wait", "--wait-timeout", "900"]

   This configures a 15-minute timeout for initial work assignment.

3. **Environment variables**: Apsis sets these environment variables
   automatically:

   - ``PROCSTAR_GROUP_ID``: Unique identifier for this run (format:
     ``aws-ecs-{run_id}``)
   - ``APSIS_RUN_ID``: The Apsis run ID

4. **Network access**: The container must be able to connect to the Apsis
   Procstar agent server via WebSocket.

.. warning::

    Always configure ``--wait`` and ``--wait-timeout`` in your Procstar agent
    container. Without these flags, ECS tasks will continue running indefinitely
    after job completion or if Apsis fails to assign work, resulting in
    unnecessary AWS costs.


Isolation Model
^^^^^^^^^^^^^^^

Each ECS run gets a unique ``group_id`` in the format ``aws-ecs-{run_id}``. This
ensures:

- Complete isolation between runs
- No cross-run interference
- Automatic cleanup when the run completes

Unlike standard Procstar programs, you cannot specify a custom ``group_id`` for
ECS programs - attempting to do so will raise an error.


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


