.. _config:

Configuring Apsis
=================

Configure the Apsis server by writing a YAML configuration file and specifying
it with the `\--config` command line option.  You can override individual
configurations using the `\--override NAME=VALUE` command line option; use dotted
names like `runs.lookback=86400` to specify heirarchical configuration names.

A sample file illustrating all config variables and their default values is
below.  Relative paths are interpreted relative to the path containing the
config file.

.. code:: yaml

    jobs: ./jobs                # path
    database:
      path: ./apsis.db          # path
      timeout: 10s              # duration

    runs:
      lookback: null            # seconds

    schedule:
      since: null               # now, or YYYY-MM-DDTHH:MM:SSZ
      max_age: null             # seconds
      horizon: 86400            # seconds

    waiting:
      max_time: null            # duration

    program_types:
      # ...

    schedule_types:
      # ...

    action_types:
      # ...

    host_groups:
      # ...

    python:
      gc:
        enabled: true
        threshold: [100000, 100, 100]

    procstar:
      # ...


A duration is in in seconds, or you may give durations like `30s`, `10 min`
(600 seconds), `1.5h` (5400 seconds), and `1 day` (86400 seconds).


Files
-----

`jobdir` specifies the path to the directory containing job files.

`database.path` specifies the path to the database file containing run state.

`database.timeout` specifies the lock timeout when accessing the database.


Runs
----

`runs.lookback` specifies the maximum run age, in seconds.  Runs older than this
are not held in memory and are not visible in user interfaces.  They are
retained in the database file, however.


Schedule
--------

`schedule.since` specifies since when to schedule runs *on startup*.  This can
cause Apsis to skip over runs that would have been scheduled in the past, were
Apsis running at the time.

- `null`: schedule runs since the last time Apsis was run with the database
  file.  If the database file is used for the first time, schedule runs starting now.

- `now`: schedule runs starting now.

- A time with time zone, e.g. `2023-04-27T16:00:00Z`.

`schedule.max_age` sets a limit on how long Apsis can schedule a run after its
nominal schedule time.  Apsis fails if it ever tries to schedule a run older
than this.  This prevents very old runs from running spuriously.

`schedule.horizon` specified how far foward in time, in seconds, to schedule new
runs.


Waiting
-------

`waiting.max_time` limits the time, in seconds, a single run can be in the
waiting state.  After this time, Apsis transitions the run to the error state.


Types
-----

A program may specify a program, schedule, or action type by full Python name.  This allows
you to use a custom program type to extend Apsis, as long as the class is importable by
Apsis.  You may also configure shorter aliases for types:

.. code:: yaml

    program_types:
      foo_shell: foo.apsis.extension.programs.Shell
      foo_cmd: foo.apsis.extension.programs.Command

Likewise for schedules and actions:

.. code:: yaml

    schedule_types:
      foo_schedule: foo.apsis.extension.schedule.MySchedule

    action_types:
      foo_action: foo.apsis.extension.action.MyCustomAction


Host groups
-----------

A host groups enables a job to run a program on one of a group of hosts, rather
than on a fixed host.  You can specify a host group name in place of a host
name.  Host groups are configured globally.

The group type, `round-robin` or `random`, controls how hosts are chosen from
the group.

A single host name is effectively a host alias.

.. code:: yaml

    host_groups:
      my_group:
        type: round-robin
        hosts:
        - host1.example.com
        - host2.example.com
        - host3.example.com

      my_alias: host4.example.com


Python
------

Apsis allocates large numbers of Python objects, but does not heavily use Python
data structures.  Python's garbage collection (GC) will occasionally run for a
substantial time, which blocks Apsis and can lead to timeouts.  To enable
(default) or disable GC, or adjust its thresholds:

.. code:: yaml

    python:
      gc:
        enabled: false
        threshold: [100000, 100, 100]

The thresholds apply only if GC is enabled.  See documentation for
`gc.set_threshold()
<https://docs.python.org/3/library/gc.html#gc.set_threshold>`_ for an
explanation of these values.


Procstar
--------

The `procstar` section configures how Procstar-based programs are run.


Procstar agent server
~~~~~~~~~~~~~~~~~~~~~

Apsis can run a server that accepts connections from Procstar agents.  When
Apsis starts a run with a Procstar program, it chooses a connected Procstar
agent and dispatches the program to there for execution.

.. code:: yaml

    procstar:
      agent:
        enable: true

This enables the Procstar agent server on the default port and all network
interfaces.


.. code:: yaml

    procstar:
      agent:
        server:
          port: 50000
          host: "*"
          access_token: "topsecretaccesstoken"
          tls:
            cert_path: "/opt/cert/host.crt"
            key_path: "/opt/cert/host.key"
          reconnect_timeout: "1 hour"

This configures the server.

- `port` is the port to which to connect.  If not configured, Apsis uses the
  value of the `PROCSTAR_AGENT_PORT` environment variable, or 59789 if unset.

- `host` is the local hostname or IP number corresponding to the interface on
  which to serve.  If the hostname is `*`, runs on all interfaces.  If not
  configured, Apsis uses the value of the `PROCSTAR_AGENT_HOSTNAME` environment
  variable, or `*`.

- `access_token` is a secret string that agents must provide to connect to the
  server.  If not configured, Apsis uses the value of the `PROCSTAR_AGENT_TOKEN`
  environment variable.  The default is the empty string.

- `tls.cert_path` and `tls.key_path` are paths to TLS cert and corresponding key
  files.  If not configured, Apsis uses the `PROCSTAR_AGENT_CERT` and
  `PROCSTAR_AGENT_KEY` enviornment variables.  By default, uses a cert from the
  system cert bundle.


.. code:: yaml

    procstar:
      agent:
        connection:
          start_timeout: "1 min"
          reconnect_timeout: "1 hour"

This configures how Apsis handles Procstar groups.  When a Procstar instance
connects, it provides a group ID to which it belongs.  Each Procstar program
likewise carries a group ID in which it should run.  The default group ID for
both is named "default".  There is no registry of allowed group IDs: Apsis
accepts any group ID from a procstar instance, and if a program specifies a
group ID that Apsis hasn't seen, it optimistically assumes a Procstar instance
with this group ID will later connect.

If a Procstar run starts but no Procstar instance is connected in the specified
group, the run remains in the _starting_ state.  The `start_timeout`
configuration determines how long a Procstar run remains _starting_, before
Apsis transitions it to _error_.  The default is 0.

The `reconnect_timeout` duration determines how long Apsis waits for a Procstar
agent to reconnect.  This applies when Apsis restarts and attempts to reconnect
_running_ runs, or if a Procstar agent unexpectedly disconnects (due to a
network error or similar).  If the timeout expires, Apsis transitions any runs
on this agent to _error_ and forgets the agent.


.. code:: yaml

    procstar:
      agent:
        sudo:
          argv: ["/usr/bin/sudo", "--preserve-env", "--home"]

This configures how Procstar invokes `sudo` in agents, to run programs as
another user.  Apsis always adds `--user USERNAME` and `--non-interactive` to
the argv list.


.. code:: yaml

    procstar:
      agent:
        run:
          update_interval: "1 min"
          output_interval" "15 sec"

This configures how often Apsis requests process (including metadata) and output
updates for a run from the agent running it.  If null or omitted, Apsis does not
retrieve process metadata and output while the run is running, only once it
terminates.

