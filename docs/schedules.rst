.. _schedules:

Schedules
=========

The `schedule` key configures when Apsis should schedule runs of the job.  You
can give a single schedule, or a list of multiple schedules.

Each schedule has a `type` key, which specifies the schedule to use.  These are
described below.


Enabled
-------

Each schedule has an `enabled` key.  If set to false, the schedule is disabled
and Apsis schedules no runs with it.  If omitted, the schedule is enabled.


Args
----

Each schedule has an `args` key, which specifies the arg values corresponding to
the job's params.  For example, when you schedul ethe job `check
host(hostname)`, you must provide an arg value for `hostname`.

.. code:: yaml

    schedule:
        ...
        args:
            hostname: server7.example.com

Some schedules can automatically provide certain args.  For example, the
`Interval schedule`_ provides the `time` arg automatically.  When Apsis creates
a run with this schedule, it provides a `time` arg with the (stringified) time
at which the job was scheduled.

If you don't need to provide any args, either because the job has no params, or
because all args are provided by the schedule, you can omit the `args` key.


Interval schedule
-----------------

An *interval schedule* (`type: interval`) schedules a run at regular intervals.
The interval is given in seconds.

.. code:: yaml

    schedule:
        type: interval
        interval: 14400  # every four hours

Apsis schedules a run for each time where `time % interval == 0`, where `time`
is the UTC time since epoch (1970-01-01T00:00:00+00:00).  To specify another
phase, give the `phase` key; its value is also in seconds.

The interval schedule will automatically provide an arg for a `time` param.  The
value for each run is the time for which the run scheduled.


Daily schedule
--------------

With a *daily schedule* (`type: daily`), Apsis schedules a run at a certain
daytime (time of day) every day.  You must provide the daytime, as well as the
time zone in which to interpret the daytime.

.. code:: yaml

    schedule:
        type: daily
        daytime: 10:30:00
        tz: Europe/Berlin

Apsis will schedule a run each day at 10:30 AM, Berlin time.

You can provide multiple daytimes.

.. code:: yaml

    schedule:
        type: daily
        daytime:
          - 10:30:00
          - 12:30:00
          - 16:30:00
        tz: Europe/Berlin

You can limit the dates that Apsis will schedule the run on, by specify a
_calendar_, which contains only certain dates.  For instances, the calendar
`Mon-Fri` contains all Mondays through Fridays, but not Saturdays or Sundays.

.. code:: yaml

    schedule:
        type: daily
        daytime: 10:30:00
        tz: Europe/Berlin
        calendar: Mon-Fri

Apsis gets calendars from Ora.  In addition to weekday calendars like `Thu`,
`Mon-Fri`, and `Tue,Thu`, you can provide your own custom calendars.  See `Ora's
calendar docs
<https://ora.readthedocs.io/en/latest/calendars.html#finding-calendars>`_ for
details.

The daily schedule will automatically provide args for params `date`, `time`,
`daytime`, `calendar`, and `tz`, though you can override these explicitly.


.. _shifts:

Date and calendar shift
```````````````````````

You can shift the schedule time for each run entire days backward or forward.
This will result in the schedule time for the run occurring on a different date
than the nominal date of the run, which is passed as the `date` arg.

For example, this job is scheduled to run at 20:00 UTC on the day before each
Monday, i.e. on each Sunday.  The `date` arg will always be a Monday, though.
For example, a run will be scheduled to start at 2020-11-09T20:00:00+00:00, but
its arg will be `date=2020-11-10`.

.. code:: yaml

    params: [date]

    schedule:
        type: daily
        daytime: 20:00:00
        tz: UTC
        calendar: Mon
        date_shift: -1

You can also specify a shift in calendar days; this shifts days according to the
job's calendar.  If you specify both date and calendar shifts, the calendar
shift is applied before the physical shift.

The job below runs for every weekday, Monday through Friday.  The job is
scheduled for the same day at 20:00 UTC, except that Friday jobs run on the
following Sundays instead.  This is because a Friday is shifted forward one day
in the calendar to Monday, and then shifted one physical day backward to Sunday.

.. code:: yaml

    params: [date]

    schedule:
        type: daily
        daytime: 20:00:00
        tz: UTC
        calendar: Mon-Fri
        cal_shift: 1
        date_shift: -1


Daily interval schedule
-----------------------

A *daily interval schedule* (`type: daily-interval`) schedules a run at regular
intervals between a start and stop daytime.  You must provide a time zone.

.. code:: yaml

    schedule:
        type: daily-interval
        start: 12:00:00
        stop: 17:00:00
        interval: 1800
        tz: Asia/Tokyo

Apsis schedule a run every half hour (1800 seconds) from noon until 5 PM, Tokyo
time.  The stop time is not inclusive, so the last scheduled run will be at 4:30
PM.

As with the daily schedule, speficy a calendar to run on certain days only.

.. code:: yaml

    schedule:
        type: daily-interval
        start: 12:00:00
        stop: 17:00:00
        interval: 1800
        tz: Asia/Tokyo
        calendar: Mon-Fri

The daily schedule will automatically provide args for params `date`, `time`,
`daytime`, `calendar`, and `tz`, though you can override these explicitly.

You can also specify a date and/or calendar shift (see :ref:`shifts`).

.. code:: yaml

    schedule:
        type: daily-interval
        start:
          daytime: 12:00:00
          cal_shift: -1
        stop:
          daytime: 17:00:00
          date_shift: -1
        ...

Note that if the start time does not exist on a given date, due to a DST
transition, the schedule will include no times on this date at all.  For
example, a daily schedule with a start time between 2:00:00 and 3:00:00 and a
U.S. time zone will contain no times on the dates in the spring when DST begins.


.. _stop-schedules:

Stop schedules
--------------

You can also configure a job so that a run will stop at a certain time.  When
this time elapsis, Apsis stops the run, assuming it is running.  The ways you
schedule a run to stop are different from the schedule types above, since the
stop time is generally related to the schedule time.

To configure a stop schedule in addition to the normal start schedule, place the
latter into `start:` subkey, and use `stop:` to specify the stop schedule.  The
available stop schedule types are `duration` and `daytime`.  For example,

.. code:: yaml

    schedule:
        start:
            type: daily
            daytime: 10:30:00
            tz: Europe/Berlin
        stop:
            type: duration
            duration: 30m

This instructs Apsis to run the program for 30 minutes after the schedule time,
namely 11:00 Europe/Berlin.

The `daytime` stop schedule type instructs Apsis to stop the run at the next
occurence of a specific daytime after the schedule time.  The following example
has the same effect as the previous:

.. code:: yaml

    schedule:
        start:
            type: daily
            daytime: 10:30:00
            tz: Europe/Berlin
        stop:
            type: daytime
            daytime: 11:00:00
            tz: Europe/Berlin

You can use any of the schedule types in the previous section for the `start`
schedule.

When the stop time elapses, Apsis stops the run in accordance with the program's
stop method.  This is discussed in :ref:`program-stop`.

