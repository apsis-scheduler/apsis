from ora import Time, Daytime, now, get_display_time_zone
import rich.box
import rich.console
from rich.style import Style
from rich.table import Table
from rich.text import Text
import rich.theme
import yaml

import apsis.lib.itr
import apsis.lib.memo
import apsis.lib.parse
import apsis.lib.py
import apsis.lib.string

# -------------------------------------------------------------------------------

THEME = rich.theme.Theme(
    {
        "arg": "#5f5f87",
        "error": "red1",
        "job": "#008787",
        "key": "color(244)",
        "param": "#5f5f87",
        "run": "#00af87",
        "time": "#505050",
        "warning": "orange3",
    }
)


@apsis.lib.memo.memoize
def get_console():
    return rich.console.Console(theme=THEME)


# -------------------------------------------------------------------------------

TABLE_KWARGS = {
    "box": rich.box.SIMPLE_HEAVY,
    "padding": (0, 0),
    "show_edge": False,
}

STATE_STYLE = {
    "new": Style(color="#00005f"),
    "scheduled": Style(color="#767676"),
    "starting": Style(color="#767676"),
    "waiting": Style(color="#626262"),
    "running": Style(color="#af8700"),
    "stopping": Style(color="#767676"),
    "success": Style(color="#00875f"),
    "failure": Style(color="#af0000"),
    "error": Style(color="#af00af"),
    "skipped": Style(color="#888888"),
}

_STATE_SYM = {
    "new": ".",
    "scheduled": "O",
    "starting": "›",
    "waiting": "|",
    "running": "»",
    "stopping": "≯",
    "success": "+",
    "failure": "X",
    "error": "!",
    "skipped": "-",
}

STATE_SYM = {
    s: Text("[") + Text(c, style=STATE_STYLE[s]) + Text("]") for s, c in _STATE_SYM.items()
}

# -------------------------------------------------------------------------------


def indent(string, indent):
    ind = " " * indent
    return "\n".join(ind + l for l in string.splitlines())


def format_duration(sec):
    m, s = divmod(int(round(sec)), 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return (
        f"{d}:{h:02}:{m:02}:{s:02}" if d > 0 else f"{h}:{m:02}:{s:02}" if h > 0 else f"{m}:{s:02}"
    )


def format_time(time):
    return "" if time == "" else format(Time(time), "%D %C@")


def match_state(state):
    return apsis.lib.string.prefix_match(STATE_STYLE.keys(), state.lower())


def get_run_start(run):
    return run["times"].get("running", run["times"].get("schedule", ""))


def get_run_elapsed(cur, run):
    try:
        start = Time(run["times"]["running"])
    except KeyError:
        return None
    else:
        return Time(cur if run["state"] == "running" else run["times"][run["state"]]) - start


def format_jso(jso, indent=0):
    ind = " " * indent
    wid = 12 - indent
    keys = (k for k in jso if k not in {"str"})
    keys = apsis.lib.py.to_front(keys, ("type",))
    return "\n".join(f"{ind}[key]{k:{wid}s}:[/] {jso[k]}" for k in keys)


def print_cond(cond, con, *, verbosity=0):
    if verbosity < 1:
        con.print(f"- {cond['str']}")
    else:
        con.print("•" + format_jso(cond, indent=2)[1:])


def format_program(program, *, verbosity=0, indent=0):
    return (
        f"{program['type']}: [bold]{program['str']}[/]"
        if verbosity < 1
        else format_jso(program, indent=indent)
    )


def format_schedule(schedule, indent=0):
    yield from format_jso(schedule, indent=indent)


def format_params(params):
    return ", ".join(f"[param]{p}[/]" for p in params)


def format_instance(run):
    return (
        f"[job]{run['job_id']}[/]("
        + ", ".join(f"{k}=[arg]{v}[/]" for k, v in run["args"].items())
        + ")"
    )


def print_job(job, con):
    # FIXME: Do better.
    con.print(yaml.dump(job))


def print_jobs(jobs, con):
    table = Table(**TABLE_KWARGS)
    table.add_column("job_id", style="job")
    table.add_column("params")
    table.add_column("labels")
    for job in sorted(jobs, key=lambda j: j["job_id"]):
        table.add_row(
            job["job_id"],
            "(" + format_params(job["params"]) + ")",
            " ".join(job.get("metadata", {}).get("labels", [])),
        )
    con.print(table)


def print_run(run, con, *, verbosity=0, run_log=None, similar_runs=None):
    # Run ID.
    run_id = run["run_id"]
    job = format_instance(run)
    con.print(f"[bold]run [run]{run_id}[/] {job}")

    def header(title):
        if verbosity >= 1:
            con.print()
            con.print(f"[u]{title}[/]")

    elapsed = get_run_elapsed(now(), run)
    elapsed = "" if elapsed is None else format_duration(elapsed)

    program = run.get("program", None)
    if program:
        header("Program")
        con.print(format_program(program, verbosity=verbosity))

    # Format conds.
    header("Conditions")
    if len(run["conds"]) > 0:
        for cond in run["conds"]:
            print_cond(cond, con, verbosity=verbosity)
    else:
        print("none")

    if verbosity < 1:
        state = run["state"]
        fmt_time = lambda n: format_time(run["times"][n])
        if state == "scheduled":
            time = "for " + fmt_time("schedule")
        elif state == "running":
            time = "since " + fmt_time("running")
        else:
            time = "at " + fmt_time(state)
        if elapsed:
            time += f" elapsed {elapsed}"
        con.print(STATE_SYM[state] + " " + time)

    if run_log is not None:
        header("Log")
        print_run_log(run_log, con)

    if verbosity > 0 and len(similar_runs) > 0:
        header("Run History")
        print_runs(similar_runs, con, arg_style=None)

    con.print()


def print_run_log(run_log, con):
    table = Table(show_header=False, **TABLE_KWARGS)
    table.add_column("time", style="time")
    table.add_column("message")
    for h in run_log:
        table.add_row(format_time(Time(h["timestamp"])), h["message"])
    con.print(table)


def print_runs(runs, con, *, arg_style="separate"):
    if len(runs) == 0:
        con.print("[red]No runs.[/]")
        return

    # FIXME: Support other sorts.
    runs = sorted(runs.values(), key=get_run_start)

    cur = now()

    table = Table(**TABLE_KWARGS)
    table.add_column("run_id")
    table.add_column("ste")
    table.add_column("start", style="time")
    table.add_column("elapsed", justify="right")
    table.add_column("job_id")
    if arg_style == "combined":
        for arg in runs[0]["args"]:
            table.add_column(arg)
    elif arg_style == "combined":
        table.add_column("args")

    for run in runs:
        elapsed = get_run_elapsed(cur, run)
        row = [
            f"[run]{run['run_id']}[/]",
            STATE_SYM[run["state"]],
            format_time(get_run_start(run)),
            "" if elapsed is None else format_duration(elapsed),
            f"[job]{run['job_id']}[/]",
        ]
        if arg_style == "combined":
            row.extend(run["args"].values())
        elif arg_style == "separate":
            row.append(" ".join(f"{k}={v}" for k, v in run["args"].items()))
        table.add_row(*row)

    con.print(table)
    con.print()


def print_api_error(err, con):
    con.print(f"[bold][red]Error:[/] {err}[/] [API status: {err.status}]")

    try:
        job_errors = err.jso["job_errors"]
    except KeyError:
        pass
    else:
        for job_id, msg in job_errors:
            con.print(f"- [job]{job_id}[/]")
            con.print(indent(msg, 2))


# -------------------------------------------------------------------------------


def parse_at_time(string):
    """
    Parses a time for when to run.

    Accepts a full time or a daytime (next occurrence).
    """
    if string == "now":
        return "now"

    if string.startswith("+"):
        return now() + apsis.lib.parse.parse_duration(string[1:])

    try:
        return Time(string)
    except ValueError:
        pass

    try:
        daytime = Daytime(string)
    except ValueError:
        pass
    else:
        # Assume this is today's daytime in the display time zone, unless
        # that has already passed, in which case use tomorrow.
        z = get_display_time_zone()
        date, daytime_now = now() @ z
        return (date if daytime_now < daytime else date + 1, daytime) @ z

    # FIXME: Accept expressions like "1 hour".

    raise ValueError(f"cannot interpret as time: {string}")
