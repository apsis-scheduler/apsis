import argparse
import ora
from pathlib import Path
import random
import sys
import yaml

# -------------------------------------------------------------------------------

PROB_DURATION = 0.1

MAX_JOB_ID_DEPTH = 3

LABELS = ("foo", "bar", "baz", "bif")

ARGS = {
    "fruit": ("apple", "pear", "kiwi", "mango"),
    "hue": ("red", "blue", "green", "yellow"),
}

# --------------------------------------------------------------------------------


def random_daytime():
    return str(ora.Daytime.from_ssm(random.randint(0, 86399)))


def gen_interval_schedule():
    count = 1 + int((random.random() ** 5) * 1000)
    interval = 86400 // count
    phase = random.randint(0, interval - 1)
    return {
        "type": "interval",
        "interval": interval,
        "phase": phase,
    }


def gen_daily_schedule(params):
    count = 1 + int((random.random() ** 3) * 1000)
    params.append("date")
    return {
        "type": "daily",
        "tz": "UTC",
        "daytime": sorted(random_daytime() for _ in range(count)),
    }


def gen_schedule(params):
    if random.random() < 0.25:
        return gen_interval_schedule()
    else:
        return gen_daily_schedule(params)


def gen_duration():
    r = random.random()
    return (
        0
        if r < 0.50
        else random.randint(1, 60)
        if r < 0.99
        else random.randint(1, 180) * 60  # up to 3 hours
    )


OUTPUT_SCRIPT = """
import random, string, sys, time
lines, length, delay = sys.argv[1 :]
length = int(length)
delay = float(delay)
for _ in range(int(lines)):
    print("".join( random.choice(string.ascii_letters) for _ in range(length) ))
    time.sleep(delay)
"""


def gen_output_program(labels):
    lines = random.randint(1, 10000)
    length = random.randint(8, 78)
    delay = round(random.random() * 0.02, 4)
    labels.append("output")
    return {
        "type": "program",
        "argv": [
            sys.executable,
            "-c",
            OUTPUT_SCRIPT,
            str(lines),
            str(length),
            str(delay),
        ],
    }


def gen_program(labels):
    r = random.random()
    if r < 0.99:
        return {
            "type": "no-op",
            "duration": str(gen_duration()),
        }

    else:
        return gen_output_program(labels)


def gen_deps(jobs, params):
    conds = []
    if random.random() < 0.1:
        job_id, job = random.choice(list(jobs.items()))
        if job["params"] == params:
            conds.append(
                {
                    "type": "dependency",
                    "job_id": job_id,
                }
            )
    return conds


def gen_job(jobs):
    params = []
    labels = list({random.choice(LABELS) for _ in range(int(random.uniform(0, 2) ** 2))})

    program = gen_program(labels)
    schedule = gen_schedule(params)
    conds = gen_deps(jobs, params)

    for param, args in ARGS.items():
        if random.random() < 0.05:
            params.append(param)
            schedule.setdefault("args", {})[param] = random.choice(args)

    return {
        "metadata": {
            "labels": labels,
        },
        "params": params,
        "program": program,
        "schedule": schedule,
        "condition": conds,
    }


def gen_job_id():
    return "/".join(
        (
            *(f"group{random.randint(0, 100)}" for _ in range(random.randint(0, MAX_JOB_ID_DEPTH))),
            f"job{random.randint(0, 100000)}",
        )
    )


def gen_jobs(num):
    jobs = {}
    for _ in range(args.num):
        job = gen_job(jobs)
        job_id = gen_job_id()
        jobs[job_id] = job
    return jobs


# --------------------------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument("num", metavar="NUM", type=int, help="generate NUM jobs")
parser.add_argument(
    "--output",
    metavar="DIR",
    type=Path,
    default=Path("./jobs"),
    help="generate to DIR [def: ./jobs]",
)
args = parser.parse_args()

jobs = gen_jobs(args.num)

for job_id, job in jobs.items():
    path = (args.output / job_id).with_suffix(".yaml")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as file:
        yaml.dump(job, file)
