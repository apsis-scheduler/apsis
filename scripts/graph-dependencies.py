# Requires graphviz and graphviz Python module.

import argparse
import asyncio
import graphviz
from   pathlib import Path

from   apsis.actions import ScheduleAction
from   apsis.cond.dependency import Dependency
from   apsis.jobs import load_jobs_dir

#-------------------------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument(
    "path", metavar="DIR", type=Path,
    help="check the jobs in DIR")
parser.add_argument(
    "--output", metavar="PATH", type=Path, default="./dependencies.pdf",
    help="write output to PATH")
args = parser.parse_args()

# Load all jobs.
jobs_dir = asyncio.run(load_jobs_dir(args.path))
jobs = jobs_dir.get_jobs()

dot = graphviz.Digraph(
    graph_attr={
        "rankdir": "LR",
    },
    node_attr={
        "shape": "box",
        "fontname": "Helvetica",
        "fontsize": "10",
    },
)
for job in jobs:
    # dot.node(job.job_id, job.job_id)
    for cond in job.conds:
        if isinstance(cond, Dependency):
            dot.edge(cond.job_id, job.job_id)
    for action in job.actions:
        if isinstance(action, ScheduleAction):
            dot.edge(job.job_id, action.job_id, dir="both", arrowtail="dot")

dot.view(args.output)

