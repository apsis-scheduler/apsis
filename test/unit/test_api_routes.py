"""
Routing of `/jobs/<job_id>` and `/jobs/<job_id>/runs`.

Job IDs may contain slashes, so `job_id` is a `path` param.  Sanic 21.6 keeps a
blueprint's routes in a set, so two overlapping path routes are registered in
arbitrary order and `GET /jobs/X/runs` used to resolve to either handler
depending on the process.  These tests check that the resolution is the same
in every process.
"""

import os
import subprocess
import sys

from sanic import Sanic

from apsis.service import api

# -------------------------------------------------------------------------------

PATHS = (
    "/api/v1/jobs/a",
    "/api/v1/jobs/a/b",
    "/api/v1/jobs/a/runs",
    "/api/v1/jobs/a/b/runs",
    "/api/v1/jobs/a/runs/c",
)


def resolve():
    """
    Returns {path: (handler name, job_id param)} for `PATHS`.
    """
    app = Sanic("test_api_routes")
    app.blueprint(api.API, url_prefix="/api/v1")
    app.router.finalize()
    res = {}
    for path in PATHS:
        _, handler, params = app.router.get(path, "GET", "")
        res[path] = (handler.__name__, params["job_id"])
    return res


def test_jobs_routes_resolve_to_single_handler():
    res = resolve()
    # Every /jobs/<...> path hits the same handler, which dispatches on the
    # suffix itself; the router no longer has a say.
    assert res == {
        "/api/v1/jobs/a": ("job", "a"),
        "/api/v1/jobs/a/b": ("job", "a/b"),
        "/api/v1/jobs/a/runs": ("job", "a/runs"),
        "/api/v1/jobs/a/b/runs": ("job", "a/b/runs"),
        "/api/v1/jobs/a/runs/c": ("job", "a/runs/c"),
    }


def test_jobs_routes_resolve_identically_across_processes():
    """
    Resolves the paths in several fresh interpreters with different hash seeds
    and checks they all agree.
    """
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]); "
        "from test_api_routes import resolve; print(repr(resolve()))"
    )
    results = set()
    for seed in range(6):
        env = dict(os.environ, PYTHONHASHSEED=str(seed))
        out = subprocess.run(
            [sys.executable, "-c", code, os.path.dirname(__file__)],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        results.add(out.stdout.strip())
    assert len(results) == 1, results
