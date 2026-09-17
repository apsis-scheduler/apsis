"""
Routing of `/jobs/<job_id>` and `/jobs/<job_id>/runs`.

Job IDs may contain slashes, so `job_id` is a `path` param.  Sanic 21.6 keeps a
blueprint's routes in a set, so two overlapping path routes are registered in
arbitrary order and `GET /jobs/X/runs` used to resolve to either handler
depending on the process.  Assert that the overlapping route is absent, so
regression detection does not depend on which registration order happens to win.
"""

import uuid

import pytest
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


@pytest.fixture
def app():
    # sanic rejects a second app with the same name in one process
    app = Sanic(f"test_api_routes_{uuid.uuid4().hex}")
    app.blueprint(api.API, url_prefix="/api/v1")
    app.router.finalize()
    return app


def test_jobs_routes_do_not_overlap(app):
    paths = sorted(
        route.path
        for route in app.router.routes
        if route.path == "api/v1/jobs" or route.path.startswith("api/v1/jobs/")
    )
    assert paths == ["api/v1/jobs", "api/v1/jobs/<job_id:path>"]


def test_jobs_routes_resolve_to_single_handler(app):
    res = {}
    for path in PATHS:
        _, handler, params = app.router.get(path, "GET", "")
        res[path] = (handler.__name__, params["job_id"])
    # Every /jobs/<...> path hits the same handler, which dispatches on the
    # suffix itself; the router no longer has a say.
    assert res == {
        "/api/v1/jobs/a": ("job", "a"),
        "/api/v1/jobs/a/b": ("job", "a/b"),
        "/api/v1/jobs/a/runs": ("job", "a/runs"),
        "/api/v1/jobs/a/b/runs": ("job", "a/b/runs"),
        "/api/v1/jobs/a/runs/c": ("job", "a/runs/c"),
    }
