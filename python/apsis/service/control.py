import logging
import os
import sanic
import signal
import urllib.parse

import apsis.apsis
from apsis.exc import JobsDirErrors
from apsis.lib.api import to_bool, response_json, error

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------

API = sanic.Blueprint("control")


@API.route("/debug")
async def on_debug(request):
    breakpoint()
    return response_json({})


@API.route("/reload_jobs", methods={"POST"})
async def on_reload_jobs(request):
    (dry_run,) = request.args.pop("dry_run", ["False"])
    dry_run = to_bool(dry_run)
    try:
        rem_ids, add_ids, chg_ids = await apsis.apsis.reload_jobs(
            request.app.apsis, dry_run=dry_run
        )
    except JobsDirErrors as exc:
        return error(exc, job_errors=[[str(e.job_id), str(e)] for e in exc.errors])
    else:
        return response_json(
            {
                "removed": sorted(rem_ids),
                "added": sorted(add_ids),
                "changed": sorted(chg_ids),
                "dry_run": dry_run,
            }
        )


# FIXME: Reload a single job id?


@API.route("/rename_job_history", methods={"POST"})
async def on_rename_job_history(request):
    old_job_id = request.json["old_job_id"]
    new_job_id = request.json["new_job_id"]
    try:
        count = apsis.apsis.rename_job_history(request.app.apsis, old_job_id, new_job_id)
    except LookupError as exc:
        return error(exc, status=404)
    except ValueError as exc:
        return error(exc)
    else:
        return response_json({"runs": count})


@API.route("/shut_down", methods={"POST"})
async def on_shut_down(request):
    # Sanic ignores query params without values.
    restart = "restart" in urllib.parse.parse_qs(request.query_string, True)
    request.json  # FIXME: Ignored.

    # Request to restart after shutdown.
    log.info(f"shut down starting; restart={restart}")
    request.app.restart = restart

    # Shut down the application.
    os.kill(os.getpid(), signal.SIGTERM)

    # shut_down() calls loop.stop(), so no further scheduled callbacks will be
    # invoked.  We need to respond to the request with no further await. (?)
    return response_json({})


@API.route("/version")
async def on_version(request):
    return response_json({"version": apsis.__version__})


# ws /control/log
