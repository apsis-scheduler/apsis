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
    if request.app.authenticated:
        return error("debug is disabled on a unix socket", 403)
    breakpoint()
    return response_json({})


@API.route("/reload_jobs", methods={"POST"})
async def on_reload_jobs(request):
    (dry_run,) = request.args.pop("dry_run", False)
    dry_run = to_bool(dry_run)
    try:
        rem_ids, add_ids, chg_ids = await apsis.apsis.reload_jobs(
            request.app.apsis, dry_run=dry_run, user=request.ctx.user
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


@API.route("/shut_down", methods={"POST"})
async def on_shut_down(request):
    # Sanic ignores query params without values.
    restart = "restart" in urllib.parse.parse_qs(request.query_string, True)
    request.json  # FIXME: Ignored.

    # Request to restart after shutdown.
    log.info(apsis.apsis.by_user(f"shut down starting; restart={restart}", request.ctx.user))
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
