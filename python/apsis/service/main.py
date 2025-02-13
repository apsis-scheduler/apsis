import asyncio
import logging
from   pathlib import Path
import sanic
import sanic.response
import sanic.router
import signal

from   apsis import __version__
from   apsis.apsis import Apsis
from   apsis.exc import JobsDirErrors
from   apsis.jobs import load_jobs_dir
from   apsis.lib.asyn import cancel_task
from   apsis.sqlite import SqliteDB
from   . import api, control, procstar
from   . import DEFAULT_PORT

log = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

SANIC_LOG_CONFIG = {
    **sanic.log.LOGGING_CONFIG_DEFAULTS,
    "formatters": {
        "generic": {
            "class": "apsis.lib.logging.Formatter",
        },
        "access": {
            "class": "apsis.lib.logging.AccessFormatter",
        },
    },
}
SANIC_LOG_CONFIG["loggers"]["sanic.access"]["propagate"] = False

class Router(sanic.router.Router):
    """
    Extended router that supports a catch-all path for missing pages.
    """

    CATCH_ALL_PATH = "/index.html"

    def get(self, path, method, host):
        try:
            return super().get(path, method, host)
        except sanic.router.NotFound:
            return self._get(self.CATCH_ALL_PATH, method, "")



app = sanic.Sanic("apsis", router=Router(), log_config=SANIC_LOG_CONFIG)
app.config.LOGO = None

app.blueprint(api.API, url_prefix="/api/v1")
app.blueprint(control.API, url_prefix="/api/control")
app.blueprint(procstar.API, url_prefix="/api/procstar")

vue_dir = Path(__file__).parent / "vue"
assert vue_dir.is_dir()

# The SPA.
app.static("/index.html", str(vue_dir / "index.html"))
# Web assets.
app.static("/static", str(vue_dir / "static"))

#-------------------------------------------------------------------------------

def build_apsis(cfg):
    db_cfg = cfg["database"]
    db_path = db_cfg["path"]

    log.info(f"opening state file {db_path}")
    db = SqliteDB.open(db_path, timeout=db_cfg.get("timeout"))

    job_dir = cfg["job_dir"]
    log.info(f"opening jobs dir {job_dir}")
    try:
        jobs = asyncio.get_event_loop().run_until_complete(load_jobs_dir(job_dir))
    except JobsDirErrors as exc:
        for err in exc.errors:
            log.error(f"job {err.job_id}: {err}")
        raise

    return Apsis(cfg, jobs, db)


# FIXME: Get host, port from config.
def serve(cfg, host="127.0.0.1", port=DEFAULT_PORT, debug=False):
    """
    Runs the Apsis service.

    :return:
      True if the service should be restarted on exit.
    """
    log.info(f"starting Apsis {__version__} service")

    apsis = build_apsis(cfg)

    app.apsis = apsis
    # Flag to indicate whether to restart after shutting down.
    app.restart = False
    app.running = True  # FIXME: ??  Remove?

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    # Set up the HTTP server.
    log.info("starting HTTP service")
    server = app.create_server(
        host        =host,
        port        =port,
        debug       =debug,
        return_asyncio_server=True,
    )
    server_task = asyncio.ensure_future(server)

    # Get Apsis running.
    restore_task = asyncio.ensure_future(apsis.restore())
    apsis.start_loops()

    # Shut down on signals; this is the correct way to request shutdown.
    def on_shutdown(signum, stack_frame):
        if signum == signal.SIGINT:
            # If this was actually from the keyboard, there'll be a ^C to the
            # console without a newline; add it.
            print()
        log.error(f"caught {signal.Signals(signum).name}")

        async def stop():
            try:
                # Clean up the restore task.
                await cancel_task(restore_task, "restore", log)
                # Shut down the Sanic web service.
                await cancel_task(server_task, "Sanic", log)

                # Shut down Apsis and all its bits.
                await apsis.shut_down()

            finally:
                # Then tell the asyncio event loop to stop.
                log.info("stopping event loop")
                asyncio.get_event_loop().stop()

        asyncio.ensure_future(stop())

    log.info("setting signal handlers")
    signal.signal(signal.SIGINT , on_shutdown)  # instead of KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_shutdown)

    log.info("service ready to run")
    try:
        loop.run_forever()
    finally:
        # Explicitly close the loop, so we find out about any pending tasks we
        # have incorrectly left behind.
        loop.close()
    log.info("service done")

    return app.restart


