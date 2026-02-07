import asyncio
import logging
from logging import log, debug, info, warning, error, critical
import logging.handlers
import ora
import rich.highlighter
import rich.logging
import rich.text
import threading

import apsis.cmdline

# -------------------------------------------------------------------------------


def set_log_levels():
    # Quiet some noisy stuff.
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.INFO)
    logging.getLogger("websockets.protocol").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)


# -------------------------------------------------------------------------------


class Formatter(logging.Formatter):
    def formatMessage(self, rec):
        time = ora.UNIX_EPOCH + rec.created
        level = rec.levelname
        return f"{time:%Y-%m-%dT%.3C} {rec.name:24s} {level[0]} {rec.message}"


class AccessFormatter(logging.Formatter):
    def formatMessage(self, record):
        time = ora.UNIX_EPOCH + record.created
        level = record.levelname
        return (
            f"{time:%Y-%m-%dT%.3C} {record.name:24s} {level[0]} "
            f"{record.status} {record.request} ({record.host})"
        )


def configure(*, level="WARNING"):
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Root logger's formatter.
    logging.getLogger().handlers[0].formatter = Formatter()

    set_log_levels()


# -------------------------------------------------------------------------------

LEVEL_STYLES = {
    "DEBUG": "color(249)",
    "INFO": "color(243)",
    "WARNING": "color(100)",
    "ERROR": "color(125)",
    "CRITICAL": "color(196) bold",
}


class RichFormatter(logging.Formatter):
    def formatMessage(self, record):
        time = ora.UNIX_EPOCH + record.created
        level = record.levelname
        level = f"[{LEVEL_STYLES[level]}]{level[0]}[/]"
        return f"{time:%.3C} [color(24)]{record.name:24s}[/] {level} {record.message}"


class RichHandler(rich.logging.RichHandler):
    def render(self, *, record, traceback, message_renderable):
        return rich.text.Text.from_markup(str(message_renderable))


def rich_configure(*, level="WARNING"):
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=apsis.cmdline.get_console())],
    )

    # Root logger's formatter.
    logging.getLogger().handlers[0].formatter = RichFormatter()

    set_log_levels()
