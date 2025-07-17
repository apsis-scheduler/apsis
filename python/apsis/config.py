import logging
from pathlib import Path
import warnings
from ruamel.yaml import YAML

from .lib.json import to_array
from .lib.parse import nparse_duration
from .lib.py import get_cfg, set_cfg

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


def normalize_path(path, base_path: Path):
    path = Path(path)
    if not path.is_absolute():
        path = base_path / path
    return path


def check(cfg, base_path: Path):
    def _check_duration(path):
        duration = nparse_duration(get_cfg(cfg, path, None))
        if duration is not None and duration <= 0:
            log.error("negative duration: {path}")
        else:
            set_cfg(cfg, path, duration)

    job_dir = normalize_path(cfg.get("job_dir", "jobs"), base_path)
    if not job_dir.exists():
        log.error(f"missing job directory: {job_dir}")
    cfg["job_dir"] = job_dir

    db_cfg = cfg.setdefault("database", {})
    # Backward compatibility: just the DB path.
    if isinstance(db_cfg, str):
        cfg["database"] = db_cfg = {"path": db_cfg}
    db_path = normalize_path(db_cfg.get("path", "apsis.db"), base_path)
    if not db_path.exists():
        log.error(f"missing database: {db_path}")
    db_cfg["path"] = db_path
    _check_duration("database.timeout")

    cfg["actions"] = to_array(cfg.get("action", []))

    waiting = cfg["waiting"] = cfg.setdefault("waiting", {})
    max_time = waiting["max_time"] = nparse_duration(waiting.get("max_time", None))
    if max_time is not None and max_time <= 0:
        log.error("negative waiting.max_time: {max_time}")

    _check_duration("procstar.agent.connection.start_timeout")
    _check_duration("procstar.agent.connection.reconnect_timeout")
    _check_duration("procstar.agent.run.update_interval")
    _check_duration("procstar.agent.run.output_interval")

    # runs_lookback → runs.lookback
    try:
        lookback = cfg["runs_lookback"]
    except KeyError:
        pass
    else:
        warnings.warn("config runs_lookback → runs.lookback", DeprecationWarning)
        cfg.setdefault("runs", {}).setdefault("lookback", lookback)

    return cfg


def load(path):
    """
    Loads configuration from `path`.

    If path is none, uses default configuration, based in the CWD.
    """
    if path is None:
        return check({}, Path.cwd())
    else:
        path = Path(path)
        with open(path) as file:
            cfg = YAML().load(file)
            if cfg is None:
                # Empty config.
                cfg = {}
        return check(cfg, path.parent.absolute())
