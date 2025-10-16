"""
Test to verify that WAL (Write-Ahead Log) file is removed when Apsis is properly stopped.

The WAL file should only exist while Apsis is running, not after it has been stopped.
"""

import time
from pathlib import Path

from instance import ApsisService


def test_wal_file_removed_on_stop():
    """
    Verify that the SQLite WAL file is removed after Apsis is properly stopped.
    """
    svc = ApsisService()
    svc.create_db()

    svc.write_cfg()

    db_path = svc.db_path
    wal_path = Path(str(db_path) + "-wal")
    shm_path = Path(str(db_path) + "-shm")

    svc.start_serve()
    svc.wait_for_serve()

    client = svc.client
    res = client.schedule_adhoc("now", {"program": {"type": "no-op"}})
    run_id0 = res["run_id"]
    res = client.schedule_adhoc(
        "now",
        {
            "program": {
                "type": "shell",
                "command": "echo 'Hello, world!'",
            }
        },
    )
    run_id1 = res["run_id"]

    time.sleep(1)

    assert svc.is_running(), "Service should be running"
    assert (
        wal_path.exists()
    ), f"WAL file should exist at {wal_path} while Apsis is running"

    wal_size = wal_path.stat().st_size
    assert wal_size > 0, f"WAL file should have content, but size is {wal_size} bytes"
    print(f"WAL file size before shutdown: {wal_size} bytes")

    return_code = svc.stop_serve()
    print(db_path)
    time.sleep(10)
    assert (
        return_code == 0
    ), f"Service should stop cleanly, got return code {return_code}"

    assert not svc.is_running(), "Service should be stopped"

    assert (
        not wal_path.exists()
    ), f"WAL file at {wal_path} should be removed after Apsis stops"
    assert (
        not shm_path.exists()
    ), f"SHM file at {shm_path} should be removed after Apsis stops"
