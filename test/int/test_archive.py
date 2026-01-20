from contextlib import closing
import pytest
import sqlite3
import time

from apsis.service.client import APIError
from procstar_instance import ApsisService

# -------------------------------------------------------------------------------


def test_archive(tmp_path):
    path = tmp_path / "archive.db"
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()

    with (
        closing(
            ApsisService(
                cfg={"schedule": {"horizon": 1}},
                job_dir=job_dir,
            )
        ) as inst,
        inst.agent(),
    ):
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        client = inst.client

        # Run two runs, wait 2 s, then run another.

        res = client.schedule_adhoc("now", {"program": {"type": "no-op"}})
        run_id0 = res["run_id"]
        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "procstar-shell",
                    "command": "echo 'Hello, world!'",
                }
            },
        )
        run_id1 = res["run_id"]

        time.sleep(2)

        res = client.schedule_adhoc("now", {"program": {"type": "no-op"}})
        run_id2 = res["run_id"]

        # Archive, with a max age of 2 s and up to 1 run.
        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "apsis.program.internal.archive.ArchiveProgram",
                    "age": 2,
                    "count": 1,
                    "path": str(path),
                },
            },
        )
        res = inst.wait_run(res["run_id"])
        # The first run has been archived.
        assert res["meta"]["program"]["run count"] == 1
        assert res["meta"]["program"]["run_ids"] == [[run_id0]]

        # The first run is no longer be available; the other two are.
        with pytest.raises(APIError):
            client.get_run(run_id0)
        assert client.get_run(run_id1)["state"] == "success"
        assert client.get_run(run_id2)["state"] == "success"

        # Archive, with a max age of 2 s.
        time.sleep(0.5)
        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "apsis.program.internal.archive.ArchiveProgram",
                    "age": 2,
                    "count": 10,
                    "path": str(path),
                },
            },
        )
        # The second run was archived, but the third isn't old enough yet.
        res = inst.wait_run(res["run_id"])
        assert res["meta"]["program"]["run count"] == 1
        assert res["meta"]["program"]["run_ids"] == [[run_id1]]

        # The second run is no longer available.
        with pytest.raises(APIError):
            client.get_run(run_id1)
        assert client.get_run(run_id2)["state"] == "success"

        # Check the archive file.
        with closing(sqlite3.connect(path)) as db:
            rows = list(db.execute("SELECT run_id, state FROM runs"))
            assert rows == [
                (run_id0, "success"),
                (run_id1, "success"),
            ]

            rows = list(db.execute("SELECT message FROM run_history WHERE run_id = ?", (run_id0,)))
            assert "scheduled: now" in rows[0][0]
            assert "success" in rows[-1][0]

            rows = list(db.execute("SELECT run_id, name, length FROM output"))
            # Only the second run had output.
            assert len(rows) == 1
            assert rows[0] == (run_id1, "combined stdout & stderr", 14)


def test_archive_chunks(tmp_path):
    path = tmp_path / "archive.db"
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()

    with closing(
        ApsisService(
            cfg={"schedule": {"horizon": 1}},
            job_dir=job_dir,
        )
    ) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        client = inst.client

        # Run 100 runs.
        res = client.schedule_adhoc("now", {"program": {"type": "no-op"}}, count=100)
        run_ids = {r["run_id"] for r in res}
        for run_id in run_ids:
            inst.wait_run(run_id)

        time.sleep(1)

        # Archive, with a max age of 1 s and up to 80 runs, chunked by 10.
        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "apsis.program.internal.archive.ArchiveProgram",
                    "age": 1,
                    "count": 80,
                    "chunk_size": 10,
                    "chunk_sleep": 0.1,
                    "path": str(path),
                },
            },
        )
        res = inst.wait_run(res["run_id"])
        # Runs have been archived.
        meta = res["meta"]["program"]
        assert meta["run count"] == 80
        assert len(meta["run_ids"]) == 8
        assert all(len(c) == 10 for c in meta["run_ids"])
        assert all(r in run_ids for c in meta["run_ids"] for r in c)

        # Check the archive file.
        with closing(sqlite3.connect(path)) as db:
            rows = set(db.execute("SELECT run_id, state FROM runs"))
            assert len(rows) == 80
            assert all(r[0] in run_ids and r[1] == "success" for r in rows)


def test_clean_up_jobs(tmp_path):
    path = tmp_path / "archive.db"
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()

    with closing(
        ApsisService(
            cfg={"schedule": {"horizon": 1}},
            job_dir=job_dir,
        )
    ) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        client = inst.client

        # Run two runs, wait 2 s, then run another.

        res = client.schedule_adhoc("now", {"program": {"type": "no-op"}})
        run_id0 = res["run_id"]
        job_id0 = res["job_id"]
        res = client.schedule_adhoc("now", {"program": {"type": "no-op"}})
        run_id1 = res["run_id"]
        job_id1 = res["job_id"]

        time.sleep(2.5)

        res = client.schedule_adhoc("now", {"program": {"type": "no-op"}})
        run_id2 = res["run_id"]
        job_id2 = res["job_id"]

        inst.stop_serve()

        # Check the DB.
        with closing(sqlite3.connect(inst.db_path)) as db:
            job_ids = {j for (j,) in db.execute("SELECT job_id FROM jobs")}
        assert job_ids == {job_id0, job_id1, job_id2}

        inst.start_serve()
        inst.wait_for_serve()

        # Archive with a max age of 2 s.
        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "apsis.program.internal.archive.ArchiveProgram",
                    "age": 2,
                    "count": 10,
                    "path": str(path),
                },
            },
        )
        res = inst.wait_run(res["run_id"])
        archive_job_id = res["job_id"]
        # The first two runs have been archived.
        meta = res["meta"]["program"]
        assert meta["run count"] == 2
        assert len(meta["run_ids"]) == 1  # one chunk
        assert set(meta["run_ids"][0]) == {run_id0, run_id1}

    # Check the DB.  Only the third job ID should remain, plus the job ID from
    # the archive job.
    with closing(sqlite3.connect(inst.db_path)) as db:
        job_ids = {j for (j,) in db.execute("SELECT job_id FROM jobs")}
    assert job_ids == {job_id2, archive_job_id}


def test_vacuum():
    with closing(ApsisService()) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        client = inst.client

        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "apsis.program.internal.vacuum.VacuumProgram",
                },
            },
        )
        res = inst.wait_run(res["run_id"])
        assert res["state"] == "success"
        meta = res["meta"]["program"]
        assert meta["time"] > 0
