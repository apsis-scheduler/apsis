"""
Phase 0 de-risking probes for iac#2129.

A1: can paginated reads run OFF the event loop, on a separate read-only SQLite
    connection, concurrently with writes on the main connection?
A2: how bad is the unindexed `with_args` filter? A page's LIMIT bounds rows
    RETURNED, not rows SCANNED -- measure a selective filter over the whole table.

Throwaway temp DB only. Run: python test/manual/bench_runs_probe.py [N_RUNS]
"""

import os
import sqlite3
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

import ora
import ujson

import apsis.sqlite as S
from apsis.program.noop import NoOpProgram
from apsis.runs import Instance, Run
from apsis.states import State

N_RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
N_JOBS = 5


def _template():
    inst = Instance("job_0", {"date": "2026-09-03", "instance": "prod", "shard": "07"})
    run = Run(inst)
    run.run_id, run.timestamp, run.state = "r1", ora.now(), State.success
    run.program = NoOpProgram()
    run.times = {"schedule": ora.now(), "running": ora.now(), "success": ora.now()}
    run.meta = {
        "program": {"pid": 12345, "host": "compute-node-42"},
        "elapsed": 12.3,
        "notes": "seeded run " * 4,
    }
    run.run_state = {"procstar": {"conn_id": "abc-123", "proc_id": "p-98765"}}
    return (
        ujson.dumps(run.program.to_jso()),
        ujson.dumps({n: str(t) for n, t in run.times.items()}),
        ujson.dumps(run.meta),
        ujson.dumps(run.run_state),
    )


def seed(db, n):
    program, times, meta, run_state = _template()
    states = [s.name for s in (State.success, State.failure, State.running, State.error)]
    conn = db.conn
    base = ora.now() - n
    rows = [
        (
            f"r{i}",
            S.dump_time(base + i),
            f"job_{i % N_JOBS}",
            # 1 in 5000 rows has shard=RARE -> a very selective with_args filter
            ujson.dumps(
                {"date": "2026-09-03", "shard": ("RARE" if i % 5000 == 0 else f"{i % 100:02d}")}
            ),
            states[i % len(states)],
            program,
            None,
            None,
            times,
            meta,
            run_state,
            0,
            i,
        )
        for i in range(1, n + 1)
    ]
    conn.executemany(
        """INSERT INTO runs (run_id,timestamp,job_id,args,state,program,conds,
           actions,times,meta,run_state,expected,rowid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    print(f"seeded {n:,} runs\n")


# ---- A1: off-loop read on a separate read-only connection, during writes ----


def probe_offloop(path):
    print("=== A1: separate read-only connection, reading while main conn writes ===")
    # A brand-new sqlite3 connection, read-only, opened in THIS call (we'll hand it
    # to a worker thread). check_same_thread=False so a thread pool can use it.
    ro = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)

    result = {}

    def worker():
        t0 = time.perf_counter()
        cur = ro.execute(
            "SELECT rowid, run_id, args, meta FROM runs WHERE job_id='job_0' "
            "ORDER BY rowid DESC LIMIT 500"
        )
        rows = cur.fetchall()
        result["rows"] = len(rows)
        result["ms"] = (time.perf_counter() - t0) * 1000

    # Meanwhile hammer writes on a *different* connection to simulate the live app.
    rw = sqlite3.connect(path, check_same_thread=False)
    rw.execute("PRAGMA journal_mode=WAL")
    writes = {"n": 0, "stop": False}

    def writer():
        i = N_RUNS
        while not writes["stop"]:
            i += 1
            rw.execute("UPDATE runs SET state='running' WHERE rowid=?", ((i % N_RUNS) + 1,))
            rw.commit()
            writes["n"] += 1

    wt = threading.Thread(target=writer)
    wt.start()
    try:
        th = threading.Thread(target=worker)
        th.start()
        th.join(timeout=10)
    finally:
        writes["stop"] = True
        wt.join()

    ok = "rows" in result
    print(
        f"  read from worker thread while {writes['n']} concurrent writes ran: "
        f"{'OK' if ok else 'FAILED/BLOCKED'}"
    )
    if ok:
        print(f"  got {result['rows']} rows in {result['ms']:.1f} ms (off the main thread)")
    print(
        "  => off-loop paginated reads via a separate read-only connection are "
        f"{'VIABLE' if ok else 'NOT viable'}.\n"
    )
    ro.close()
    rw.close()


# ---- A2: unindexed with_args -- LIMIT bounds returned, not scanned ----


def probe_with_args(db):
    print("=== A2: unindexed with_args filter (shard=RARE, ~1/5000 rows) ===")
    conn = db.conn
    # One page: newest 500 matching shard=RARE. There are only ~N/5000 total, so to
    # fill (or exhaust) a page of 500 SQLite must scan a huge slice of the table.
    t0 = time.perf_counter()
    cur = conn.execute(
        "SELECT rowid FROM runs "
        "WHERE job_id='job_0' AND json_extract(args,'$.shard')='RARE' "
        "ORDER BY rowid DESC LIMIT 500"
    )
    rows = cur.fetchall()
    ms = (time.perf_counter() - t0) * 1000
    print(f"  returned {len(rows)} rows in {ms:.1f} ms for ONE page")

    # Compare: EXPLAIN QUERY PLAN to show it's a full scan, and count how many rows
    # exist so we see the scan/return ratio.
    plan = conn.execute(
        "EXPLAIN QUERY PLAN SELECT rowid FROM runs "
        "WHERE job_id='job_0' AND json_extract(args,'$.shard')='RARE' "
        "ORDER BY rowid DESC LIMIT 500"
    ).fetchall()
    total_match = conn.execute(
        "SELECT count(*) FROM runs WHERE job_id='job_0' AND json_extract(args,'$.shard')='RARE'"
    ).fetchone()[0]
    total_job = conn.execute("SELECT count(*) FROM runs WHERE job_id='job_0'").fetchone()[0]
    print(f"  only {total_match} matching rows exist across {total_job:,} job rows")
    print("  query plan:", " | ".join(str(p[-1]) for p in plan))
    verdict = (
        "blocks ~as long as a full scan regardless of page size"
        if ms > 50
        else "cheap here, but scan cost grows with table size"
    )
    print(f"  => a page over an unindexed filter {verdict}.\n")


def main():
    tmp = tempfile.mkdtemp(prefix="apsis_probe_")
    path = os.path.join(tmp, "p.db")
    print(f"temp DB: {path}\n")
    S.SqliteDB.create(path)
    db = S.SqliteDB.open(path)
    try:
        seed(db, N_RUNS)
        probe_with_args(db)
    finally:
        db.close()
    # A1 opens its own connections against the file after the app db is closed.
    probe_offloop(path)
    import shutil

    shutil.rmtree(tmp, ignore_errors=True)
    print(f"cleaned up {tmp}")


if __name__ == "__main__":
    main()
