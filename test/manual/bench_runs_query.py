"""
Phase 0 experiment for iac#2129 (paginate /runs).

Measures, against a THROWAWAY temp SQLite DB (no production touched):
  1. baseline: full unpaged query + decode at several result sizes
     (~= how long the event loop is blocked today),
  2. where the time goes: SQL fetch vs. Python decode,
  3. batched keyset prototype (rowid < cursor ORDER BY rowid DESC LIMIT B)
     swept over batch sizes, reporting per-batch p50/p95/max + total.

Run:  python test/manual/bench_runs_query.py [N_RUNS]
"""

import os
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))

import ora
import sqlalchemy as sa
import ujson

import apsis.sqlite as S
from apsis.program.noop import NoOpProgram
from apsis.runs import Instance, Run
from apsis.states import State

N_RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
N_JOBS = 5
BASELINE_SIZES = [1_000, 10_000, 50_000, 100_000]
BATCH_SIZES = [100, 250, 500, 1_000, 2_000, 5_000]
LOOP_TARGET_MS = 100.0


def _template_columns():
    """
    Build realistic JSON column blobs once from a real Run, so decode cost is
    representative (wide payloads round-trip through from_jso).
    """
    inst = Instance("job_0", {"date": "2026-09-03", "instance": "prod", "shard": "07"})
    run = Run(inst)
    run.run_id = "r1"
    run.timestamp = ora.now()
    run.state = State.success
    run.program = NoOpProgram()
    run.conds = None
    run.actions = None
    run.times = {"schedule": ora.now(), "running": ora.now(), "success": ora.now()}
    # Wide, representative meta/run_state (this is what dominates decode).
    run.meta = {
        "program": {"pid": 12345, "host": "compute-node-42.global.tudor.com"},
        "elapsed": 12.34,
        "outputs": {"output": {"length": 4096, "content_type": "text/plain"}},
        "labels": ["prod", "eod", "batch"],
        "notes": "seeded run for pagination benchmark " * 4,
    }
    run.run_state = {"procstar": {"conn_id": "abc-123-def-456", "proc_id": "p-987654"}}

    program = ujson.dumps(run.program.to_jso())
    times = ujson.dumps({n: str(t) for n, t in run.times.items()})
    meta = ujson.dumps(run.meta)
    run_state = ujson.dumps(run.run_state)
    return program, times, meta, run_state


def seed(db, n):
    program, times, meta, run_state = _template_columns()
    states = [s.name for s in (State.success, State.failure, State.running, State.error)]
    conn = db.conn
    base = ora.now() - n  # spread timestamps ~1s apart going back
    rows = []
    for i in range(1, n + 1):
        rowid = i
        job_id = f"job_{i % N_JOBS}"
        args = ujson.dumps({"date": "2026-09-03", "instance": "prod", "shard": f"{i % 100:02d}"})
        ts = S.dump_time(base + i)
        rows.append(
            (
                f"r{rowid}",
                ts,
                job_id,
                args,
                states[i % len(states)],
                program,
                None,  # conds
                None,  # actions
                times,
                meta,
                run_state,
                0,  # expected
                rowid,
            )
        )
    t0 = time.perf_counter()
    conn.executemany(
        """INSERT INTO runs
           (run_id, timestamp, job_id, args, state, program, conds, actions,
            times, meta, run_state, expected, rowid)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    print(
        f"seeded {n:,} runs in {time.perf_counter() - t0:.1f}s "
        f"({N_JOBS} jobs, ~{n // N_JOBS:,} per job)\n"
    )


def _decode_row(row):
    """Mirror RunDB.__query_runs decode work for one row."""
    (
        rowid,
        run_id,
        timestamp,
        job_id,
        args,
        state,
        program,
        conds,
        actions,
        times,
        meta,
        run_state,
    ) = row
    program = None if program is None else S.Program.from_jso(ujson.loads(program))
    conds = None if conds is None else [S.Condition.from_jso(c) for c in ujson.loads(conds)]
    actions = None if actions is None else [S.Action.from_jso(a) for a in ujson.loads(actions)]
    times = {n: ora.Time(t) for n, t in ujson.loads(times).items()}
    args = ujson.loads(args)
    inst = Instance(job_id, args)
    run = Run(inst)
    run.run_id = run_id
    run.timestamp = S.load_time(timestamp)
    run.state = State[state]
    run.program = program
    run.conds = conds
    run.actions = actions
    run.times = times
    run.meta = ujson.loads(meta)
    run.run_state = ujson.loads(run_state)
    run._rowid = rowid
    return run


SELECT_COLS = (
    "rowid, run_id, timestamp, job_id, args, state, program, conds, actions, times, meta, run_state"
)


def baseline(db, job_id, size):
    """Full unpaged fetch + decode for `size` rows of one job. Splits fetch vs decode."""
    conn = db.conn
    # fetch
    t0 = time.perf_counter()
    cur = conn.execute(
        f"SELECT {SELECT_COLS} FROM runs WHERE job_id = ? ORDER BY rowid DESC LIMIT ?",
        (job_id, size),
    )
    fetched = cur.fetchall()
    t_fetch = time.perf_counter() - t0
    # decode
    t1 = time.perf_counter()
    runs = [_decode_row(r) for r in fetched]
    t_decode = time.perf_counter() - t1
    return len(runs), t_fetch, t_decode


def batched(db, job_id, batch):
    """Keyset paginate one job's runs; time each batch's fetch+decode."""
    conn = db.conn
    cursor_rowid = None
    per_batch = []
    total = 0
    while True:
        t0 = time.perf_counter()
        if cursor_rowid is None:
            cur = conn.execute(
                f"SELECT {SELECT_COLS} FROM runs WHERE job_id = ? ORDER BY rowid DESC LIMIT ?",
                (job_id, batch),
            )
        else:
            cur = conn.execute(
                f"SELECT {SELECT_COLS} FROM runs WHERE job_id = ? AND rowid < ? "
                f"ORDER BY rowid DESC LIMIT ?",
                (job_id, cursor_rowid, batch),
            )
        rows = cur.fetchall()
        runs = [_decode_row(r) for r in rows]
        per_batch.append((time.perf_counter() - t0) * 1000.0)  # ms
        if not rows:
            break
        total += len(runs)
        cursor_rowid = runs[-1]._rowid
        if len(rows) < batch:
            break
    return total, per_batch


def _median_of(fn, repeats=3):
    """Run fn repeats times, drop the first (warm cache), return median result-tuple."""
    results = [fn() for _ in range(repeats)]
    return results[1:]  # warm runs only


def main():
    tmp = tempfile.mkdtemp(prefix="apsis_bench_")
    path = os.path.join(tmp, "bench.db")
    print(f"temp DB: {path}\n")
    S.SqliteDB.create(path)
    db = S.SqliteDB.open(path)
    try:
        seed(db, N_RUNS)
        job_id = "job_0"  # indexed path (job_id filter)

        print("=== BASELINE: full unpaged fetch + decode (warm cache, median of 2) ===")
        print(f"{'rows':>8} | {'fetch ms':>9} | {'decode ms':>10} | {'total ms':>9}")
        print("-" * 46)
        for size in BASELINE_SIZES:
            if size > N_RUNS // N_JOBS:
                continue
            warm = _median_of(lambda: baseline(db, job_id, size))
            n = warm[0][0]
            fetch_ms = statistics.median(w[1] for w in warm) * 1000
            decode_ms = statistics.median(w[2] for w in warm) * 1000
            print(f"{n:>8,} | {fetch_ms:>9.1f} | {decode_ms:>10.1f} | {fetch_ms + decode_ms:>9.1f}")

        print("\n=== BATCHED: keyset pagination, per-batch fetch+decode (warm, median of 2) ===")
        print(
            f"{'batch':>6} | {'#batches':>8} | {'p50 ms':>7} | {'p95 ms':>7} | "
            f"{'max ms':>7} | {'total ms':>9} | {'runs/s':>9} | under {int(LOOP_TARGET_MS)}ms?"
        )
        print("-" * 88)
        for batch in BATCH_SIZES:
            warm = _median_of(lambda: batched(db, job_id, batch))
            total = warm[0][0]
            # aggregate per-batch times across warm runs
            all_batch_ms = [ms for w in warm for ms in w[1]]
            p50 = statistics.median(all_batch_ms)
            p95 = sorted(all_batch_ms)[int(len(all_batch_ms) * 0.95)] if all_batch_ms else 0
            mx = max(all_batch_ms)
            total_ms = statistics.median(sum(w[1]) for w in warm)
            throughput = total / (total_ms / 1000) if total_ms else 0
            ok = "yes" if mx < LOOP_TARGET_MS else "NO"
            n_batches = len(warm[0][1])
            print(
                f"{batch:>6} | {n_batches:>8} | {p50:>7.1f} | {p95:>7.1f} | "
                f"{mx:>7.1f} | {total_ms:>9.1f} | {throughput:>9,.0f} | {ok}"
            )

        print(
            "\nDecision rule: pick the largest batch whose max per-batch time "
            f"< {int(LOOP_TARGET_MS)} ms."
        )
        print(
            "NOTE: warm-cache numbers (can't drop OS page cache without root); "
            "real cold scans are slower."
        )
    finally:
        db.close()
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)
        print(f"\ncleaned up {tmp}")


if __name__ == "__main__":
    main()
