import ora
import random

from apsis.runs import Instance, Run, RunStore

# -------------------------------------------------------------------------------


class MockRunDb:
    def __init__(self, runs=()):
        self.__runs = runs

    def query(self, min_timestamp=None):
        return iter(self.__runs)

    def upsert(self, run):
        pass


class MockRunIdDb:
    def __init__(self):
        self.i = 0

    def get_next_run_id(self):
        self.i += 1
        return f"r{self.i:06d}"


class MockDb:
    def __init__(self, runs=()):
        self.run_db = MockRunDb(runs)
        self.next_run_id_db = MockRunIdDb()


# -------------------------------------------------------------------------------


def test_runs_by_job():
    rnd = random.Random(0)
    n = 10000

    job_ids = [f"job{i:02d}" for i in range(100)]
    rnd.shuffle(job_ids)

    # Create a random run store and add a bunch of runs.
    run_store = RunStore(MockDb(), min_timestamp=ora.now())
    for _ in range(n):
        run = Run(Instance(rnd.choice(job_ids), {}), expected=True)
        run_store.add(run)

    query = lambda *a, **k: run_store.query(*a, **k)[1]

    run_ids = [r.run_id for r in query()]
    assert len(run_ids) == n

    # Confirm that they are all available by job.
    runs = query()
    for run in runs:
        assert run in query(job_id=run.inst.job_id)

    # Now remove some runs.
    for run_id in rnd.sample(run_ids, len(run_ids) // 5):
        run_store.remove(run_id)
        run_ids.remove(run_id)

    assert set(r.run_id for r in query()) == set(run_ids)

    # Confirm that they are all available by job.
    for run_id in run_ids:
        _, run = run_store.get(run_id)
        assert run in query(job_id=run.inst.job_id)

    # Confirm that no extraneous jobs are left.
    r = set.union(*(set(r.run_id for r in query(job_id=j)) for j in job_ids))
    assert r == set(run_ids)

    # Now remove some more runs.
    for run_id in rnd.sample(run_ids, len(run_ids) // 4):
        run_store.remove(run_id)
        run_ids.remove(run_id)

    assert set(r.run_id for r in query()) == set(run_ids)

    # Confirm that they are all available by job.
    for run_id in run_ids:
        _, run = run_store.get(run_id)
        assert run in query(job_id=run.inst.job_id)

    # Confirm that no extraneous jobs are left.
    r = set.union(*(set(r.run_id for r in query(job_id=j)) for j in job_ids))
    assert r == set(run_ids)


def test_run_store_populate():
    """
    Tests a RunStore populated from existing runs.

    Existing runs come from a mocked run DB.
    """
    rnd = random.Random(0)
    n = 10000

    job_ids = [f"job{i:02d}" for i in range(100)]
    rnd.shuffle(job_ids)

    # Since we're not calling run_store.add(), we have to assign run IDs.
    run_ids = MockRunIdDb()

    def make_run():
        run = Run(Instance(rnd.choice(job_ids), {}), expected=True)
        run.run_id = run_ids.get_next_run_id()
        return run

    runs = [make_run() for _ in range(n)]
    run_ids = {r.run_id for r in runs}
    assert len(run_ids) == len(runs)

    # Create a random run store and add a bunch of runs.
    run_store = RunStore(MockDb(runs), min_timestamp=ora.now())

    # Query each job by run ID.
    for run in runs:
        assert list(run_store.query(run_ids=run.run_id)[1]) == [run]

    # Query jobs by job ID.
    for job_id in job_ids:
        q = set(run_store.query(job_id=job_id)[1])
        assert q == {r for r in runs if r.inst.job_id == job_id}

    # Now remove some runs.
    for run in rnd.sample(runs, len(runs) // 4):
        run_store.remove(run.run_id)
        runs.remove(run)

    # Query each job by run ID.
    for run in runs:
        assert list(run_store.query(run_ids=run.run_id)[1]) == [run]

    # Query jobs by job ID.
    for job_id in job_ids:
        q = set(run_store.query(job_id=job_id)[1])
        assert q == {r for r in runs if r.inst.job_id == job_id}

    # Now remove the rest of the runs.
    for run in runs:
        run_store.remove(run.run_id)

    # All queries should be empty.
    assert len(list(run_store.query()[1])) == 0
    for run_id in run_ids:
        assert len(run_store.query(run_ids=run_id)[1]) == 0
    for job_id in job_ids:
        assert len(run_store.query(job_id=job_id)[1]) == 0
