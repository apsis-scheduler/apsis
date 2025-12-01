from contextlib import closing
from pathlib import Path
import pytest

from procstar_instance import ApsisService

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "jobs"


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        with inst.agent(serve=True):
            yield inst


def test_run_action(inst):
    run_id = inst.client.schedule("with snapshot", {})["run_id"]
    inst.wait_run(run_id)
    log = inst.get_log_lines()

    # There should be a log line with the run ID.
    token = f"run ID: {run_id}"
    assert any(token in l for l in log)

    # There should be a log line with the output.
    assert any("output: Hello, world!" in l for l in log)


def test_check_label_action(inst):
    run_id = inst.client.schedule("check label", {})["run_id"]
    inst.wait_run(run_id)

    # Logs should show that the action started and raised.
    with inst.get_log() as log:
        log = list(log)
    assert not any("RuntimeError: run missing label" in l for l in log)


def test_check_label_error_action(inst):
    run_id = inst.client.schedule("check label error", {})["run_id"]
    inst.wait_run(run_id)

    # Logs should show that the action started and raised.
    with inst.get_log() as log:
        log = list(log)
    assert any("RuntimeError: run missing label: foo" in l for l in log)
