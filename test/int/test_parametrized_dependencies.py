"""
Integration tests for parametrized dependencies.
"""

from contextlib import closing
from pathlib import Path
import pytest

from instance import ApsisService

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "parametrized_deps" / "jobs"


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


@pytest.fixture
def client(inst):
    return inst.client


def test_parametrized_dependencies_integration(inst, client):
    """
    Integration test that schedules and runs jobs with parametrized dependencies.
    """
    DATE = "2023-01-01"
    
    # First, schedule the data source jobs
    res = client.schedule("data_as", {"date": DATE})
    data_as_run_id = res["run_id"]
    
    res = client.schedule("data_us", {"date": DATE})
    data_us_run_id = res["run_id"]
    
    # Wait for data jobs to complete
    res = inst.wait_run(data_as_run_id)
    assert res["state"] == "success"
    
    res = inst.wait_run(data_us_run_id)
    assert res["state"] == "success"
    
    # Now schedule the jobs with parametrized dependencies
    res = client.schedule("process_data", {"date": DATE, "region": "as"})
    process_as_run_id = res["run_id"]
    
    res = client.schedule("process_data", {"date": DATE, "region": "us"})
    process_us_run_id = res["run_id"]
    
    # Wait for processing jobs to complete
    res = inst.wait_run(process_as_run_id)
    assert res["state"] == "success"
    
    res = inst.wait_run(process_us_run_id)
    assert res["state"] == "success"
    
    # Verify that the dependencies were resolved correctly
    deps = client.get_run_dependencies(process_as_run_id)
    assert len(deps) == 1
    assert deps[0]["job_id"] == "data_as"
    assert deps[0]["args"]["date"] == DATE
    assert deps[0]["run_ids"] == [data_as_run_id]
    
    deps = client.get_run_dependencies(process_us_run_id)
    assert len(deps) == 1
    assert deps[0]["job_id"] == "data_us"
    assert deps[0]["args"]["date"] == DATE
    assert deps[0]["run_ids"] == [data_us_run_id]


def test_parametrized_dependencies_waiting(inst, client):
    """
    Test that jobs with parametrized dependencies wait for the correct dependency.
    """
    DATE = "2023-01-01"
    
    # Schedule a job with parametrized dependency before its dependency exists
    res = client.schedule("process_data", {"date": DATE, "region": "as"})
    process_run_id = res["run_id"]
    
    # The job should be waiting
    import time
    time.sleep(0.1)  # Give it a moment to process
    res = client.get_run(process_run_id)
    assert res["state"] == "waiting"
    
    # Now schedule the dependency
    res = client.schedule("data_as", {"date": DATE})
    data_run_id = res["run_id"]
    
    # Wait for the dependency to complete
    res = inst.wait_run(data_run_id)
    assert res["state"] == "success"
    
    # Now the dependent job should complete
    res = inst.wait_run(process_run_id)
    assert res["state"] == "success"
    
    # Verify the dependency was resolved correctly
    deps = client.get_run_dependencies(process_run_id)
    assert len(deps) == 1
    assert deps[0]["job_id"] == "data_as"
    assert deps[0]["run_ids"] == [data_run_id]


def test_multiple_parametrized_dependencies(inst, client):
    """
    Test a job with multiple parametrized dependencies.
    """
    DATE = "2023-01-01"
    
    # Schedule the dependencies
    res = client.schedule("data_as", {"date": DATE})
    data_as_run_id = res["run_id"]
    
    res = client.schedule("data_prod", {"date": DATE})
    data_prod_run_id = res["run_id"]
    
    # Wait for dependencies to complete
    res = inst.wait_run(data_as_run_id)
    assert res["state"] == "success"
    
    res = inst.wait_run(data_prod_run_id)
    assert res["state"] == "success"
    
    res = client.schedule("complex_process", {"date": DATE, "region": "as", "env": "prod"})
    complex_run_id = res["run_id"]
    
    res = inst.wait_run(complex_run_id)
    assert res["state"] == "success"
    
    deps = client.get_run_dependencies(complex_run_id)
    assert len(deps) == 2
    
    deps.sort(key=lambda x: x["job_id"])
    
    assert deps[0]["job_id"] == "data_as"
    assert deps[0]["run_ids"] == [data_as_run_id]
    
    assert deps[1]["job_id"] == "data_prod"
    assert deps[1]["run_ids"] == [data_prod_run_id]


def test_user_original_example(inst, client):
    DATE = "2023-01-01"
    
    res = client.schedule("data_as", {"date": DATE})
    data_as_run_id = res["run_id"]
    
    res = client.schedule("data_us", {"date": DATE})
    data_us_run_id = res["run_id"]
    
    # Wait for data jobs to complete
    res = inst.wait_run(data_as_run_id)
    assert res["state"] == "success"
    
    res = inst.wait_run(data_us_run_id)
    assert res["state"] == "success"
    
    # Now schedule the user's example job with different strategies
    res = client.schedule("user_example", {"date": DATE, "strat": "as"})
    user_as_run_id = res["run_id"]
    
    res = client.schedule("user_example", {"date": DATE, "strat": "us"})
    user_us_run_id = res["run_id"]
    
    # Wait for user example jobs to complete
    res = inst.wait_run(user_as_run_id)
    assert res["state"] == "success"
    
    res = inst.wait_run(user_us_run_id)
    assert res["state"] == "success"
    
    # Verify that the dependencies were resolved correctly
    deps = client.get_run_dependencies(user_as_run_id)
    assert len(deps) == 1
    assert deps[0]["job_id"] == "data_as"
    assert deps[0]["run_ids"] == [data_as_run_id]
    
    deps = client.get_run_dependencies(user_us_run_id)
    assert len(deps) == 1
    assert deps[0]["job_id"] == "data_us"
    assert deps[0]["run_ids"] == [data_us_run_id]