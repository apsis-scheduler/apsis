from apsis.lib.api import job_to_jso, run_to_summary_jso

# -------------------------------------------------------------------------------


def make_agent_conn(conn):
    return {
        "type": "agent_conn",
        "conn": conn.to_jso(),
    }


def make_agent_conn_delete(conn_id):
    return {
        "type": "agent_conn_delete",
        "conn_id": conn_id,
    }


def make_job(job, jobs=None):
    return {
        "type": "job",
        "job": job_to_jso(job, jobs=jobs),
    }


def make_job_add(job, jobs=None):
    return {
        "type": "job_add",
        "job": job_to_jso(job, jobs=jobs),
    }


def make_job_delete(job_id):
    return {
        "type": "job_delete",
        "job_id": job_id,
    }


def make_run_delete(run):
    return {
        "type": "run_delete",
        "run_id": run.run_id,
    }


def make_run_summary(run):
    return {
        "type": "run_summary",
        "run_summary": run_to_summary_jso(run),
    }


def make_run_transition(run):
    return {
        "type": "run_transition",
        "run_summary": run_to_summary_jso(run),
    }
