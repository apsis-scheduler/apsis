"""
Tests for parametrized dependencies.
"""

import pytest
from apsis.cond.dependency import Dependency
from apsis.jobs import Jobs, JobsDir
from apsis.runs import Instance, Run, template_expand, get_bind_args
from apsis.check import MockJobDb


def test_parametrized_dependency_binding():
    """Test that parametrized dependencies expand correctly during binding."""
    # Create mock jobs
    jobs = {
        "data_as": type('Job', (), {'job_id': 'data_as', 'params': frozenset(['date'])})(),
        "data_us": type('Job', (), {'job_id': 'data_us', 'params': frozenset(['date'])})(),
        "test_job": type('Job', (), {'job_id': 'test_job', 'params': frozenset(['date', 'strat'])})(),
    }
    
    jobs_dir = type('JobsDir', (), {'get_job': lambda self, job_id: jobs[job_id]})()
    jobs_store = Jobs(jobs_dir, MockJobDb())
    
    # Override the __getitem__ method to return our mock jobs
    def mock_getitem(job_id):
        if job_id in jobs:
            return jobs[job_id]
        raise LookupError(f"unknown job ID: {job_id}")
    
    jobs_store.__getitem__ = mock_getitem
    
    # Create a parametrized dependency
    dep = Dependency(job_id='data_{{strat}}', args={'date': '{{date}}'})
    
    # Test with 'as' strategy
    run_as = Run(Instance('test_job', {'date': '2023-01-01', 'strat': 'as'}))
    bound_dep_as = dep.bind(run_as, jobs_store)
    
    assert bound_dep_as.job_id == 'data_as'
    assert bound_dep_as.args == {'date': '2023-01-01'}
    
    # Test with 'us' strategy
    run_us = Run(Instance('test_job', {'date': '2023-01-01', 'strat': 'us'}))
    bound_dep_us = dep.bind(run_us, jobs_store)
    
    assert bound_dep_us.job_id == 'data_us'
    assert bound_dep_us.args == {'date': '2023-01-01'}


def test_template_expand_basic():
    """Test basic template expansion functionality."""
    template = 'data_{{strat}}'
    args = {'strat': 'as'}
    
    result = template_expand(template, args)
    assert result == 'data_as'
    
    args = {'strat': 'us'}
    result = template_expand(template, args)
    assert result == 'data_us'


def test_template_expand_multiple_params():
    """Test template expansion with multiple parameters."""
    template = 'data_{{region}}_{{strat}}'
    args = {'region': 'asia', 'strat': 'equity'}
    
    result = template_expand(template, args)
    assert result == 'data_asia_equity'


def test_template_expand_missing_param():
    """Test that missing parameters raise NameError."""
    template = 'data_{{missing_param}}'
    args = {'strat': 'as'}
    
    with pytest.raises(NameError, match="name 'missing_param' is not defined"):
        template_expand(template, args)


def test_non_parametrized_dependency_unchanged():
    """Test that non-parametrized dependencies work as before."""
    jobs = {
        "data_source": type('Job', (), {'job_id': 'data_source', 'params': frozenset(['date'])})(),
        "test_job": type('Job', (), {'job_id': 'test_job', 'params': frozenset(['date'])})(),
    }
    
    jobs_dir = type('JobsDir', (), {'get_job': lambda self, job_id: jobs[job_id]})()
    jobs_store = Jobs(jobs_dir, MockJobDb())
    
    def mock_getitem(job_id):
        if job_id in jobs:
            return jobs[job_id]
        raise LookupError(f"unknown job ID: {job_id}")
    
    jobs_store.__getitem__ = mock_getitem
    
    # Create a non-parametrized dependency
    dep = Dependency(job_id='data_source', args={'date': '2023-01-01'})
    
    run = Run(Instance('test_job', {'date': '2023-01-01'}))
    bound_dep = dep.bind(run, jobs_store)
    
    # Should remain unchanged
    assert bound_dep.job_id == 'data_source'
    assert bound_dep.args == {'date': '2023-01-01'}


def test_parametrized_dependency_validation():
    """Test that parametrized dependency validation works correctly."""
    from apsis.check import check_job
    from apsis.jobs import JobsDir
    from apsis.cond.dependency import Dependency
    
    # Create a mock job with parametrized dependency
    job = type('Job', (), {
        'job_id': 'test_job',
        'params': frozenset(['date', 'strat']),
        'schedules': [],
        'actions': [],
        'conds': [Dependency(job_id='data_{{strat}}', args={'date': '{{date}}'})],
    })()
    
    # Create empty jobs directory
    jobs_dir = type('JobsDir', (), {
        'get_job': lambda self, job_id: None,
        'get_jobs': lambda self: []
    })()
    
    # Check that valid template passes validation
    errors = list(check_job(jobs_dir, job))
    # Should not have template syntax errors
    template_errors = [e for e in errors if 'invalid template' in str(e)]
    assert len(template_errors) == 0
    
    # Test invalid template
    invalid_job = type('Job', (), {
        'job_id': 'invalid_job',
        'params': frozenset(['date', 'strat']),
        'schedules': [],
        'actions': [],
        'conds': [Dependency(job_id='data_{{missing_param}}', args={})],
    })()
    
    errors = list(check_job(jobs_dir, invalid_job))
    template_errors = [e for e in errors if 'invalid template' in str(e)]
    assert len(template_errors) > 0
    assert 'missing_param' in str(template_errors[0])