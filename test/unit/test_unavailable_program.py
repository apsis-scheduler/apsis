"""Unit tests for UnavailableProgram rollback safety net."""

import pytest

from apsis.exc import SchemaError
from apsis.program import Program
from apsis.program.base import (
    UnavailableProgram,
    _UnavailableRunningProgram,
    ProgramError,
)


@pytest.fixture
def rollbackable_types(monkeypatch):
    """Override Program.ROLLBACKABLE_PROGRAM_TYPES for testing."""
    monkeypatch.setattr(
        Program,
        "ROLLBACKABLE_PROGRAM_TYPES",
        frozenset(("apsis.program.future.RolledBackProgram",)),
    )


class TestUnavailableProgram:
    def test_normal_program_loading(self):
        """Test that normal program types still load correctly."""
        # Test a known program type
        jso = {
            "type": "no-op",
            "duration": "10",
            "success": True,
        }
        program = Program.from_jso(jso)
        assert program.__class__.__name__ == "NoOpProgram"
        assert program.duration == "10"
        assert program.success is True

    def test_unknown_program_type_raises_error(self):
        jso = {
            "type": "completely.unknown.program.Type",
            "some": "data",
        }
        with pytest.raises(SchemaError) as exc_info:
            Program.from_jso(jso)
        assert "unknown program type: completely.unknown.program.Type" in str(exc_info.value)

    def test_rollbackable_unavailable_program_loads(self, rollbackable_types):
        jso = {
            "type": "apsis.program.future.RolledBackProgram",
            "argv": ["/usr/bin/python", "script.py"],
            "mem_gb": 4,
            "vcpu": 2,
            "disk_gb": 20,
        }

        # Should return UnavailableProgram, not raise an error
        program = Program.from_jso(jso)
        assert isinstance(program, UnavailableProgram)
        assert program.type_name == "apsis.program.future.RolledBackProgram"

    def test_unavailable_program_preserves_data(self, rollbackable_types):
        original_jso = {
            "type": "apsis.program.future.RolledBackProgram",
            "argv": ["/usr/bin/python", "script.py"],
            "mem_gb": 4,
            "vcpu": 2,
            "disk_gb": 20,
            "custom_field": "preserved",
        }

        program = Program.from_jso(original_jso.copy())
        assert isinstance(program, UnavailableProgram)

        # to_jso should return the original data
        restored_jso = program.to_jso()
        assert restored_jso == original_jso

    def test_unavailable_program_bind_raises_error(self, rollbackable_types):
        jso = {
            "type": "apsis.program.future.RolledBackProgram",
            "argv": ["/usr/bin/python", "script.py"],
        }

        program = Program.from_jso(jso)
        assert isinstance(program, UnavailableProgram)

        # Attempting to bind should raise ProgramError
        with pytest.raises(ProgramError) as exc_info:
            program.bind({"arg": "value"})
        assert "cannot bind unavailable program type" in str(exc_info.value)

    def test_unavailable_program_run_returns_error_runner(self, rollbackable_types):
        jso = {
            "type": "apsis.program.future.RolledBackProgram",
            "argv": ["/usr/bin/python", "script.py"],
        }

        program = Program.from_jso(jso)
        assert isinstance(program, UnavailableProgram)

        # Running should return _UnavailableRunningProgram
        runner = program.run("test_run_id", {})
        assert isinstance(runner, _UnavailableRunningProgram)
        assert runner.type_name == "apsis.program.future.RolledBackProgram"

    def test_unavailable_program_connect_returns_error_runner(self, rollbackable_types):
        jso = {
            "type": "apsis.program.future.RolledBackProgram",
            "argv": ["/usr/bin/python", "script.py"],
        }

        program = Program.from_jso(jso)
        assert isinstance(program, UnavailableProgram)

        # Connecting should return _UnavailableRunningProgram
        runner = program.connect("test_run_id", {"some": "state"}, {})
        assert isinstance(runner, _UnavailableRunningProgram)
        assert runner.type_name == "apsis.program.future.RolledBackProgram"

    def test_unavailable_program_str_representation(self, rollbackable_types):
        jso = {
            "type": "apsis.program.future.RolledBackProgram",
            "argv": ["/usr/bin/python", "script.py"],
        }

        program = Program.from_jso(jso)
        assert isinstance(program, UnavailableProgram)
        assert str(program) == "UnavailableProgram(apsis.program.future.RolledBackProgram)"

    @pytest.mark.asyncio
    async def test_unavailable_running_program_yields_error(self):
        runner = _UnavailableRunningProgram("test_run_id", "some.unavailable.Type")

        # The updates generator should yield a ProgramError
        updates = runner.updates
        result = await updates.__anext__()

        assert isinstance(result, ProgramError)
        assert "cannot run unavailable program type: some.unavailable.Type" in str(result)

        # Should only yield one error and then stop
        with pytest.raises(StopAsyncIteration):
            await updates.__anext__()

    def test_multiple_rollbackable_types(self, monkeypatch):
        monkeypatch.setattr(
            Program,
            "ROLLBACKABLE_PROGRAM_TYPES",
            frozenset(
                [
                    "apsis.program.future.RolledBackProgram",
                    "another.future.feature.BoundProgram",
                ]
            ),
        )

        # Both should return UnavailableProgram
        jso1 = {"type": "apsis.program.future.RolledBackProgram"}
        program1 = Program.from_jso(jso1)
        assert isinstance(program1, UnavailableProgram)

        jso2 = {"type": "another.future.feature.BoundProgram"}
        program2 = Program.from_jso(jso2)
        assert isinstance(program2, UnavailableProgram)

        # Unknown type should still raise error
        jso3 = {"type": "not.in.rollbackable.list"}
        with pytest.raises(SchemaError):
            Program.from_jso(jso3)

    def test_missing_type_key_raises_error(self):
        jso = {
            "argv": ["/usr/bin/python", "script.py"],
            # Missing 'type' key
        }
        with pytest.raises(SchemaError) as exc_info:
            Program.from_jso(jso)
        assert "missing type" in str(exc_info.value)

    def test_from_jso_preserves_all_fields(self, rollbackable_types):
        """Test that all fields are preserved through serialization/deserialization."""
        jso = {
            "type": "apsis.program.future.RolledBackProgram",
            "argv": ["/usr/bin/python", "script.py", "--verbose"],
            "stop": {"signal": "SIGTERM", "grace_period": 30},
            "timeout": {"duration": 3600, "signal": "SIGTERM"},
            "mem_gb": 8.5,
            "vcpu": 2.5,
            "disk_gb": 100,
            "role": "my-task-role",
            "task_definition": "my-task-def",
            "custom_metadata": {"key": "value"},
        }

        program = Program.from_jso(jso.copy())
        assert isinstance(program, UnavailableProgram)

        # All fields should be preserved
        restored = program.to_jso()
        assert restored == jso

    def test_ecs_program_loads_normally_when_available(self):
        """Test that ECS program loads normally when it's available in the codebase."""
        # The ECS program is actually available, so it should load normally
        jso = {
            "type": "procstar-ecs",
            "argv": ["/usr/bin/python", "script.py"],
            "stop": {"signal": "SIGTERM"},
        }

        program = Program.from_jso(jso)
        # It should load as the actual ECS program, not UnavailableProgram
        assert program.__class__.__name__ == "ProcstarECSProgram"
        assert not isinstance(program, UnavailableProgram)
