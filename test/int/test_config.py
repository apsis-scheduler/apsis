import logging
import pytest

from ruamel.yaml.constructor import DuplicateKeyError
import apsis.config


def test_duplicate_key_in_config(tmp_path):
    """
    A config that contains the same key twice in the same mapping must raise `DuplicateKeyError`.
    """
    yaml_with_duplicate_db_key = """
    database:
        path: /first/path
        timeout: 120s
        path: /this/is/wrong/duplicate/key

    """

    cfg_file = tmp_path / "dup_key_cfg.yaml"
    cfg_file.write_text(yaml_with_duplicate_db_key)

    with pytest.raises(DuplicateKeyError):
        apsis.config.load(cfg_file)


def test_invalid_config_values_raise_errors(tmp_path, caplog):
    db_file = tmp_path / "test.db"
    db_file.touch()
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()
    negative_duration = "-30s"
    cfg_with_negative_durations = f"""
    database:
        path: {db_file}
        timeout: {negative_duration}
    waiting:
        max_time: 5m
    job_dir: {job_dir}
    """

    cfg_file = tmp_path / "invalid_config.yaml"
    cfg_file.write_text(cfg_with_negative_durations)

    with pytest.raises(ValueError) as exc:
        apsis.config.load(cfg_file)
    assert f"database.timeout has negative duration: {negative_duration}" in str(exc.value)

    invalid_signal = "INVALID_SIGNAL"
    cfg_with_invalid_signal = f"""
    database:
        path: {db_file}
        timeout: 30s
    waiting:
        max_time: 5m
    program:
        timeout:
            duration: 3h
            signal: {invalid_signal}
    job_dir: {job_dir}
    """

    cfg_file.write_text(cfg_with_invalid_signal)

    with pytest.raises(ValueError) as exc:
        apsis.config.load(cfg_file)
    assert f"not a signal: {invalid_signal}" in str(exc.value)


def test_missing_config_values_not_set_to_none(tmp_path, caplog):
    """
    Test that the config check does not set missing values to None.
    """
    db_file = tmp_path / "test.db"
    db_file.touch()

    job_dir = tmp_path / "jobs"
    job_dir.mkdir()

    simple_cfg = f"""
    database:
        path: {db_file}
        timeout: 30s
    job_dir: {job_dir}
    """

    cfg_file = tmp_path / "simple_cfg.yaml"
    cfg_file.write_text(simple_cfg)
    cfg = apsis.config.load(cfg_file)
    assert "program" not in cfg
    assert "waiting" not in cfg
    assert "procstar" not in cfg
