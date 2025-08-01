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


def test_invalid_config_values(tmp_path, caplog):
    db_file = tmp_path / "test.db"
    db_file.touch()
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()
    cfg_with_negative_durations = f"""
    database:
        path: {db_file}
        timeout: -30s
    waiting:
        max_time: -5m
    procstar:
        agent:
            connection:
                start_timeout: -2h
                reconnect_timeout: -15s
            run:
                update_interval: -2s
                output_interval: -1s
    program:
        timeout:
            duration: -3h
            signal: INVALID_SIGNAL
    job_dir: {job_dir}
    """

    cfg_file = tmp_path / "negative_duration_cfg.yaml"
    cfg_file.write_text(cfg_with_negative_durations)

    apsis.config.load(cfg_file)

    log_messages = [record.message for record in caplog.records if record.levelno == logging.ERROR]

    expected_duration_errors = 7
    expected_signal_errors = 1

    duration_error_count = sum(1 for msg in log_messages if "negative duration:" in msg)
    signal_error_count = sum(1 for msg in log_messages if "invalid signal:" in msg)

    assert duration_error_count == expected_duration_errors, (
        f"Expected {expected_duration_errors} duration errors, got {duration_error_count}"
    )
    assert signal_error_count == expected_signal_errors, (
        f"Expected {expected_signal_errors} signal errors, got {signal_error_count}"
    )


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
        timeout: -30s
    job_dir: {job_dir}
    """

    cfg_file = tmp_path / "simple_cfg.yaml"
    cfg_file.write_text(simple_cfg)
    cfg = apsis.config.load(cfg_file)
    assert "program" not in cfg
    assert "waiting" not in cfg
    assert "procstar" not in cfg
