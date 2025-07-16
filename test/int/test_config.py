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
