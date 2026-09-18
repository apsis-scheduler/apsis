import re

import pytest

from procstar_instance import ApsisService


@pytest.mark.parametrize(
    "args, expected",
    [
        (("now", "/usr/bin/printf", "%s\\n", "a b", "$HOME"), b"a b\n$HOME\n"),
        (("--shell", "now", "printf '%s\\n' \"$((2 + 3))\""), b"5\n"),
    ],
)
def test_adhoc_cli(args: tuple[str, ...], expected: bytes) -> None:
    with ApsisService() as svc, svc.agent():
        returncode, output = svc.run_apsis_cmd("adhoc", *args)
        assert returncode == 0, output.decode()
        match = re.search(rb"\brun (r\d+)\b", output)
        assert match is not None, output.decode()
        run_id = match[1].decode()
        assert svc.wait_run(run_id, timeout=10)["state"] == "success"
        assert svc.client.get_output(run_id, "output") == expected
