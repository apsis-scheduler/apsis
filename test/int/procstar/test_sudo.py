import json
from pathlib import Path

from apsis.lib.json import expand_dotted_keys
from apsis.program.procstar.agent import SUDO_ARGV_DEFAULT
from procstar_instance import ApsisService

# -------------------------------------------------------------------------------

# Tell Apsis to use our mock sudo instead of the real one.
MOCK_SUDO = Path(__file__).parent / "mock_sudo"
CFG = expand_dotted_keys({"procstar.agent.sudo.argv": [str(MOCK_SUDO)] + SUDO_ARGV_DEFAULT[1:]})


def test_sudo_program():
    """
    Tests a Procstar agent program with a sudo user.
    """
    ARGV = ["/usr/bin/echo", "Hello, world!"]

    with ApsisService(cfg=CFG) as svc, svc.agent():
        run_id = svc.client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "procstar",
                    "argv": ARGV,
                    "sudo_user": "testuser",
                }
            },
        )["run_id"]
        res = svc.wait_run(run_id)
        out = svc.client.get_output(run_id, "output")

        assert res["state"] == "success"
        out = json.loads(out)
        assert out["preserve_env"] is True
        assert out["non_interactive"] is True
        assert out["user"] == "testuser"
        assert out["argv"] == ARGV


def test_sudo_shell_program():
    """
    Tests a Procstar agent shell program with a sudo user.
    """
    COMMAND = "echo 'Hello, world!'"

    with ApsisService(cfg=CFG) as svc, svc.agent():
        run_id = svc.client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "procstar-shell",
                    "command": COMMAND,
                    "sudo_user": "usermock",
                }
            },
        )["run_id"]
        res = svc.wait_run(run_id)
        out = svc.client.get_output(run_id, "output")

        assert res["state"] == "success"
        out = json.loads(out)
        assert out["preserve_env"] is True
        assert out["non_interactive"] is True
        assert out["user"] == "usermock"
        assert out["argv"] == ["/usr/bin/bash", "-c", COMMAND]
