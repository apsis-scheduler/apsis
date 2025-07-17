import apsis.actions
import apsis.lib.json
from apsis.lib.py import tupleize
from apsis.lib import email

# -------------------------------------------------------------------------------

# FIXME: jinja2?
TEMPLATE = """<!doctype html>
<html>
<head>
  <title>{subject}</title>
</head>
<body>
<p>
  program: <code>{program}</code>
</p>
<pre>{output}</pre>
</body>
</html>
"""


class EmailAction:
    """
    Action that sends an HTML email summarizing the run.
    """

    def __init__(self, to=(), *, from_=None, condition=None):
        self.__to = tupleize(to)
        self.__from = from_
        self.__condition = condition

    @classmethod
    def from_jso(cls, jso):
        with apsis.lib.json.check_schema(jso) as pop:
            to = pop("to", str)
            from_ = pop("from", str)
            cnd = pop("if", apsis.actions.Condition.from_jso, default=None)
        return cls(to, from_=from_, condition=cnd)

    def to_jso(self):
        cnd = None if self.__condition is None else self.__condition.to_jso()
        return {
            "to": list(self.__to),
            "from": self.__from,
            "if": cnd,
        }

    async def __call__(self, apsis, run):
        if self.__condition is not None and not self.__condition(run):
            return

        subject = f"Apsis {run.run_id}: {run.inst}: {run.state.name}"

        program = str(run.program)
        output_meta = apsis.outputs.get_metadata(run.run_id)
        if "output" in output_meta:
            output = apsis.outputs.get_data(run.run_id, "output").decode()
        else:
            output = ""
        body = TEMPLATE.format(**locals())

        smtp_cfg = apsis.cfg.get("smtp", {})
        email.send_html(self.__to, subject, body, from_=self.__from, smtp_cfg=smtp_cfg)
