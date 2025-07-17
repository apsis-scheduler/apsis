import argparse
import logging

# -------------------------------------------------------------------------------


class HelpFormatter(argparse.HelpFormatter):
    """
    An utterly hacked-up help formatter to make it look like I want it to look.
    """

    def format_help(self):
        help = self._root_section.format_help()
        help = self._long_break_matcher.sub("\n\n", help)
        return help

    class _Section(argparse.HelpFormatter._Section):
        def __init__(self, formatter, parent, heading=None):
            if heading not in {None, argparse.SUPPRESS}:
                heading = heading.capitalize()
            super().__init__(formatter, parent, heading)

    def _format_action(self, action):
        if isinstance(action, argparse._SubParsersAction):
            indent = " " * self._current_indent
            return "\n".join(
                f"{indent}{n:14s}{p.description}" for n, p in action._name_parser_map.items()
            )
        else:
            return super()._format_action(action)

    def _format_action_invocation(self, action):
        if action.option_strings:
            short_op = "  "
            long_op = ""
            for op_str in action.option_strings:
                if op_str.startswith("--"):
                    long_op = op_str
                else:
                    short_op = op_str
            fmt = short_op + " " + long_op
            if action.nargs != 0:
                fmt += " " + action.metavar
            return fmt
        else:
            return super()._format_action_invocation(action)

    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = "Usage:\n  "
        return (
            prefix
            + self._prog
            + " [ OPTIONS ] "
            + " ".join(
                "COMMAND ..." if isinstance(a, argparse._SubParsersAction) else a.metavar
                for a in actions
                if len(a.option_strings) == 0
            )
            + "\n\n"
        )


# -------------------------------------------------------------------------------


class CommandArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kw_args):
        kw_args.setdefault("usage", "%(prog)s [ OPTIONS ] COMMAND ...")
        kw_args.setdefault("formatter_class", HelpFormatter)
        super().__init__(*args, **kw_args)

        self.add_argument(
            "--log-level",
            metavar="LEVEL",
            default=None,
            choices={"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"},
            help="log at LEVEL",
        )

        self.__commands = self.add_subparsers(
            title="commands",
            parser_class=argparse.ArgumentParser,
        )
        self.set_defaults(cmd=lambda *a, **k: self.error("no command"))

    def add_command(self, name, fn, description=None):
        cmd = self.__commands.add_parser(
            name,
            formatter_class=self.formatter_class,
            description=description,
        )
        cmd.set_defaults(cmd=fn)
        return cmd

    def parse_args(self, *args, **kw_args):
        pargs = super().parse_args(*args, **kw_args)
        if pargs.log_level is not None:
            logging.getLogger().setLevel(getattr(logging, pargs.log_level))
        return pargs
