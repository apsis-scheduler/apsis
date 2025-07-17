"""
CLI for the Apsis Agent, the component responsible for running child
programs.

For debugging and maintenance only.
"""

import argparse
import asyncio
import json
import logging
import sys

from apsis.agent.client import Agent

# -------------------------------------------------------------------------------


async def _main():
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(name)-18s [%(levelname)-7s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log-level",
        metavar="LEVEL",
        default=None,
        choices={"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"},
        help="log at LEVEL [def: INFO]",
    )

    commands = parser.add_subparsers(title="commands")
    parser.set_defaults(cmd=None)

    parser.add_argument(
        "--host",
        metavar="HOST",
        default=None,
        help="run agent on remote HOST [def: local]",
    )
    parser.add_argument(
        "--user",
        metavar="USER",
        default=None,
        help="run agent as USER [def: this user]",
    )

    parser.set_defaults(connect=None)
    excl = parser.add_mutually_exclusive_group()
    excl.add_argument(
        "--no-connect",
        action="store_false",
        dest="connect",
        help="start only; fail if already running",
    )
    excl.add_argument(
        "--connect",
        action="store_true",
        dest="connect",
        help="reconnect only; fail if not running",
    )

    cmd = commands.add_parser("start")
    cmd.set_defaults(cmd="start")

    cmd = commands.add_parser("list")
    cmd.set_defaults(cmd="list")

    cmd = commands.add_parser("signal")
    cmd.add_argument("proc_id")
    cmd.add_argument("signal")
    cmd.set_defaults(cmd="signal")

    cmd = commands.add_parser("get")
    cmd.add_argument("proc_id")
    cmd.set_defaults(cmd="get")

    cmd = commands.add_parser("del")
    cmd.add_argument("proc_id")
    cmd.set_defaults(cmd="del")

    cmd = commands.add_parser("stop")
    cmd.set_defaults(cmd="stop")

    cmd = commands.add_parser("clean")
    cmd.set_defaults(cmd="clean")

    args = parser.parse_args()
    if args.log_level is not None:
        logging.getLogger(None).setLevel(getattr(logging, args.log_level))

    agent = Agent(host=args.host, user=args.user, connect=args.connect)

    try:
        if args.cmd is None:
            # Nothing to do.
            raise SystemExit(0)

        elif args.cmd == "start":
            result = await agent.is_running()

        elif args.cmd == "list":
            result = await agent.get_processes()

        elif args.cmd == "signal":
            result = await agent.signal(args.proc_id, args.signal)

        elif args.cmd == "get":
            result = await agent.get_process(args.proc_id)

        elif args.cmd == "del":
            result = await agent.del_process(args.proc_id)

        elif args.cmd == "stop":
            result = await agent.stop()

        elif args.cmd == "clean":

            async def clean(agent):
                processes = await agent.get_processes()
                return await asyncio.gather(*(agent.del_process(p["proc_id"]) for p in processes))

            result = await clean(agent)

    except RuntimeError as err:
        logging.error(f"{args.cmd} failed: {err}")

    else:
        try:
            json.dump(result, sys.stdout, indent=2)
            print()
        except TypeError:
            print(result)


def main():
    asyncio.run(_main())


# -------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
