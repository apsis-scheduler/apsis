"""
Control CLI, for maintenance and running the service.
"""

import logging
import os
from   pathlib import Path
import sys

import apsis.cmdline
import apsis.config
import apsis.lib.argparse
import apsis.lib.json
import apsis.lib.logging
from   apsis.service import DEFAULT_PORT
import apsis.service.client
import apsis.service.main
import apsis.sqlite
from   apsis.sqlite import SqliteDB

log = logging.getLogger(__name__)

#-------------------------------------------------------------

def main():
    apsis.lib.logging.configure()

    parser = apsis.lib.argparse.CommandArgumentParser(prog="apsisctl")
    addr = apsis.service.client.get_address()
    parser.add_argument(
        "--host", metavar="HOST", default=addr.host,
        help=f"connect to HOST [def: {addr.host}]")
    parser.add_argument(
        "--port", metavar="PORT", default=addr.port,
        help=f"connect to PORT [def: {addr.port}]")

    def get_client(args):
        return apsis.service.client.Client((args.host, args.port))


    #-------------------------------------------------------------
    # command: check-db

    def cmd_check_db(args):
        db = SqliteDB.open(args.db)

        apsis.sqlite.check(db)


    cmd = parser.add_command(
        "check-db", cmd_check_db,
        description="Checks DB for consistency.")
    cmd.add_argument(
        "db", metavar="DBPATH",
        help="path to Apsis database")

    #-------------------------------------------------------------
    # command: check-jobs

    def cmd_check_jobs(args):
        status = 0
        try:
            for err in apsis.jobs.check_job_dir(args.path):
                con.print(err, style="error")
                status = 1
        except NotADirectoryError as exc:
            parser.error(exc)
        return status


    cmd = parser.add_command(
        "check-jobs", cmd_check_jobs,
        description="Checks jobs in a directory.")
    cmd.add_argument(
        "path", metavar="DIR", type=Path,
        help="check the jobs in DIR")
    cmd.add_argument(
        "--config", metavar="CFGFILE", nargs="?", type=Path, default=None,
        help="read config from CFGFILE")

    #-------------------------------------------------------------
    # command: create

    def cmd_create(args):
        SqliteDB.create(args.state_path)


    cmd = parser.add_command(
        "create", cmd_create,
        description="Initializes an Apsis state file.")
    cmd.add_argument(
        "state_path", metavar="PATH",
        help="state file")

    #-------------------------------------------------------------
    # command: reload_jobs

    def cmd_reload_jobs(args):
        result = get_client(args).reload_jobs(dry_run=args.dry_run)

        if not args.quiet:
            def section(name):
                job_ids = result[name]
                if len(job_ids) > 0:
                    con.print(f"[u]Jobs {name}[/]")
                    for job_id in job_ids:
                        con.print(f"- [job]{job_id}[/]")
                    con.print()
                return len(job_ids)

            if section("removed") + section("added") + section("changed") == 0:
                con.print("no changes")
            elif result["dry_run"]:
                con.print("[warning]Dry run; no changes made.[/]")


    cmd = parser.add_command(
        "reload-jobs", cmd_reload_jobs,
        description="Reloads jobs from the jobs dir.")
    cmd.add_argument(
        "--dry-run", action="store_true", default=False,
        help="determine job changes but don't apply them")
    cmd.add_argument(
        "--quiet", action="store_true", default=False,
        help="don't print job changes")

    #-------------------------------------------------------------
    # command: rename-job

    def _replace_occurrences(file_path, old_value, new_value):
        with open(file_path, 'r') as file:
            lines = file.readlines()

        modified = any(old_value in line for line in lines)

        if not modified:
            return

        new_lines = [line.replace(old_value, new_value) for line in lines]
        with open(file_path, 'w') as file:
            file.writelines(new_lines)


    def cmd_rename_job_and_dependencies(args):
        old_path = args.old_path.with_suffix(".yaml")
        new_path = args.new_path.with_suffix(".yaml")

        if not old_path.is_file():
            raise FileNotFoundError(f"Invalid argument: OLD_JOB_PATH does not exist: '{old_path}'.")

        if new_path.is_file():
            raise FileExistsError(f"Invalid argument: NEW_JOB_PATH already exists: '{new_path}'.")

        new_path.parent.mkdir(parents=True, exist_ok=True)
        old_path.rename(new_path)

        old_id = str(old_path.with_suffix(""))
        new_id = str(new_path.with_suffix(""))

        cur_dir = Path.cwd()
        if cur_dir.name != "jobs":
            logging.warning(f"Please, make sure to be at your jobs dir root to find all dependencies: {cur_dir}")

        for root, _, files in os.walk(cur_dir):
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix != '.yaml':
                    continue
                _replace_occurrences(file_path, old_id, new_id)

            
    cmd = parser.add_command(
        "rename-job", cmd_rename_job_and_dependencies,
        description="Renames a job and all the dependencies to that job starting from current dir.")
    cmd.add_argument(
        "old_path", metavar="OLD_JOB_PATH", type=Path,
        help="Path of the job to be renamed.")
    cmd.add_argument(
        "new_path", metavar="NEW_JOB_PATH", type=Path,
        help="The new path to use.")
    
    #-------------------------------------------------------------
    # command: restart

    def cmd_restart(args):
        get_client(args).shut_down(restart=True)


    cmd = parser.add_command(
        "restart", cmd_restart,
        description="Restarts the Apsis service.")

    #-------------------------------------------------------------
    # command: serve

    def cmd_serve(args):
        cfg = apsis.config.load(args.config)
        for ovr in args.override:
            name, val = ovr.split("=", 1)
            apsis.lib.json.set_dotted(cfg, name, val)

        restart = apsis.service.main.serve(
            cfg, host=args.host, port=args.port, debug=args.debug)

        if restart:
            # Start all over.
            argv = [sys.executable, *sys.argv]
            log.info(f"restarting: {' '.join(argv)}")
            log.info("...")
            os.execv(argv[0], argv)


    cmd = parser.add_command(
        "serve", cmd_serve,
        description="Runs the Apsis service.")

    cmd.add_argument(
        "--debug", action="store_true", default=False,
        help="run in debug mode")
    # FIXME: Host, port collide with global options.
    # FIXME: Can't use localhost on OSX, where it resolves to an IPV6 address,
    # until we pick up this Sanic fix:
    # https://github.com/channelcat/sanic/pull/1053
    cmd.add_argument(
        "--host", metavar="HOST", default="127.0.0.1",
        help="server host address")
    cmd.add_argument(
        "--port", metavar="PORT", type=int, default=DEFAULT_PORT,
        help=f"server port [def: {DEFAULT_PORT}]")
    cmd.add_argument(
        "--config", metavar="CFGFILE", nargs="?", type=Path, default=None,
        help="read config from CFGFILE")
    cmd.add_argument(
        "--override", "-o", metavar="NAME=VAL", action="append", default=[],
        help="override dotted NAME as VAL in config")


    #-------------------------------------------------------------
    # command: shut_down

    def cmd_shut_down(args):
        get_client(args).shut_down()


    cmd = parser.add_command(
        "shut-down", cmd_shut_down,
        description="Shuts down the Apsis service.")

    #-------------------------------------------------------------
    # command: version

    def cmd_version(args):
        version = get_client(args).version()["version"]
        con.print(f"server address: {addr.host}:{addr.port}")
        con.print(f"server version: {version}")
        con.print(f"client version: {apsis.__version__}")


    cmd = parser.add_command(
        "version", cmd_version,
        description="Prints the version information.")

    #-------------------------------------------------------------

    args = parser.parse_args()
    con = apsis.cmdline.get_console()

    try:
        status = args.cmd(args)
    except apsis.service.client.APIError as err:
        apsis.cmdline.print_api_error(err, con)
        raise SystemExit(1)
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    else:
        raise SystemExit(0 if status is None else status)


#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

