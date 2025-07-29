#!/usr/bin/env python
from argparse import ArgumentParser
import signal
import time
import sys

parser = ArgumentParser()
parser.add_argument("sleep", metavar="SECS", type=float)
parser.add_argument("--cleanup-time", type=float, default=1.0, help="Time to spend in cleanup")
args = parser.parse_args()


def sigterm_handler(signum, frame):
    sig = signal.Signals(signum)
    print(f"received {sig.name}, starting cleanup...")

    # Simulate slow cleanup process
    cleanup_start = time.time()
    while time.time() - cleanup_start < args.cleanup_time:
        print("cleaning up...")
        time.sleep(0.2)

    print("cleanup complete")
    sys.exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)

print(f"sleeping for {args.sleep} sec")
time.sleep(args.sleep)
print("completed normally")
