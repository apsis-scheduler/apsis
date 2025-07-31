#!/usr/bin/env python

from argparse import ArgumentParser
import signal
import time
import sys

parser = ArgumentParser()
parser.add_argument("sleep", metavar="SECS", type=float)
args = parser.parse_args()


def sigterm(signum, frame):
    sig = signal.Signals(signum)
    sys.exit(0)


signal.signal(signal.Signals.SIGTERM, sigterm)

print(f"sleeping for {args.sleep} sec", file=sys.stderr)
time.sleep(args.sleep)
print("done", file=sys.stderr)
