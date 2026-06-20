#!/usr/bin/env python

import signal
import sys
import time
from argparse import ArgumentParser

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
