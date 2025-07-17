#!/usr/bin/env python

from argparse import ArgumentParser
import signal
import time

parser = ArgumentParser()
parser.add_argument("sleep", metavar="SECS", type=float)
args = parser.parse_args()


def sigterm(signum, frame):
    sig = signal.Signals(signum)
    print(f"ignoring {sig.name}")


signal.signal(signal.Signals.SIGTERM, sigterm)

print(f"sleeping for {args.sleep} sec")
time.sleep(args.sleep)
print("done")
