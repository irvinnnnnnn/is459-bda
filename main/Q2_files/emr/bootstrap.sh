#!/bin/bash
# EMR bootstrap action, run on every node: q2_score.py needs boto3, which EMR 7 does not install for Spark's Python.
set -e
sudo python3 -m pip install boto3
