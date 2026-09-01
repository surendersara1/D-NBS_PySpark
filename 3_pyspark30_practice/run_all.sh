#!/usr/bin/env bash
# Runs setup then all eight family files, stopping at the first failure.
set -e
python3 00_setup.py
for f in 01_shape 02_filter 03_strings 04_dates 05_aggregate 06_join_reshape 07_windows 08_control; do
  echo; echo "################  $f  ################"
  python3 "$f.py"
done
echo; echo "All eight families passed."
