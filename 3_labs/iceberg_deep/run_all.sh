#!/usr/bin/env bash
# Run the whole Iceberg suite in order, stopping at the first failed check.
#
#   ./run_all.sh
#
# On Windows, run `call windows_env.bat` in a cmd shell first (or export the
# same four variables in Git Bash) or the very first write will fail.
set -e

for f in 00_setup 01_anatomy 02_commits_time_travel 03_evolution \
         04_cow_mor_deletes 05_maintenance_wap; do
    echo ""
    echo "############################################################"
    echo "#  $f.py"
    echo "############################################################"
    python "$f.py"
done

echo ""
echo "All Iceberg labs passed."
