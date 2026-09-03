#!/usr/bin/env bash
# Trigger every lab DAG and wait for the results. Run from 3_labs/airflow_local
# with the stack up (docker compose up -d). ~10 minutes, mostly DAG 04's six
# Spark jobs running one at a time through the spark_local pool.
#
# What it exercises, per DAG:
#   01 02 05 08      triggered directly
#   03               triggered after a landing file is created for today and yesterday
#   04               triggered; watch it in the UI, it is the slow one
#   06               only unpaused — catchup creates its 7 runs by itself
#   07a              triggered; 07b must then run twice (Asset + explicit trigger)
set -u
cd "$(dirname "$0")"
af() { docker compose exec -T airflow-scheduler airflow "$@"; }

TODAY=$(date -u +%F)
YDAY=$(date -u -d yesterday +%F 2>/dev/null || date -u -v-1d +%F)
mkdir -p "landing/$TODAY" "landing/$YDAY"
printf 'order_id,amount\n1,10\n2,20\n3,30\n' > "landing/$TODAY/orders.csv"
cp "landing/$TODAY/orders.csv" "landing/$YDAY/orders.csv"
echo "landing file created for $TODAY and $YDAY"

DAGS="01_hello_dag 02_taskflow_xcom_branch 03_sensor_wait_for_file 04_medallion_iceberg_pipeline 05_dynamic_task_mapping 06_backfill_and_catchup 07a_produce_silver_orders 07b_consume_silver_orders 08_failures_retries_callbacks"
for d in $DAGS; do af dags unpause "$d" >/dev/null 2>&1 && echo "unpaused  $d"; done
for d in 01_hello_dag 02_taskflow_xcom_branch 03_sensor_wait_for_file 05_dynamic_task_mapping 08_failures_retries_callbacks 07a_produce_silver_orders 04_medallion_iceberg_pipeline; do
  af dags trigger "$d" >/dev/null 2>&1 && echo "triggered $d"
done

summary() {
  for d in $DAGS; do
    af dags list-runs "$d" -o plain 2>/dev/null | awk -v d="$d" '
      NR>1 { c[$3]++; n++ }
      END  { printf "  %-32s runs=%2d ", d, n; for (k in c) printf " %s=%d", k, c[k]; print "" }'
  done
}

echo; echo "waiting for runs to settle (Ctrl-C is safe; the DAGs keep running in Airflow)"
for i in $(seq 1 27); do
  sleep 20
  s=$(summary)
  active=$(echo "$s" | grep -cE "running=|queued=")
  total=$(echo "$s" | awk '{for(i=1;i<=NF;i++) if($i ~ /^runs=/){split($i,a,"="); t+=a[2]}} END{print t+0}')
  printf '\r[%3ds] runs=%s active_dags=%s ' $((i*20)) "$total" "$active"
  [ "$active" -eq 0 ] && [ "$total" -ge 9 ] && break
done
echo; echo; echo "=== DAG run states"; summary

echo; echo "=== task states of any failed run"
for d in $DAGS; do
  af dags list-runs "$d" -o plain 2>/dev/null | awk 'NR>1 && $3=="failed"{print $1, $2}' | while read -r dag run; do
    echo "--- $dag  $run"; af tasks states-for-dag-run "$dag" "$run" -o plain 2>/dev/null
  done
done

echo; echo "=== outputs written by the DAGs (./work)"
ls work 2>/dev/null; ls work/backfill work/mapped 2>/dev/null | head -20
ls -d work/warehouse/iceberg/*/* 2>/dev/null | head
