"""
features_network_experience.py — called by DAG 03, TaskGroup
subscriber_360/features_network_experience

Joins each subscriber to the quality of the cells they actually used. A
subscriber whose home cell has a rising drop-call rate is being pushed out by
the network, and no retention offer will fix that.

TECHNIQUE ON SHOW
  * a genuinely skewed join: subscriber-cell events against cell KPIs. A busy
    city-centre cell appears in millions of subscriber rows. AQE's skew join
    handles most of it; the broadcast of the small KPI side handles the rest.
  * weighted experience: a subscriber's drop-call exposure is the average of
    their cells' rates WEIGHTED by how much they used each cell, not a plain
    average over distinct cells.
  * "home cell" identification by usage share, using a ranked window.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--output", required=True)
    p.add_argument("--lookback-days", type=int, default=30)
    args = p.parse_args()

    spark = J.build_spark(f"features-network-{args.run_date}")
    cdr = J.fq("telco_prod_silver", "cdr_events")
    kpi = J.fq("telco_prod_gold", "ran_cell_kpi")
    run_date = args.run_date

    J.banner(f"network experience features as of {run_date}")

    usage = (
        spark.table(cdr)
        .where(F.col("event_date") > F.date_sub(F.lit(run_date), args.lookback_days))
        .where(F.col("event_date") <= F.lit(run_date))
        .where(F.col("cell_id").isNotNull())
        .groupBy("subscriber_id", "cell_id")
        .agg(F.count("*").alias("events_on_cell"),
             F.sum("duration_sec").alias("sec_on_cell"),
             F.sum(F.when(F.col("call_end_cause") == "DROPPED", 1).otherwise(0))
              .alias("own_drops"))
    )

    # Cell quality over the same window: one row per cell, small enough to
    # broadcast (~40k rows), which removes the skew from the join entirely.
    cell_quality = (
        spark.table(kpi)
        .where(F.col("window_start") > F.date_sub(F.lit(run_date), args.lookback_days))
        .where(F.col("window_start") <= F.lit(run_date))
        .groupBy("cell_id")
        .agg(
            # Ratios recombined from counters, never averaged. Same rule as
            # ran_kpi_aggregate.
            F.round(100.0 * F.sum("call_drops") / F.greatest(F.sum("call_attempts"),
                                                             F.lit(1)), 4)
             .alias("cell_drop_rate_30d"),
            F.round(F.avg("prb_utilisation"), 2).alias("cell_congestion_30d"),
            F.round(F.avg("throughput_mbps"), 3).alias("cell_throughput_30d"),
            F.first("technology").alias("cell_technology"),
        )
    )

    joined = usage.join(F.broadcast(cell_quality), "cell_id", "left")

    # Home cell = the cell carrying the most seconds.
    rank_w = Window.partitionBy("subscriber_id").orderBy(F.col("sec_on_cell").desc())
    with_home = joined.withColumn("_rn", F.row_number().over(rank_w))

    home = (
        with_home.where(F.col("_rn") == 1)
        .select("subscriber_id",
                F.col("cell_id").alias("home_cell_id"),
                F.col("cell_drop_rate_30d").alias("home_cell_drop_rate"),
                F.col("cell_congestion_30d").alias("home_cell_congestion"),
                F.col("cell_technology").alias("home_cell_technology"))
    )

    weighted = joined.groupBy("subscriber_id").agg(
        # Exposure weighted by time on each cell.
        F.round(F.sum(F.col("cell_drop_rate_30d") * F.col("sec_on_cell"))
                / F.greatest(F.sum("sec_on_cell"), F.lit(1)), 4)
         .alias("weighted_drop_rate_exposure"),
        F.round(F.sum(F.col("cell_congestion_30d") * F.col("sec_on_cell"))
                / F.greatest(F.sum("sec_on_cell"), F.lit(1)), 4)
         .alias("weighted_congestion_exposure"),
        F.countDistinct("cell_id").alias("distinct_cells_30d"),
        F.sum("own_drops").alias("own_dropped_calls_30d"),
        F.sum("events_on_cell").alias("cell_events_30d"),
    )

    features = (
        weighted.join(home, "subscriber_id", "left")
        .withColumn("own_drop_rate_30d",
                    F.when(F.col("cell_events_30d") > 0,
                           F.round(100.0 * F.col("own_dropped_calls_30d")
                                   / F.col("cell_events_30d"), 4))
                     .otherwise(F.lit(None)))
        # Mobility: many cells means a commuter, one cell means a home user.
        .withColumn("mobility_bucket",
                    F.when(F.col("distinct_cells_30d") <= 3, "STATIC")
                     .when(F.col("distinct_cells_30d") <= 20, "LOCAL")
                     .otherwise("MOBILE"))
        .withColumn("feature_date", F.lit(run_date).cast("date"))
    )

    J.assert_not_empty(features, "network experience features")
    J.assert_unique(spark, features, ["subscriber_id"], "network features")

    features.repartition(200).write.mode("overwrite").parquet(args.output)
    J.banner(f"network experience features written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
