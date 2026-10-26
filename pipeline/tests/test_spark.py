from pathlib import Path

from pyspark.sql import SparkSession


def test_delta_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    rows = [(i, f"site-{i % 3}") for i in range(10)]
    df = spark.createDataFrame(rows, ["event_id", "site_id"])
    target = str(tmp_path / "events")

    df.write.format("delta").mode("overwrite").save(target)

    back = spark.read.format("delta").load(target)
    assert back.count() == 10
    assert back.groupBy("site_id").count().count() == 3
