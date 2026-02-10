from pyspark import pipelines as dp
from pyspark.sql.functions import col, unix_timestamp, to_date

catalog = spark.conf.get("catalog")

@dp.table(name=f"{catalog}.02_silver.silver_citibike")
def silver_citibike():
    df_bronze = dp.read(f"{catalog}.01_bronze.bronze_citibike")

    # Compute duration, extract date, then select the final columns
    df_silver = (
        df_bronze
        .withColumn(
            "trip_duration_mins",
            (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60
        )
        .withColumn(
            "trip_start_date",
            to_date(col("started_at"))
        )
        .select(
            "ride_id",
            "trip_start_date",
            "started_at",
            "ended_at",
            "start_station_name",
            "end_station_name",
            "trip_duration_mins"
        )
    )

    return df_silver
