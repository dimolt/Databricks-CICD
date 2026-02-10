from pyspark import pipelines as dp
from pyspark.sql.functions import max, min, avg, count, round

catalog = spark.conf.get("catalog")

@dp.table(name=f"{catalog}.03_gold.gold_citibike_daily_ride_summary")
def gold_citibike_daily_ride_summary():
    df = (
        dp.read(f"{catalog}.02_silver.silver_citibike").\
                groupBy("trip_start_date").agg(
                round(max("trip_duration_mins"),2).alias("max_trip_duration_mins"),
                round(min("trip_duration_mins"),2).alias("min_trip_duration_mins"),
                round(avg("trip_duration_mins"),2).alias("avg_trip_duration_mins"),
                count("ride_id").alias("total_trips")
        ))
    return df
