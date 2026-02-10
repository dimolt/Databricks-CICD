from pyspark import pipelines as dp
from pyspark.sql.functions import count, round, avg

catalog = spark.conf.get("catalog")

@dp.table(name=f"{catalog}.03_gold.gold_citibike_daily_station_performance")
def gold_citibike_daily_station_performance():
    df = (
        dp.read(f"{catalog}.02_silver.silver_citibike").\
                    groupBy("trip_start_date", "start_station_name").\
                    agg(
                    round(avg("trip_duration_mins"),2).alias("avg_trip_duration_mins"),
                    count("ride_id").alias("total_trips")
        ))
    return df