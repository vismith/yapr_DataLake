from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

def read_events(events_path: str, spark: SparkSession) -> DataFrame:
    return (spark
        .read
        .options(basePath=events_path)
        .parquet(events_path))

def type_events(events: DataFrame, event_type: str) -> DataFrame:
    return events.filter(F.col('event_type') == event_type)

def date_events(events: DataFrame, event_date: str) -> DataFrame:
    return events.filter(F.col('date') == event_date)        

def read_city(city_path: str, spark: SparkSession) -> DataFrame:
    return spark.read.parquet(city_path)

def users_message_events(events: DataFrame) -> DataFrame:
    return (events
        .filter((F.col('event_type') == 'message')
            & (~ F.isnull(F.col('event.message_from')))  
        )
        .select(
        F.coalesce(
            F.col('event.message_ts'), # a personal message
            F.col('event.datetime') # a channel message
        ).cast(T.TimestampType()).alias('message_ts')
        , F.col('event.message_from').alias('user_id')
        , F.radians('lat').alias('event_latitude_rad')
        , F.radians('lon').alias('event_longitude_rad')
        , 'date'
        , 'event_type'
        )
    )

def events_join_city(users_message_events: DataFrame, city: DataFrame) -> DataFrame:
    return (
        users_message_events.crossJoin(city)
        .select('*'
        , F.row_number().over(
            Window
                .partitionBy('user_id', 'message_ts')
                .orderBy(
                    (
                        # Since we only need relative comparison,
                        # it is possible to use a simplified expression to calculate the distance.
                            (1 - F.cos(F.col('event_latitude_rad') - F.col('rad_latitude'))
                            + F.cos(F.col('event_latitude_rad'))*F.cos(F.col('rad_latitude'))
                            *(1 - F.cos(F.col('event_longitude_rad') - F.col('rad_longitude')))
                            )
                    )
                )
            ).alias('distance_sort')
    )
    .filter('distance_sort = 1')
    .select('user_id', 'message_ts', 'city_name', 'timezone', 'city_id', 'date', 'event_type')
    )

def city_message_events_write(event_date:str, city_message_events: DataFrame,
                result_path:str):
    
    (
        city_message_events.write.partitionBy('event_type')
        .mode('overwrite').parquet(f"{result_path}/date={event_date}")
    )

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 5:
        raise Exception('arguments error')
    
    events_path = sys.argv[1]
    city_path = sys.argv[2]
    events_date = sys.argv[3]
    result_path = sys.argv[4]
    
    spark = SparkSession.builder.master('local').appName('user events datamart').getOrCreate()
    
    events = read_events(events_path=events_path, spark=spark)
    date_events_df = date_events(events, events_date)
    message_date_events = type_events(date_events_df, 'message')
    
    city = read_city(city_path, spark)
    
    city_message_date_events = events_join_city(
        users_message_events=users_message_events(message_date_events),
        city=city
        )
    city_message_events_write(events_date, city_message_date_events, result_path)
    
    spark.stop()