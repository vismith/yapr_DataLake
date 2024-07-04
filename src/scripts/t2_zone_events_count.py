from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window



def date_user_events_count(events):
    event_types = ['reaction', 'subscription']
    result = (events
        .filter("event_type in ('reaction', 'subscription')")
        .select('*',
            F.coalesce(
                F.col('event.user').cast(T.LongType()), # subscription
                F.col('event.reaction_from').cast(T.LongType()) # reaction
                ).alias('user_id')

        )
        .select('*',
            *[F.sum(F.when(F.col('event_type') == event_type, 1).otherwise(0))
                .over(Window.partitionBy(['date', 'user_id', 'event_type'])).alias(f'day_user_{event_type}')
                    for event_type in event_types]

        )
        .select('date', 'user_id', 'event_type', *[f'day_user_{event_type}' for event_type in event_types])
    )
    return result

def city_date_eventtype_count (day_user_counts: DataFrame, message_user_city: DataFrame) -> DataFrame:
    event_types = ['reaction', 'subscription']
    date_intervals = ('month', 'week')
    return (day_user_counts
        .join(message_user_city.select('user_id', 'date', 'city_id'),
            on=['date', 'user_id'])
        .select('*',
            *[F.date_trunc(n , F.col('date')).cast(T.DateType()).alias(n) for n in date_intervals]
        )
        .select('*',
            *[F.sum(f'day_user_{event_type}')
                .over(Window.partitionBy([date_interval, 'city_id'])).alias(f'{date_interval}_{event_type}')
                    for event_type in event_types for date_interval in date_intervals]
        )
        .select('city_id', *[di for di in date_intervals], *[f'{di}_{et}' for et in event_types for di in date_intervals])
        .distinct()
)

def reaction_subscription_city_count(events: DataFrame, city_events: DataFrame) -> DataFrame:
    return city_date_eventtype_count(date_user_events_count(events), city_events)

def message_user_city_count(city_events: DataFrame) -> DataFrame:
    message_number_colum_name = 'user_message_number'
    date_intervals = ('month', 'week')
    return (city_events
        .drop('city_name', 'timezone', 'event_type')
        .select('*',
            *[F.date_trunc(di, 'date').cast(T.DateType()).alias(di)
            for di in date_intervals]    
        )
        .withColumn(message_number_colum_name, F.row_number()
            .over(Window.partitionBy('user_id').orderBy('message_ts'))
        )
        .select('*',
            *[F.count('date').over(Window.partitionBy(di, 'city_id'))
                .alias(f'{di}_message') for di in date_intervals],
            *[F.sum(F.when(F.col(message_number_colum_name) == 1, 1).otherwise(0))
                .over(Window.partitionBy(di, 'city_id')).alias(f'{di}_user')
            for di in date_intervals]
        )
        .drop(message_number_colum_name, 'date', 'message_ts', 'user_id')
        .distinct()
    )

def resu_meus_join(events: DataFrame, city_events: DataFrame) -> DataFrame:
    
    return (reaction_subscription_city_count(events=events, city_events=city_events)
        .join(message_user_city_count(city_events=city_events),
            on=['city_id', 'week', 'month'])
        .withColumnRenamed('city_id', 'zone_id'))

def main(month: str, events_path: str, city_events_path: str, result_path: str, spark: SparkSession):
    events, city_events = [(spark.read
        .parquet(path)
        .filter(F.date_trunc('month', 'date') == month))
        for path in [events_path, city_events_path]]
    (resu_meus_join(events=events, city_events=city_events)
        .write.mode('overwrite').parquet(f'{result_path}/month={month}')
    )
    

if __name__ == '__main__':
    from sys import argv
    month = argv[1] #'2022-02-01'
    events_path = argv[2]
    city_events_path = argv[3]
    result_path = argv[4]

    spark = SparkSession.builder.appName('PiSmith city events count').getOrCreate()

    main(month=month, events_path=events_path,
        city_events_path=city_events_path, result_path=result_path, spark=spark)



    


