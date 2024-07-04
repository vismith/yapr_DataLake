from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

def subscription_get(events: DataFrame) -> DataFrame:
    return (events.filter('event_type = "subscription"')
    .select(
        F.col('event.subscription_channel').alias('channel_id'),
        F.col('event.user').cast(T.LongType()).alias('user_id'))
    .distinct())

def user_contacts(events:DataFrame) -> DataFrame:
    return (events
    .filter('event_type = "message" and event.message_to is not null')
    .select('event.message_from', 'event.message_to',
        F.explode(F.array('event.message_from', 'event.message_to')).alias('user_id')
    )
    .withColumn('contact_id',
        F.when(F.col('user_id') == F.col('message_from'), F.col('message_to'))
        .otherwise(F.col('message_from')))
    .select('user_id', 'contact_id')
    .distinct()
    )

def messageg_get(events: DataFrame, date: str) -> DataFrame:
    return (events
    .filter((F.col('event_type') == "message") &
        (F.col('event.message_from').isNotNull()) &
        (F.col('date') == date)
    )
    .select(
        F.col('event.message_from').alias('user_id'),
        F.coalesce(
            F.col('event.message_ts'),
            F.col('event.datetime'))
            .cast(T.TimestampType()).alias('message_ts'),
        F.radians(F.col('lat')).alias('latitude_rad'),
        F.radians(F.col('lon')).alias('longitude_rad'),
        F.col('date')
    )
    )

def rec_pairs(message_events: DataFrame,
        subscriptions: DataFrame,
        contacts: DataFrame) -> DataFrame:
    event_channel_contacs = (
        message_events
        .select('user_id')
        .distinct()
        .join(subscriptions, on='user_id')
        .join(contacts, on='user_id', how='left')
        )
    return (event_channel_contacs.alias('left')
        .join(event_channel_contacs.alias('right'),
            on=[F.col('left.channel_id') == F.col('right.channel_id'),
                F.col('left.user_id') != F.col('right.user_id')    
            ]    
        )
        .filter('left.contact_id <> right.user_id')
        .select(
            'left.channel_id', # исключить из результата?
            F.array_sort(F.array('left.user_id', 'right.user_id')).alias('pair')
            )
        .distinct()
        )



def event_user_pair(day_messages:DataFrame, rec_pairs: DataFrame) -> DataFrame:
    cols = ['message_ts', 'latitude_rad', 'longitude_rad']
    return (
        rec_pairs
        .join(day_messages.alias('left'), 
            on = [F.col('pair')[0] == F.col('left.user_id')]
            )
        .select(
            'channel_id', 
            'pair',
            *[F.col(n).alias(f'left_{n}') for n in cols]
        )
        .join(day_messages.alias('right'), 
            on = [F.col('pair')[1] == F.col('right.user_id')]
            )
        .select(
            'channel_id',
            'pair',
            *[F.col(f'left_{n}') for n in cols],
            *[F.col(n).alias(f'right_{n}') for n in cols]
        )
        
    )

def recommend_pair_by_distance(message_recommend_pair: DataFrame) -> DataFrame:
    cols = ['message_ts', 'latitude_rad', 'longitude_rad']
    spheD = 2*6371
    return (
        message_recommend_pair
        .withColumn('distance',
            F.floor(
            spheD*F.asin(F.sqrt(
                (1 - F.cos(F.col('left_latitude_rad')
                        - F.col('right_latitude_rad'))
                + F.cos(F.col('left_latitude_rad'))
                    *F.cos(F.col('right_latitude_rad'))
                    *(1 - F.cos(F.col('left_longitude_rad')
                            - F.col('right_longitude_rad')
                            )
                    )
                ) / F.lit(2)
            ))
            )
        )
        .filter('distance <= 1000')
        .select(
            F.col('pair'),
            *[F.col(f'left_{n}').alias(n) for n in cols]
            )
        )

def recommend_datamart(events: DataFrame, city: DataFrame) -> DataFrame:
    return (
        events.crossJoin(city.alias('city'))
        .select('*'
        , F.row_number().over(
            Window
                .partitionBy('message_ts')
                .orderBy(
                    (
                        #spheD*F.asin(F.sqrt(
                            (1 - F.cos(F.col('latitude_rad') - F.col('city.rad_latitude'))
                            + F.cos(F.col('latitude_rad'))*F.cos(F.col('city.rad_latitude'))
                            *(1 - F.cos(F.col('longitude_rad') - F.col('city.rad_longitude')))
                            )
                        #      / F.lit(2)))
                    )
                )
            ).alias('distance_sort')
        )
        .filter('distance_sort = 1')
        .select(
            F.col('pair')[0].alias('user_left'),
            F.col('pair')[1].alias('user_right'),
            F.col('message_ts').cast(T.DateType()).alias('processed_dttm'),
            F.col('city_id').alias('zone_id'),
            F.from_utc_timestamp(F.col('message_ts'), F.col('timezone')).alias('local_time')
        )
    )


if __name__ == '__main__':
    from sys import argv
    date = argv[1] #'2022-02-01'
    events_path = argv[2]
    city_path = argv[3]
    result_path = argv[4]

    spark = SparkSession.builder.appName('PiSmith recommend pairs').getOrCreate()

    city = spark.read.parquet(city_path)

    events = (spark.read.parquet(events_path).filter(F.col('date') <= date))

    subscriptions = subscription_get(events)
    contacts = user_contacts(events)
    messages = messageg_get(events, date)

    candidate_pairs = rec_pairs(messages, subscriptions, contacts)

    events_candidate_pairs = event_user_pair(messages, candidate_pairs)

    events_recommend_pair_distance = recommend_pair_by_distance(events_candidate_pairs)

    events_recommend_datamart = recommend_datamart(events_recommend_pair_distance, city)

    (events_recommend_datamart
        .write
        .mode('overwrite')
        .partitionBy('processed_dttm')
        .parquet(result_path)
    )









