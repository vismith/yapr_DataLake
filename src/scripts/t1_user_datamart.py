from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

def read_city_events(spark: SparkSession, city_events_path: str) -> DataFrame:
    return spark.read.parquet(city_events_path)

def date_events(events: DataFrame, event_date: str) -> DataFrame:
    return events.filter(F.col('date') == event_date)
    
def last_message_events(events: DataFrame) -> DataFrame:
    return (
        events
        .withColumn('message_number', F.row_number().over(
        Window.partitionBy('date', 'user_id').orderBy(F.desc('message_ts'))
        ))                 
        .filter('message_number = 1')
        .drop('message_number')
        .withColumn('localtime',
            F.from_utc_timestamp(F.col('message_ts'), F.col('timezone')))
        .withColumnRenamed('city_name', 'act_city')
    )

def user_travel(city_message_events: DataFrame) -> DataFrame:   
    wnd_user = Window.partitionBy('user_id').orderBy('message_ts')
    return (city_message_events
        .withColumn('trip_city'
            , F.when(F.col('city_id') == 
                F.lag('city_id').over(wnd_user), 0).otherwise(1)
        )
        .filter('trip_city == 1')
        .groupBy('user_id', 'date').agg(F.collect_list('city_name').alias('travel_array'))
        .withColumn('travel_count', F.size('travel_array'))
    )

def user_home_city(city_message_events: DataFrame, days: int) -> DataFrame:
    return (
        city_message_events
        .select('user_id', 'date', 'city_id', 'city_name')
        .distinct()
        .withColumn('diff',
            (F.col('date') 
             - F.row_number()
                .over(Window.partitionBy('user_id', 'city_id').orderBy('date'))
            )
        )
        .groupBy('user_id', 'city_id', 'city_name', 'diff')
            .agg(F.count('diff').alias('day_seq'))
        .filter(F.col('day_seq') >= days)
        .withColumn('city_count',
            F.row_number().over(Window.partitionBy('user_id')
                            .orderBy(F.desc('diff'))))
        .withColumn('city_rank',
            F.dense_rank().over(Window.partitionBy('user_id')
                            .orderBy(F.desc('city_count'), F.desc('diff'))))
        .filter('city_rank = 1')
        .select('user_id', 'city_id', F.col('city_name').alias('home_city'))
    )

def user_datamart(last_message_events: DataFrame,
                user_travel: DataFrame,
                user_home_city: DataFrame) -> DataFrame:
    return (last_message_events
    .join(user_travel, 'user_id')
    .select('user_id', 'act_city', 'travel_array', 'travel_count', last_message_events['date'])
    .join(user_home_city, 'user_id', how='left')
    .drop('city_id')
    )
    
def user_datamart_write(events_date: str, user_datamart: DataFrame, result_path: str):
    (
        user_datamart.write.mode('overwrite').parquet(f"{result_path}/date={events_date}")
    )

if __name__ == '__main__':
    import sys
    
    events_date = sys.argv[1]
    city_message_path = sys.argv[2]
    result_path = sys.argv[3]
    
    spark = SparkSession.builder.appName('PiSmith user events datamart').getOrCreate()
    
    city_message_events_df = read_city_events(spark, city_message_path)
    date_city_message_events_df = date_events(city_message_events_df, events_date).cache()

    last_date_message_events_df = last_message_events(date_city_message_events_df)

    date_user_travel_df = user_travel(date_city_message_events_df)

    home_city_days = 27
    user_home_city_df = user_home_city(city_message_events_df, home_city_days)

    date_user_datamart = user_datamart(
        last_date_message_events_df,
        date_user_travel_df,
        user_home_city_df)
    
    user_datamart_write(events_date, date_user_datamart, result_path)



    