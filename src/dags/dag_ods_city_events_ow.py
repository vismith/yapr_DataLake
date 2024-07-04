from os import environ 
from datetime import datetime

environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
environ['SPARK_HOME'] = '/usr/lib/spark'
environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id='spark_city_events',
    start_date=datetime(2022, 2, 1),
    end_date=datetime(2022, 3, 31),
    schedule_interval='@daily',
    max_active_runs=1,
    is_paused_upon_creation=False,
    catchup=True
)
def city_events_dag():
    # partition by date
    raw_geo_events = '/user/master/data/geo/events'
    # partition by date/event_type
    ods_events = '/user/pismith/project/data/ods/events'
    # city with timezone
    ods_city = '/user/pismith/project/data/ods/city'
    # message events with city
    ods_city_events = '/user/pismith/project/data/ods/city_events'
    # base spark apps path
    app_base_path = '/lessons/spark_jobs'


    start_date = '{{ ds }}'

    spark_events_ods = SparkSubmitOperator(task_id='ods_events_writer',
                        application=(app_base_path + '/t1_events_raw_ods_ow.py'),
                        conn_id='yarn_spark',
                        application_args=[start_date, raw_geo_events, ods_events]
                    )
    
    spark_city_events = SparkSubmitOperator(task_id='ods_city_message_events',
                        application=(app_base_path + '/t1_city_events_write_ow.py'),
                        conn_id='yarn_spark',
                        application_args=[ods_events, ods_city, start_date, ods_city_events]
                    )
    
    spark_events_ods >> spark_city_events

city_events_dag()
