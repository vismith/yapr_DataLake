from os import environ 
from datetime import datetime

environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
environ['SPARK_HOME'] = '/usr/lib/spark'
environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id='spark_events_datamarts',
    start_date=datetime(2022, 2, 1),
    schedule_interval='@once',
    max_active_runs=1,
    is_paused_upon_creation=False,
    catchup=True
)
def datamart_dag():
    
    # partition by date/event_type
    ods_events = '/user/pismith/project/data/ods/events'
    # message events with city
    ods_city_events = '/user/pismith/project/data/ods/city_events'
    # base spark apps path
    app_base_path = '/lessons/spark_jobs'

    zone_datamart_path = '/user/pismith/project/data/datamarts/zone'

    month = '{{ ds }}'

    spark_zone_datamart = SparkSubmitOperator(task_id='zone_datamart',
            application=(app_base_path + '/t2_zone_events_count.py'),
            conn_id='yarn_spark',
            application_args=[month, ods_events, ods_city_events,
                zone_datamart_path]
        )
    
    spark_zone_datamart

datamart_dag()
