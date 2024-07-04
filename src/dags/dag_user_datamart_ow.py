from os import environ 
from datetime import datetime

environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
environ['SPARK_HOME'] = '/usr/lib/spark'
environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id='spark_datamarts',
    start_date=datetime(2022, 2, 1),
    end_date=datetime(2022, 3, 31),
    schedule_interval='@daily',
    max_active_runs=1,
    is_paused_upon_creation=False,
    catchup=True
)
def datamart_dag():
    
    # partition by date/event_type
    ods_events = '/user/pismith/project/data/ods/events'
    # city with timezone
    ods_city = '/user/pismith/project/data/ods/city'
    # message events with city
    ods_city_events = '/user/pismith/project/data/ods/city_events'
    # base spark apps path
    app_base_path = '/lessons/spark_jobs'

    user_datamart_path = '/user/pismith/project/data/datamarts/user'

    start_date = '{{ ds }}'

    spark_user_datamart = SparkSubmitOperator(task_id='user_datamart',
            application=(app_base_path + '/t1_user_datamart.py'),
            conn_id='yarn_spark',
            application_args=[start_date, ods_city_events,
                user_datamart_path]
        )
    
    spark_user_datamart

datamart_dag()
