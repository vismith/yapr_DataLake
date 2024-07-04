from os import environ 
from datetime import datetime

environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
environ['SPARK_HOME'] = '/usr/lib/spark'
environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id='spark_recommend_datamarts',
    start_date=datetime(2022, 2, 1),
    end_date=datetime(2022, 2, 3),
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
    # base spark apps path
    app_base_path = '/lessons/spark_jobs'

    datamart_path = '/user/pismith/project/data/datamarts/recommend'

    start_date = '{{ ds }}'

    spark_recommend_datamart = SparkSubmitOperator(task_id='recommend_datamart',
            application=(app_base_path + '/t3_recommend_datamart.py'),
            conn_id='yarn_spark',
            application_args=[start_date, ods_events, ods_city,
                datamart_path]
        )
    
    spark_recommend_datamart

datamart_dag()
