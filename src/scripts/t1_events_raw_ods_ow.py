from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def events_ods_write(event_date: str,
        raw_path: str,
        ods_path: str,
        spark: SparkSession):
    (spark.read.parquet(raw_path)
        .filter(F.col('date') == event_date)
        .write.mode('overwrite')
        .partitionBy('event_type')
        .parquet(f"{ods_path}/date={event_date}")
    )

if __name__ == '__main__':
    
    import os 
    os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
    os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

    import sys
    event_date = sys.argv[1]
    raw_path = sys.argv[2]
    ods_path = sys.argv[3]

    spark = SparkSession.builder.appName('PiSmith events raw - ods').getOrCreate()

    
    
    events_ods_write(event_date=event_date, raw_path=raw_path,
        ods_path=ods_path, spark=spark)
    
    spark.stop()