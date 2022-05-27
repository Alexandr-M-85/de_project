from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from beeline.airflow.hashicorp_vault.VaultOperator import VaultOperator
from airflow.operators.dummy_operator import DummyOperator
import pendulum

local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 13, 1, 0, tzinfo=local_tz),
    'email': ['amomotov@stk.xxx.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'school_de_amomotov_calc',
    default_args=default_args,
    description='Calc results',
    schedule_interval=None,
    tags=['school_de_amomotov'],
)

start_DAG = DummyOperator(
    task_id='start',
    dag=dag)

submit_spark_job = SparkSubmitOperator(
    application="hdfs://ns-etl/apps/airflow/tech_schoolde_bgd_ms/datamarts-amomotov/version/latest/datamarts-amomotov-assembly-0.1.jar",
    name="school_de_admin_calc",
    conf={'spark.yarn.queue': 'prod', 'spark.submit.deployMode': 'cluster', 'spark.driver.memory': '4g',
          'spark.executor.memory': '8g', 'spark.executor.cores': '1', 'spark.dynamicAllocation.enabled': 'true',
          'spark.shuffle.service.enabled': 'true', 'spark.dynamicAllocation.maxExecutors': '10'
          },
    task_id="submit_amomotov_calc",
    conn_id="hdp31_spark",
    java_class="ru.dmp.AppCalc",
    queue='prod',
    application_args=[
        "outPath=hdfs://ns-etl/warehouse/tablespace/external/hive/school_de.db/results_amomotov",
        "outTableName=school_de.results_amomotov",
    ],
    dag=dag
)
start_DAG >> submit_spark_job