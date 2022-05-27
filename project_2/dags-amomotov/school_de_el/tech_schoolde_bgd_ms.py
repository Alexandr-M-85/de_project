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
    'school_de_amomotov_el',
    default_args=default_args,
    description='EL for tables from postgres',
    schedule_interval=None,
    on_success_callback=VaultOperator.cleanup_xcom,
    on_failure_callback=VaultOperator.cleanup_xcom,
    tags=['school_de_amomotov'],
)


start_DAG = DummyOperator(
    task_id='start',
    dag=dag)

set_secrets = VaultOperator(
    task_id='set_secrets',
    provide_context=True,
    secret_path="airflow-secrets",
    secret_name="tech_repbgd_local",
    dag=dag
)

submit_spark_job = SparkSubmitOperator(
    application="hdfs://ns-etl/apps/airflow/tech_schoolde_bgd_ms/datamarts-amomotov/version/latest/datamarts-amomotov-assembly-0.1.jar",
    name="school_de_amomotov_el",
    conf={'spark.yarn.queue': 'prod', 'spark.submit.deployMode': 'cluster', 'spark.driver.memory': '4g',
          'spark.executor.memory': '8g', 'spark.executor.cores': '1', 'spark.dynamicAllocation.enabled': 'true',
          'spark.shuffle.service.enabled': 'true', 'spark.dynamicAllocation.maxExecutors': '10'
          },
    task_id="submit_amomotov_el",
    conn_id="hdp31_spark",
    java_class="ru.dmp.AppEl",
    queue='prod',
    application_args=[
        "{{task_instance.xcom_pull(key='tech_repbgd_local-username', task_ids=['set_secrets'])[0]}}",
        "{{task_instance.xcom_pull(key='tech_repbgd_local-password', task_ids=['set_secrets'])[0]}}",
    ],
    dag=dag
)
start_DAG >> set_secrets >> submit_spark_job