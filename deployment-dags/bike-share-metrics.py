from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,\
    DataprocClusterDeleteOperator, DataProcSparkOperator
from airflow.utils import trigger_rule
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from time import time
from datetime import datetime, timedelta

dag_name = "bike-share-metrics".strip()


def push_cluster_name(**kwargs):
    ti = kwargs['ti']
    cluster_name = dag_name + str(int(round(time() * 100)))
    ti.xcom_push(key="cluster_name", value=cluster_name)


with DAG(dag_id=dag_name,
         schedule_interval="@daily",
         start_date=datetime.strptime("2014-01-01", "%Y-%m-%d"),
         max_active_runs=1,
         concurrency=10,
         default_args={'project_id': 'spark-bike-share'}) as dag:

    push_cluster_name = PythonOperator(task_id="push_cluster_name",
                                       dag=dag,
                                       provide_context=True,
                                       python_callable=push_cluster_name)

    create_cluster_1 = DataprocClusterCreateOperator(
        task_id='create_cluster_1',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '1',
        region='us-central1',
        num_workers=2,
        worker_machine_type='n1-standard-2',
        master_machine_type='n1-standard-2',
        execution_timeout=timedelta(minutes=10))
    create_cluster_1.set_upstream(push_cluster_name)
    delete_cluster_1 = DataprocClusterDeleteOperator(
        task_id='delete_cluster_1',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '1',
        region='us-central1',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=10))

    create_cluster_2 = DataprocClusterCreateOperator(
        task_id='create_cluster_2',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '2',
        region='us-central1',
        num_workers=2,
        worker_machine_type='n1-standard-2',
        master_machine_type='n1-standard-2',
        execution_timeout=timedelta(minutes=10))
    create_cluster_2.set_upstream(push_cluster_name)
    delete_cluster_2 = DataprocClusterDeleteOperator(
        task_id='delete_cluster_2',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '2',
        region='us-central1',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=10))

    create_cluster_3 = DataprocClusterCreateOperator(
        task_id='create_cluster_3',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '3',
        region='us-central1',
        num_workers=2,
        worker_machine_type='n1-standard-2',
        master_machine_type='n1-standard-2',
        execution_timeout=timedelta(minutes=10))
    create_cluster_3.set_upstream(push_cluster_name)
    delete_cluster_3 = DataprocClusterDeleteOperator(
        task_id='delete_cluster_3',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '3',
        region='us-central1',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=10))

    create_cluster_4 = DataprocClusterCreateOperator(
        task_id='create_cluster_4',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '4',
        region='us-central1',
        num_workers=2,
        worker_machine_type='n1-standard-2',
        master_machine_type='n1-standard-2',
        execution_timeout=timedelta(minutes=10))
    create_cluster_4.set_upstream(push_cluster_name)
    delete_cluster_4 = DataprocClusterDeleteOperator(
        task_id='delete_cluster_4',
        project_id='spark-bike-share',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '4',
        region='us-central1',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=10))

    args = [
        "--env",
        "test",
        "--start.date",
        "{{ (execution_date).strftime('%Y-%m-%d') }}",
    ]

    update_unique_user = DataProcSparkOperator(
        task_id='update_unique_user',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '1',
        job_name=dag_name + '-' + 'update_unique_user',
        region='us-central1',
        main_class='com.terry.bigdata.process.UniqueUserUpdate',
        dataproc_spark_jars=[
            "gs://bike-share-data/test/jar/BikeShareMetrics-assembly-0.1.jar"
        ],
        execution_timeout=timedelta(minutes=5),
        arguments=args)

    args = [
        "--env",
        "test",
        "--start.date",
        "{{ (execution_date).strftime('%Y-%m-%d') }}",
    ]

    calc_duration = DataProcSparkOperator(
        task_id='calc_duration',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '1',
        job_name=dag_name + '-' + 'calc_duration',
        region='us-central1',
        main_class='com.terry.bigdata.process.DurationMetric',
        dataproc_spark_jars=[
            "gs://bike-share-data/test/jar/BikeShareMetrics-assembly-0.1.jar"
        ],
        execution_timeout=timedelta(minutes=5),
        arguments=args)

    args = [
        "--env", "test", "--start.date",
        "{{ (execution_date).strftime('%Y-%m-%d') }}", "--day.ago", "1"
    ]

    day_1_retention = DataProcSparkOperator(
        task_id='day_1_retention',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '2',
        job_name=dag_name + '-' + 'day_1_retention',
        region='us-central1',
        main_class='com.terry.bigdata.process.RetentionMetrics',
        dataproc_spark_jars=[
            "gs://bike-share-data/test/jar/BikeShareMetrics-assembly-0.1.jar"
        ],
        execution_timeout=timedelta(minutes=5),
        arguments=args)

    args = [
        "--env", "test", "--start.date",
        "{{ (execution_date).strftime('%Y-%m-%d') }}", "--day.ago", "3"
    ]

    day_3_retention = DataProcSparkOperator(
        task_id='day_3_retention',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '3',
        job_name=dag_name + '-' + 'day_3_retention',
        region='us-central1',
        main_class='com.terry.bigdata.process.RetentionMetrics',
        dataproc_spark_jars=[
            "gs://bike-share-data/test/jar/BikeShareMetrics-assembly-0.1.jar"
        ],
        execution_timeout=timedelta(minutes=5),
        arguments=args)

    args = [
        "--env", "test", "--start.date",
        "{{ (execution_date).strftime('%Y-%m-%d') }}", "--day.ago", "7"
    ]

    day_7_retention = DataProcSparkOperator(
        task_id='day_7_retention',
        cluster_name=
        '{{ ti.xcom_pull(key="cluster_name", task_ids="push_cluster_name") }}'
        + '-' + '4',
        job_name=dag_name + '-' + 'day_7_retention',
        region='us-central1',
        main_class='com.terry.bigdata.process.RetentionMetrics',
        dataproc_spark_jars=[
            "gs://bike-share-data/test/jar/BikeShareMetrics-assembly-0.1.jar"
        ],
        execution_timeout=timedelta(minutes=5),
        arguments=args)

    trip_file_sensor = GoogleCloudStorageObjectSensor(
        task_id='trip_file_sensor',
        poke_interval=30,
        bucket='bike-share-data',
        object='test/bike/unique-user/_SUCCESS')

    create_cluster_1.set_upstream(trip_file_sensor)

    create_cluster_1.set_downstream(update_unique_user)

    delete_cluster_1.set_upstream(calc_duration)

    create_cluster_2.set_upstream(delete_cluster_1)

    create_cluster_2.set_downstream(day_1_retention)

    delete_cluster_2.set_upstream(day_1_retention)

    create_cluster_3.set_upstream(delete_cluster_1)

    create_cluster_3.set_downstream(day_3_retention)

    delete_cluster_3.set_upstream(day_3_retention)

    create_cluster_4.set_upstream(delete_cluster_2)

    create_cluster_4.set_upstream(delete_cluster_3)

    create_cluster_4.set_downstream(day_7_retention)

    delete_cluster_4.set_upstream(day_7_retention)

    update_unique_user.set_downstream(calc_duration)
