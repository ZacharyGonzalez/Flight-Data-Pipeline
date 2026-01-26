from datetime import timedelta

from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

CLUSTER_ID = "***"

with DAG(dag_id="exPlane-emr-transforms") as dag:
    init = PythonOperator(
        task_id="init", python_callable=print, op_args="Initialized Airflow!"
    )

    emr_submit_transform_task = EmrAddStepsOperator(
        job_flow_id=CLUSTER_ID,
        task_id="emr_submit_transform_task",
        aws_conn_id="aws_default",
        region_name="us-east-2",
        steps=[
            {
                "Name": "AirflowRunTransform",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "s3://flight-analysis-s3/src/flight_analysis.py",
                    ],
                },
            },
        ],
    )

    emr_wait_for_transform_task = EmrStepSensor(
        job_flow_id=CLUSTER_ID,
        task_id="emr_wait_for_transform_task",
        step_id="{{ ti.xcom_pull(task_ids='emr_submit_transform_task')[0] }}",
        region_name="us-east-2",
    )

    emr_submit_load_task = EmrAddStepsOperator(
        job_flow_id=CLUSTER_ID,
        task_id="emr_submit_load_task",
        aws_conn_id="aws_default",
        region_name="us-east-2",
        steps=[
            {
                "Name": "AirflowRunLoad",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--packages",
                        "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3",
                        "s3://flight-analysis-s3/src/load.py",
                    ],
                },
            },
        ],
    )

    emr_wait_for_load_task = EmrStepSensor(
        job_flow_id=CLUSTER_ID,
        task_id="emr_wait_for_load_task",
        step_id="{{ ti.xcom_pull(task_ids='emr_submit_load_task')[0] }}",
        region_name="us-east-2",
    )

    finish = PythonOperator(
        task_id="finish", python_callable=print, op_args="Finished Batch Processing."
    )

    (
        init
        >> emr_submit_transform_task
        >> emr_wait_for_transform_task
        >> emr_submit_load_task
        >> emr_wait_for_load_task
        >> finish
    )
