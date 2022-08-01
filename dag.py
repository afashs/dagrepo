import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

log = logging.getLogger(__name__)

dag = DAG(
    "example_using_k8s_pod_operator",
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args={
        "owner": "admin",
        "depends_on_past": False,
        "start_date": datetime(2020, 8, 7),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "sla": timedelta(hours=23),
    },
)


with dag:
    task_1 = KubernetesPodOperator(
        image="python:3.7",
        namespace="airflow",
        cmds=["python", "-c", "print('hello pod')"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        name="task-1-hello",
        task_id="task-1-echo",
        is_delete_operator_pod=False,
        in_cluster=True,
    )
    task_2 = KubernetesPodOperator(
        image="airflow-custom:1.0.5",
        namespace="airflow",
        cmds=["bash", "-cx"],
        arguments=["pip list"],
        labels={"foo": "bar"},
        name="task-2-pip",
        task_id="task-pip-llist",
        is_delete_operator_pod=False,
        in_cluster=True,
    )

task_1 >> task_2