import logging
from datetime import datetime, timedelta
import secrets

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

log = logging.getLogger(__name__)

dag = DAG(
    "example1",
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

## keytab 적용방식 분석
# curl
# keytab [kube secret]
# vault

with dag:
    task_1 = KubernetesPodOperator(
        image="idock.daumkakao.io/ai_service/we-airflow:base",
        namespace="airflow",
        cmds=["python", "-c", "print('hello pod')"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        name="task-hello",
        task_id="task-1-echo",
        is_delete_operator_pod=False,
        in_cluster=True,
    )
    task_2 = KubernetesPodOperator(
        image="airflow-custom:1.1.1",
        namespace="airflow",
        cmds=["bash", "-cx"],
        arguments=["pip list"],
        labels={"foo": "bar"},
        name="task-pip",
        task_id="task-pip-llist",
        is_delete_operator_pod=False,
        in_cluster=True,
    )

task_1 >> task_2