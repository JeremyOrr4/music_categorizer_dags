from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# DAG default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="music_pipeline",
    default_args=default_args,
    description="Run music processing pods in sequence",
    schedule_interval=None,  # Run on demand
    start_date=days_ago(1),
    catchup=False,
) as dag:

    volume_mount = {
        "name": "music-categorizer-pvc",
        "mountPath": "/music-categorizer-data",
    }

    volume = {
        "name": "music-categorizer-pvc",
        "persistentVolumeClaim": {"claimName": "music-categorizer-pvc"},
    }

    pcm_encoder = KubernetesPodOperator(
        task_id="pcm_encoder",
        name="pcm-encoder",
        namespace="default",
        image="pcm-encoder:latest",
        image_pull_policy="IfNotPresent",
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    pr_generator = KubernetesPodOperator(
        task_id="pr_generator",
        name="pr-generator",
        namespace="default",
        image="pr-generator:latest",
        image_pull_policy="IfNotPresent",
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    lr_generator = KubernetesPodOperator(
        task_id="lr_generator",
        name="lr-generator",
        namespace="default",
        image="lr-generator:latest",
        image_pull_policy="IfNotPresent",
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    music_recommender = KubernetesPodOperator(
        task_id="music_recommender",
        name="music-recommender",
        namespace="default",
        image="music-recommender:latest",
        image_pull_policy="IfNotPresent",
        cmds=["python", "main.py"],
        arguments=["--train"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    pcm_encoder >> pr_generator >> lr_generator >> music_recommender
